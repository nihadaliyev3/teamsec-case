"""
Comprehensive test suite for the data_api app (Bank Simulator).

Tests cover:
- CSV upload with Strict Overwrite policy (no Django filename suffixes).
- Data mutation (update) endpoint.
- Multi-tenant download logic with streaming (BANK001/BANK002/BANK003 transformations).
- Streaming response handling for large files (200MB+).
- gzip compression support (using zlib.compressobj for streaming).
- Edge cases: validation, missing file_type.
"""

import gzip
import json
import os
import shutil
import tempfile
import zlib

from django.test import override_settings
from django.core.files.uploadedfile import SimpleUploadedFile
from rest_framework.test import APITestCase, APIClient

from .models import BankFile


# Temporary MEDIA_ROOT so tests never write to the real media/ folder.
TEMP_MEDIA_ROOT = tempfile.mkdtemp(prefix="data_api_test_media_")


@override_settings(MEDIA_ROOT=TEMP_MEDIA_ROOT)
class DataAPITestCase(APITestCase):
    """
    Base for data_api tests. Uses a temporary MEDIA_ROOT and cleans up after each test.
    """

    def setUp(self):
        super().setUp()
        self.client = APIClient()
        # Ensure temp MEDIA_ROOT exists for this test run
        os.makedirs(TEMP_MEDIA_ROOT, exist_ok=True)
        # Reusable sample CSV content (semicolon-separated, matching production format)
        self.sample_csv_content = (
            b"loan_account_number;outstanding_principal_balance\n"
            b"LOAN_000001;1000\n"
            b"LOAN_000002;200\n"
        )
        # Minimal valid CSV for happy-path upload
        self.valid_csv = (
            b"loan_account_number;outstanding_principal_balance\n"
            b"1000;100\n"
        )

    def tearDown(self):
        # Remove all BankFile records so file references don't point into temp dir
        BankFile.objects.all().delete()
        if os.path.exists(TEMP_MEDIA_ROOT):
            shutil.rmtree(TEMP_MEDIA_ROOT, ignore_errors=True)
        super().tearDown()
    
    def _decode_streaming_response(self, response):
        """
        Helper to decode StreamingHttpResponse content.
        
        Handles:
        - Collecting streamed chunks
        - gzip decompression if Content-Encoding: gzip
          (works with both gzip.compress and zlib.compressobj with gzip format)
        - JSON parsing
        
        Args:
            response: Django response object (may be StreamingHttpResponse)
            
        Returns:
            Parsed JSON data (list or dict)
        """
        # Collect all chunks from streaming response
        content = b''.join(response.streaming_content) if hasattr(response, 'streaming_content') else response.content
        
        # Decompress if gzip-encoded
        # Note: gzip.decompress() works with data compressed by zlib.compressobj(wbits=16+15)
        # Alternatively, could use: zlib.decompressobj(wbits=16+zlib.MAX_WBITS).decompress()
        if response.get('Content-Encoding') == 'gzip':
            content = gzip.decompress(content)
        
        # Decode bytes to string and parse JSON
        content_str = content.decode('utf-8')
        return json.loads(content_str)

    # -------------------------------------------------------------------------
    # 1. Basic Upload (Happy Path)
    # -------------------------------------------------------------------------

    def test_upload_valid_csv_creates_bank_file_version_one(self):
        """
        Happy path: uploading a valid CSV with file_type creates a BankFile
        with version=1. Ensures the upload pipeline and DB record are correct.
        """
        response = self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.sample_csv_content,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        self.assertEqual(response.status_code, 200, msg=response.data)
        self.assertIn("version", response.data)
        self.assertEqual(response.data["version"], 1)
        bank_file = BankFile.objects.get(file_type="commercial_credit")
        self.assertEqual(bank_file.version, 1)
        self.assertTrue(bank_file.file.name.endswith("commercial_credit.csv"))

    # -------------------------------------------------------------------------
    # 2. Strict Overwrite Logic (CRITICAL)
    # -------------------------------------------------------------------------

    def test_strict_overwrite_deletes_old_file_and_replaces_with_same_filename(self):
        """
        Strict Overwrite: uploading again for the same file_type must physically
        delete the old file and save the new one under the exact same filename
        (no Django auto-suffix like _AbCd1). Version must increment and on-disk
        content must be the new content.
        """
        # Upload "File A" with content "old_data"
        file_a = SimpleUploadedFile(
            "commercial_credit.csv",
            b"old_data",
            content_type="text/csv",
        )
        resp_a = self.client.post(
            "/api/upload/",
            data={"file": file_a, "file_type": "commercial_credit"},
            format="multipart",
        )
        self.assertEqual(resp_a.status_code, 200, msg=resp_a.data)
        self.assertEqual(resp_a.data["version"], 1)

        # Upload "File B" with content "new_data" for the SAME file_type
        file_b = SimpleUploadedFile(
            "commercial_credit.csv",
            b"new_data",
            content_type="text/csv",
        )
        resp_b = self.client.post(
            "/api/upload/",
            data={"file": file_b, "file_type": "commercial_credit"},
            format="multipart",
        )
        self.assertEqual(resp_b.status_code, 200, msg=resp_b.data)
        self.assertEqual(resp_b.data["version"], 2, "Version must increment on overwrite")

        bank_file = BankFile.objects.get(file_type="commercial_credit")
        self.assertEqual(bank_file.version, 2)

        # Physical path must end strictly with commercial_credit.csv (no hash suffix)
        path = bank_file.file.path
        self.assertTrue(
            path.endswith("commercial_credit.csv"),
            f"Strict Overwrite requires exact filename; got path: {path}",
        )

        # Read file from disk: must contain new_data (old file was deleted and replaced)
        with open(path, "rb") as f:
            on_disk = f.read()
        self.assertIn(b"new_data", on_disk, "On-disk file must be the new content after overwrite")
        self.assertNotIn(b"old_data", on_disk, "Old content must no longer exist on disk")

    # -------------------------------------------------------------------------
    # 3. Data Mutation (Update)
    # -------------------------------------------------------------------------

    def test_update_mutation_increments_version_and_decreases_balances(self):
        """
        POST /api/update/ mutates the CSV by randomly DECREASING 
        outstanding_principal_balance values (simulating payments), and 
        increments the BankFile version. We verify version increments and 
        that at least some balances decreased.
        """
        # Upload a file with known balance values
        csv_with_balances = (
            b"loan_account_number;outstanding_principal_balance\n"
            b"LOAN_001;5000\n"
            b"LOAN_002;3000\n"
            b"LOAN_003;8000\n"
            b"LOAN_004;2000\n"
            b"LOAN_005;6000\n"
        )
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    csv_with_balances,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        bank_file = BankFile.objects.get(file_type="commercial_credit")
        initial_version = bank_file.version
        
        # Read original balances
        import pandas as pd
        original_df = pd.read_csv(bank_file.file.path, sep=';')
        original_balances = original_df['outstanding_principal_balance'].tolist()

        # Mutate the data
        response = self.client.post(
            "/api/update/",
            data={"file_type": "commercial_credit"},
            format="json",
        )
        self.assertEqual(response.status_code, 200, msg=response.data)
        self.assertIn("new_version", response.data)
        self.assertEqual(response.data["new_version"], initial_version + 1)

        bank_file.refresh_from_db()
        self.assertEqual(bank_file.version, initial_version + 1)
        
        # Read mutated balances and verify they decreased (not multiplied)
        mutated_df = pd.read_csv(bank_file.file.path, sep=';')
        mutated_balances = mutated_df['outstanding_principal_balance'].tolist()
        
        # The update view mutates up to 5 random rows by DECREASING balance
        # (subtracting 100-1000), so at least one balance should be lower
        self.assertNotEqual(
            original_balances,
            mutated_balances,
            "Update must change at least some balances"
        )
        
        # Verify that changed balances decreased (not increased/multiplied)
        for orig, mutated in zip(original_balances, mutated_balances):
            if orig != mutated:
                self.assertLess(
                    mutated,
                    orig,
                    f"Balance should decrease (payment simulation), but went from {orig} to {mutated}"
                )

    # -------------------------------------------------------------------------
    # 4. Multi-Tenant Download Logic
    # -------------------------------------------------------------------------

    def test_download_bank1_default_returns_unchanged_loan_id(self):
        """
        Scenario A (Bank1/Default): Request with tenant=BANK001 returns master
        data unchanged; Loan ID remains as in the file (e.g. 1000).
        
        Tests streaming response handling.
        """
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.valid_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK001"},
        )
        self.assertEqual(response.status_code, 200, msg=f"Got status {response.status_code}")
        
        # Decode streaming response
        data = self._decode_streaming_response(response)
        
        self.assertEqual(len(data), 1)
        self.assertEqual(str(data[0]["loan_account_number"]), "1000")
        self.assertEqual(data[0]["outstanding_principal_balance"], 100)

    def test_download_bank2_prefixes_loan_id_and_scales_amount(self):
        """
        Scenario B (Bank2): tenant=BANK002 must prefix Loan ID with 'B2-'
        and multiply outstanding_principal_balance by 1.5.
        
        Tests multi-tenant transformation with streaming.
        """
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.valid_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK002"},
        )
        self.assertEqual(response.status_code, 200)
        
        # Decode streaming response
        data = self._decode_streaming_response(response)
        
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["loan_account_number"], "B2-1000")
        self.assertEqual(data[0]["outstanding_principal_balance"], 150.0)

    def test_download_bank3_prefixes_loan_id(self):
        """
        Scenario C (Bank3): tenant=BANK003 must prefix Loan ID with 'B3-'.
        
        Tests multi-tenant transformation with streaming.
        """
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.valid_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK003"},
        )
        self.assertEqual(response.status_code, 200)
        
        # Decode streaming response
        data = self._decode_streaming_response(response)
        
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["loan_account_number"], "B3-1000")

    # -------------------------------------------------------------------------
    # 5. Edge Cases
    # -------------------------------------------------------------------------

    def test_upload_without_file_or_file_type_returns_400(self):
        """
        Upload endpoint must require both 'file' and 'file_type'.
        Missing either should return 400 with a clear error.
        """
        # No file, no file_type
        r1 = self.client.post("/api/upload/", data={}, format="multipart")
        self.assertEqual(r1.status_code, 400)
        self.assertIn("error", r1.data)

        # file_type but no file
        r2 = self.client.post(
            "/api/upload/",
            data={"file_type": "commercial_credit"},
            format="multipart",
        )
        self.assertEqual(r2.status_code, 400)

        # file but no file_type
        r3 = self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "x.csv",
                    self.sample_csv_content,
                    content_type="text/csv",
                ),
            },
            format="multipart",
        )
        self.assertEqual(r3.status_code, 400)

    def test_download_nonexistent_file_type_returns_404(self):
        """
        Requesting data for a file_type that was never uploaded must
        return 404 (File not found), not 500.
        """
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK001"},
        )
        self.assertEqual(response.status_code, 404, msg=response.data)
        self.assertIn("error", response.data)
    
    # -------------------------------------------------------------------------
    # 6. Streaming & Compression Tests (200MB Support)
    # -------------------------------------------------------------------------
    
    def test_gzip_compression_when_accept_encoding_header_present(self):
        """
        When client sends 'Accept-Encoding: gzip', the response should
        be gzip-compressed with Content-Encoding: gzip header.
        
        This reduces network transfer by ~70-80% for large JSON responses.
        """
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.valid_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        
        # Request with Accept-Encoding: gzip
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK001"},
            HTTP_ACCEPT_ENCODING="gzip",
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get('Content-Encoding'), 'gzip', 
                        "Response should be gzip-compressed when client accepts it")
        
        # Verify we can decode the gzipped response
        data = self._decode_streaming_response(response)
        self.assertEqual(len(data), 1)
        self.assertEqual(str(data[0]["loan_account_number"]), "1000")
    
    def test_no_gzip_compression_when_not_requested(self):
        """
        When client doesn't send 'Accept-Encoding: gzip', response
        should be sent uncompressed for compatibility.
        """
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    self.valid_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        
        # Request without Accept-Encoding header
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK001"},
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(response.get('Content-Encoding'), 'gzip',
                          "Response should NOT be gzipped when not requested")
        
        # Verify response is valid JSON
        data = self._decode_streaming_response(response)
        self.assertEqual(len(data), 1)
    
    def test_large_file_streaming_handles_multiple_chunks(self):
        """
        Test that large files are processed in chunks (not loaded entirely
        into memory). This simulates a 200MB+ scenario.
        
        We create a CSV with enough rows to exceed the chunk_size (10000 rows)
        and verify multi-tenant transformations work correctly across chunks.
        """
        # Generate CSV with 15000 rows (exceeds default chunk_size of 10000)
        rows = ["loan_account_number;outstanding_principal_balance"]
        for i in range(15000):
            rows.append(f"LOAN_{i:06d};{1000 + i}")
        
        large_csv = "\n".join(rows).encode('utf-8')
        
        self.client.post(
            "/api/upload/",
            data={
                "file": SimpleUploadedFile(
                    "commercial_credit.csv",
                    large_csv,
                    content_type="text/csv",
                ),
                "file_type": "commercial_credit",
            },
            format="multipart",
        )
        
        # Download with BANK002 transformations (should prefix all IDs with B2-)
        response = self.client.get(
            "/api/data/",
            {"file_type": "commercial_credit", "tenant": "BANK002"},
            HTTP_ACCEPT_ENCODING="gzip",  # Use gzip for faster transfer
        )
        
        self.assertEqual(response.status_code, 200)
        data = self._decode_streaming_response(response)
        
        # Verify we got all 15000 records
        self.assertEqual(len(data), 15000, "All records should be returned via streaming")
        
        # Verify transformations work on first chunk (row 0)
        self.assertEqual(data[0]["loan_account_number"], "B2-LOAN_000000")
        self.assertEqual(data[0]["outstanding_principal_balance"], 1000 * 1.5)
        
        # Verify transformations work on second chunk (row 10000+)
        self.assertEqual(data[10000]["loan_account_number"], "B2-LOAN_010000")
        self.assertEqual(data[10000]["outstanding_principal_balance"], 11000 * 1.5)
        
        # Verify transformations work on last row
        self.assertEqual(data[14999]["loan_account_number"], "B2-LOAN_014999")
        self.assertEqual(data[14999]["outstanding_principal_balance"], 15999 * 1.5)
