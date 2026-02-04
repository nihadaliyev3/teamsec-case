from django.shortcuts import render

# Create your views here.
import json
import os
import random
import zlib

import pandas as pd
from django.conf import settings
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework import status
from .models import BankFile

class UploadDataView(APIView):
    """
    Requirement: "CSV dosya yükleme endpoint'i"
    Resets the simulator with new data (e.g. the homework files).
    """
    parser_classes = [MultiPartParser]

    def post(self, request):
        file_obj = request.FILES.get('file')
        file_type = request.data.get('file_type')

        if not file_obj or not file_type:
            return Response({"error": "File and file_type required"}, status=400)

        # 1. Define the EXACT path we want.
        # e.g. /app/external_bank/media/bank_files/commercial_credit.csv
        target_filename = f"{file_type}.csv"
        # Ensure directory exists
        upload_dir = os.path.join(settings.MEDIA_ROOT, 'bank_files')
        os.makedirs(upload_dir, exist_ok=True)
        
        target_path = os.path.join(upload_dir, target_filename)

        # 2. Aggressive Cleanup: If a file exists at that exact path, delete it.
        if os.path.exists(target_path):
            os.remove(target_path)
            print(f"DEBUG: Physically deleted old file at {target_path}")

        # 3. Force the incoming file to have the correct name
        # This tells Django: "Save this as commercial_credit.csv"
        file_obj.name = target_filename

        # 4. Get or Create the DB record
        bank_file, created = BankFile.objects.get_or_create(file_type=file_type)

        # 5. Assign and Save
        # Since we deleted the physical file in step 2, Django sees the path is free 
        # and will write the file without adding a random suffix.
        bank_file.file = file_obj
        
        if not created:
            bank_file.version += 1
        
        bank_file.save()

        action = "Created" if created else "Overwritten"
        return Response({"status": action, "version": bank_file.version, "file_path": bank_file.file.name})


class UpdateDataView(APIView):
    """
    Requirement: "Güncel veriyi güncelleme endpointi"
    Simulates a "Day Passing". It randomly modifies 5 rows in the CSV 
    to prove your Adapter can detect changes.
    """
    def post(self, request):
        file_type = request.data.get('file_type', 'commercial_credit')
        
        try:
            bank_file = BankFile.objects.get(file_type=file_type)
            file_path = bank_file.file.path
            
            # Read CSV
            df = pd.read_csv(file_path, sep=';')
            
            # --- THE MODIFICATION LOGIC ---
            # Randomly change 'outstanding_principal_balance' of 5 random rows
            if not df.empty:
                random_indices = df.sample(n=min(5, len(df))).index
                for idx in random_indices:
                    # Decrease balance by random amount (Simulate payment)
                    current_val = pd.to_numeric(df.at[idx, 'outstanding_principal_balance'], errors='coerce') or 0
                    df.at[idx, 'outstanding_principal_balance'] = max(0, current_val - random.randint(100, 1000))
            
            # Save back to disk
            df.to_csv(file_path, sep=';', index=False)
            
            # Update version
            bank_file.version += 1
            bank_file.save()
            
            return Response({"status": "Data mutated", "new_version": bank_file.version, "changed_rows": str(random_indices)})
            
        except BankFile.DoesNotExist:
            return Response({"error": "File not found"}, status=404)
        except Exception as e:
             return Response({"error": str(e)}, status=500)


class DataDownloadView(APIView):
    """
    Requirement: "Güncel veriyi JSON olarak dönen endpoint"
    
    Optimized for large files (200MB+) using:
    - Streaming response (chunked processing)
    - gzip compression (~70-80% size reduction)
    - Memory-efficient pandas chunk iteration
    """
    
    def _apply_tenant_transformation(self, chunk_df, tenant_id):
        """
        Apply multi-tenant transformations to a DataFrame chunk.
        
        Args:
            chunk_df: pandas DataFrame chunk
            tenant_id: Tenant identifier (BANK001, BANK002, BANK003)
            
        Returns:
            Transformed DataFrame chunk
        """
        if tenant_id == 'BANK002':
            # Rule: Prefix Loan IDs with 'B2-' and scale amounts
            if 'loan_account_number' in chunk_df.columns:
                chunk_df['loan_account_number'] = 'B2-' + chunk_df['loan_account_number'].astype(str)
            if 'outstanding_principal_balance' in chunk_df.columns:
                # Simulate different currency or scale
                chunk_df['outstanding_principal_balance'] = chunk_df['outstanding_principal_balance'] * 1.5
                
        elif tenant_id == 'BANK003':
            # Rule: Prefix Loan IDs with 'B3-'
            if 'loan_account_number' in chunk_df.columns:
                chunk_df['loan_account_number'] = 'B3-' + chunk_df['loan_account_number'].astype(str)
                
        # BANK001 returns master data unchanged
        return chunk_df
    
    def _stream_json_chunks(self, file_path, tenant_id, chunk_size=10000):
        """
        Generator that yields JSON array chunks from CSV file.
        
        Processes CSV in chunks to avoid loading entire file into memory.
        
        Args:
            file_path: Path to CSV file
            tenant_id: Tenant identifier for transformations
            chunk_size: Number of rows to process per chunk (default: 10000)
            
        Yields:
            JSON string chunks forming a valid JSON array
        """
        # Start JSON array
        yield '['
        
        first_chunk = True
        
        # Read CSV in chunks to minimize memory usage
        for chunk_df in pd.read_csv(file_path, sep=';', chunksize=chunk_size):
            # Apply tenant-specific transformations
            chunk_df = self._apply_tenant_transformation(chunk_df, tenant_id)
            
            # Replace NaN with None for proper JSON null serialization
            chunk_df = chunk_df.where(pd.notnull(chunk_df), None)
            
            # Convert chunk to list of dicts
            records = chunk_df.to_dict(orient='records')
            
            # Serialize to JSON
            for record in records:
                if not first_chunk:
                    yield ','  # Add comma separator between records
                yield json.dumps(record)
                first_chunk = False
        
        # Close JSON array
        yield ']'
    
    def _gzip_compress_stream(self, stream_generator):
        """
        Generator that incrementally compresses streamed data with gzip.
        
        Uses zlib.compressobj() for true streaming compression without
        buffering large amounts of data in memory.
        
        Args:
            stream_generator: Generator yielding string chunks
            
        Yields:
            Compressed bytes chunks (gzip format)
        """
        # Create incremental compressor
        # wbits=16+15 produces gzip format (not raw deflate)
        # 16 = gzip wrapper, 15 = max window size
        compressor = zlib.compressobj(
            level=6,  # Compression level (1=fast, 9=best, 6=default)
            method=zlib.DEFLATED,
            wbits=16 + zlib.MAX_WBITS  # gzip format
        )
        
        # Compress each chunk incrementally
        for chunk in stream_generator:
            chunk_bytes = chunk.encode('utf-8')
            compressed = compressor.compress(chunk_bytes)
            
            # Yield compressed data immediately (if any)
            # Note: compress() may not return data on every call (internal buffering)
            if compressed:
                yield compressed
        
        # Flush remaining compressed data from the compressor
        final_chunk = compressor.flush()
        if final_chunk:
            yield final_chunk
    
    def get(self, request):
        file_type = request.query_params.get('file_type')
        tenant_id = request.query_params.get('tenant')
        
        print(f"DEBUG: Request from {tenant_id} for {file_type}")

        # Validation
        if not file_type:
            return Response({"error": "file_type param required"}, status=400)
        if not tenant_id:
            return Response({"error": "tenant param required"}, status=400)
        if tenant_id not in ['BANK001', 'BANK002', 'BANK003']:
            return Response({"error": "invalid tenant"}, status=400)

        try:
            bank_file = BankFile.objects.get(file_type=file_type)
            file_path = bank_file.file.path
            
            # Check if client accepts gzip
            accept_encoding = request.META.get('HTTP_ACCEPT_ENCODING', '')
            use_gzip = 'gzip' in accept_encoding.lower()
            
            # Create streaming generator
            json_stream = self._stream_json_chunks(file_path, tenant_id)
            
            if use_gzip:
                # Compress the stream
                response_stream = self._gzip_compress_stream(json_stream)
                response = StreamingHttpResponse(
                    response_stream,
                    content_type='application/json'
                )
                response['Content-Encoding'] = 'gzip'
            else:
                # Return uncompressed stream
                response = StreamingHttpResponse(
                    json_stream,
                    content_type='application/json'
                )
            
            return response

        except BankFile.DoesNotExist:
            return Response({"error": "File not found"}, status=404)
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return Response({"error": str(e)}, status=500)