from django.shortcuts import render

# Create your views here.
import pandas as pd
import os
import random
from django.conf import settings
from django.http import HttpResponse, JsonResponse
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
    Also supports CSV for the 200MB requirement.
    """
    def get(self, request):
        file_type = request.query_params.get('file_type')
        
        # 1. Who is asking? (We simulate this via a Header or Query Param)
        # In real life, this comes from the Auth Token. 
        tenant_id = request.query_params.get('tenant')
        
        print(f"DEBUG: Request from {tenant_id} for {file_type}")

        if not file_type:
            return Response({"error": "file_type param required"}, status=400)
        if not tenant_id:
            return Response({"error": "tenant param required"}, status=400)
        if tenant_id not in ['BANK001', 'BANK002', 'BANK003']:
            return Response({"error": "invalid tenant"}, status=400)

        try:
            bank_file = BankFile.objects.get(file_type=file_type)
            file_path = bank_file.file.path

            # --- 2. MULTI-TENANT TRANSFORMATION LOGIC ---
            # We assume the file on disk is the "Master" (Bank 1).
            # If Bank 2 or 3 asks, we mutate the data on the fly.
            
            df = pd.read_csv(file_path, sep=';')

            if tenant_id == 'BANK002':
                # Rule: Prefix Loan IDs with 'B2-' and scale amounts
                if 'loan_account_number' in df.columns:
                    df['loan_account_number'] = 'B2-' + df['loan_account_number'].astype(str)
                if 'outstanding_principal_balance' in df.columns:
                     # Simulate different currency or scale
                    df['outstanding_principal_balance'] = df['outstanding_principal_balance'] * 1.5

            elif tenant_id == 'BANK003':
                 # Rule: Prefix Loan IDs with 'B3-'
                if 'loan_account_number' in df.columns:
                    df['loan_account_number'] = 'B3-' + df['loan_account_number'].astype(str)

            # --- 3. RESPONSE GENERATION ---

            df = df.where(pd.notnull(df), None)
            data = df.to_dict(orient='records')
            return JsonResponse(data, safe=False)
            

        except BankFile.DoesNotExist:
            return Response({"error": "File not found"}, status=404)
        except Exception as e:
            print(f"Error: {e}")
            return Response({"error": str(e)}, status=500)