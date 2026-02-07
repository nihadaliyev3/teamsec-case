from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from .constants import LoanCategory

from .serializers import SyncTriggerSerializer
from .tasks import trigger_sync_logic
# Create your views here.
# adapter/orchestrator/views.py

class SyncTriggerView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        serializer = SyncTriggerSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        tenant = serializer.validated_data['tenant_id']
        category = serializer.validated_data['loan_category']

        # Convert string back to Enum if needed, or pass string if tasks handles it
        # tasks.py trigger_sync_logic expects LoanCategory enum member
        category_enum = LoanCategory(category)
        
        # Reuse the logic with force=True
        job_id = trigger_sync_logic(tenant, category_enum, force=True)

        if job_id:
            return Response({
                "message": "Sync job started.", 
                "job_id": job_id
            }, status=status.HTTP_202_ACCEPTED)
        else:
            # Logic returned None (likely API error or already running)
            return Response({
                "error": "Could not start job. Check if External Bank API is up or job is already running."
            }, status=status.HTTP_409_CONFLICT)