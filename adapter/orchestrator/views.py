from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .constants import LoanCategory
from .permissions import IsTenantAuthenticated
from .serializers import SyncTriggerSerializer
from .tasks import trigger_sync_logic


class SyncTriggerView(APIView):
    """
    POST /api/sync - Triggers sync for the tenant identified by X-API-Key.
    Tenant is derived from the API key; body only contains loan_category and optional force.
    """
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        tenant = request.auth  # Set by TenantAPIKeyAuthentication

        serializer = SyncTriggerSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        category = serializer.validated_data["loan_category"]
        force = serializer.validated_data.get("force", True)
        category_enum = LoanCategory(category)

        job_id = trigger_sync_logic(tenant, category_enum, force=force)

        if job_id:
            return Response(
                {"message": "Sync job started.", "job_id": job_id},
                status=status.HTTP_202_ACCEPTED,
            )
        return Response(
            {
                "error": "Could not start job. Check if External Bank API is up or job is already running."
            },
            status=status.HTTP_409_CONFLICT,
        )
