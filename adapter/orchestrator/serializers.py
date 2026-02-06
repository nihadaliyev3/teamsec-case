from rest_framework import serializers
from .models import Tenant, SyncJob
from .constants import LoanCategory

class SyncTriggerSerializer(serializers.Serializer):
    tenant_id = serializers.CharField(required=True)
    loan_category = serializers.ChoiceField(choices=[t.value for t in LoanCategory])

    def validate_tenant_id(self, value):
        try:
            return Tenant.objects.get(tenant_id=value)
        except Tenant.DoesNotExist:
            raise serializers.ValidationError(f"Tenant {value} does not exist.")