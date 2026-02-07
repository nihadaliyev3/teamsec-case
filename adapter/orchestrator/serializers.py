from rest_framework import serializers
from .constants import LoanCategory


class SyncTriggerSerializer(serializers.Serializer):
    """Tenant is derived from X-API-Key; body only needs loan_category."""
    loan_category = serializers.ChoiceField(choices=[t.value for t in LoanCategory])
    force = serializers.BooleanField(default=True, required=False)