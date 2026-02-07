"""
DRF authentication: X-API-Key header validated against Tenant.api_token_hash.
"""
from rest_framework import authentication, exceptions

from .auth import hash_token
from .models import Tenant


class TenantAPIKeyAuthentication(authentication.BaseAuthentication):
    """
    Validates X-API-Key header against Tenant.api_token_hash.
    Sets request.user = None, request.tenant = Tenant instance.
    """
    keyword = "Api-Key"
    header = "HTTP_X_API_KEY"

    def authenticate(self, request):
        raw_token = request.META.get(self.header) or request.headers.get("X-API-Key")
        if not raw_token:
            raise exceptions.AuthenticationFailed("Missing X-API-Key header")

        token_hash = hash_token(raw_token)
        try:
            tenant = Tenant.objects.get(api_token_hash=token_hash, is_active=True)
        except Tenant.DoesNotExist:
            raise exceptions.AuthenticationFailed("Invalid API key")

        # DRF expects (user, auth); we use tenant as auth for permission checks
        return (None, tenant)
