"""
Custom DRF permissions for tenant-scoped API.
"""
from rest_framework import permissions


class IsTenantAuthenticated(permissions.BasePermission):
    """
    Requires successful TenantAPIKeyAuthentication.
    request.auth is the Tenant instance when authenticated.
    """
    def has_permission(self, request, view):
        return request.auth is not None
