from django.contrib import admin
from .models import Tenant, SyncJob

@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug', 'api_url', 'is_active')
    search_fields = ('name', 'slug')

@admin.register(SyncJob)
class SyncJobAdmin(admin.ModelAdmin):
    list_display = ('tenant', 'status', 'started_at', 'completed_at')
    list_filter = ('status', 'tenant')
    search_fields = ('tenant__name', 'error_message')