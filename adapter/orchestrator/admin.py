from django.contrib import admin
from .models import Tenant, SyncJob, SyncReport

@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ('name', 'slug', 'api_url', 'is_active')
    search_fields = ('name', 'slug')

@admin.register(SyncJob)
class SyncJobAdmin(admin.ModelAdmin):
    list_display = ('tenant', 'status', 'started_at', 'completed_at')
    list_filter = ('status', 'tenant')
    search_fields = ('tenant__name', 'error_message')


@admin.register(SyncReport)
class SyncReportAdmin(admin.ModelAdmin):
    # These columns will show up in the main list
    list_display = ('id', 'job', 'total_rows_processed', 'created_at')
    
    # Allows you to filter by date or specific job in the sidebar
    list_filter = ('created_at', 'job')
    
    # Makes the job ID clickable to go to the Job details
    raw_id_fields = ('job',)
    
    # Useful for reading the JSON stats easily
    readonly_fields = ('profiling_stats', 'validation_errors', 'created_at')