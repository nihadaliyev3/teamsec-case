from django.db import models

from .constants import SYNC_JOB_STATUS_CHOICES, SyncJobStatus


class Tenant(models.Model):
    tenant_id = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=100)
    slug = models.SlugField(unique=True, help_text="Unique identifier (e.g., 'bank001')")
    
    # Connection Details
    # Note: simulator is the docker service name. 
    # Example URL: http://simulator:8000
    api_url = models.URLField(help_text="Base URL of the external bank API")
    api_token = models.CharField(max_length=255, blank=True, null=True)
    
    is_active = models.BooleanField(default=True)
    config = models.JSONField(default=dict, blank=True, help_text="Extra config (mappings, timezone, etc.)")

    def __str__(self):
        return self.name


class SyncJob(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE, related_name='jobs')
    status = models.CharField(
        max_length=20,
        choices=SYNC_JOB_STATUS_CHOICES,
        default=SyncJobStatus.PENDING.value,
    )
    
    # Observability
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    # Store results (e.g., {"rows_inserted": 5000})
    result_summary = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['status']),
        ]

    def __str__(self):
        return f"{self.tenant.slug} - {self.run_date} ({self.status})"


class SyncReport(models.Model):
    """
    Detailed logs and stats for a specific SyncJob.
    Separated from SyncJob to keep the main table light.
    """
    job = models.OneToOneField(SyncJob, on_delete=models.CASCADE, related_name='report')
    
    # Detailed logs (e.g. specific validation errors for rows)
    log_message = models.JSONField(default=dict, blank=True, help_text="Structured log of events/errors")
    
    # Performance metrics (e.g. "extract_time": 0.5s, "load_time": 2.0s)
    profiling_stats = models.JSONField(default=dict, blank=True, help_text="Performance breakdown, Data profiling metrics")

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Report for Job {self.job.id}"