import time
from functools import wraps
from django.apps import apps

def profile_sync_step(step_name):
    """
    Times a function and saves the result to the linked SyncReport.
    Auto-detects if 'self' is the Service or the Job itself.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(instance, *args, **kwargs):
            # 1. Start Timer
            start_time = time.perf_counter()
            
            # Run the actual logic
            result = func(instance, *args, **kwargs)
            
            # 2. Stop Timer
            end_time = time.perf_counter()
            duration = round(end_time - start_time, 4)

            # 3. Resolve the SyncJob instance
            # If 'instance' is the Service (e.g. BankSyncService), it has .job
            if hasattr(instance, 'job'):
                job_instance = instance.job
            # If 'instance' is the Model itself (unlikely but possible)
            else:
                job_instance = instance

            # 4. Get or Create the SyncReport safely
            # We use get_model to avoid circular imports
            SyncReport = apps.get_model('orchestrator', 'SyncReport')
            
            # This creates the report row if it doesn't exist yet
            report, created = SyncReport.objects.get_or_create(job=job_instance)

            # 5. Update the JSON field
            # Ensure it's a dict (in case it was somehow None)
            if report.profiling_stats is None:
                report.profiling_stats = {}

            report.profiling_stats[f"{step_name}_time_sec"] = duration
            
            # Save only the stats field to be fast
            report.save(update_fields=['profiling_stats'])

            return result
        return wrapper
    return decorator