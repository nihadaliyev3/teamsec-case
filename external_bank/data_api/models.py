from django.db import models

class BankFile(models.Model):
    FILE_TYPES = [
        ('commercial_credit', 'Commercial Credit'),
        ('commercial_payment', 'Commercial Payment'),
        ('retail_credit', 'Retail Credit'),
        ('retail_payment', 'Retail Payment'),
    ]
    
    file_type = models.CharField(max_length=50, choices=FILE_TYPES, unique=True)
    file = models.FileField(upload_to='bank_files/')
    updated_at = models.DateTimeField(auto_now=True)
    version = models.IntegerField(default=1)

    def __str__(self):
        return f"{self.file_type} (v{self.version})"