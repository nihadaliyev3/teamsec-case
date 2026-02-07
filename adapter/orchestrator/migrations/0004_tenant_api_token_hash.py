# Generated migration for api_token_hash

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('orchestrator', '0003_syncjob_loan_category_syncjob_remote_version_credit_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='tenant',
            name='api_token_hash',
            field=models.CharField(blank=True, db_index=True, max_length=64, null=True),
        ),
    ]
