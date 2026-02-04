from django.core.management.base import BaseCommand
from orchestrator.models import Tenant

class Command(BaseCommand):
    help = 'Initialize default tenants for the simulator'

    def handle(self, *args, **kwargs):
        tenants = [
            {
                "name": "Bank 1 (Standard)",
                "slug": "bank001",
                "api_url": "http://simulator:8000",
                "is_active": True
            },
            {
                "name": "Bank 2 (High Volume)",
                "slug": "bank002",
                "api_url": "http://simulator:8000",
                "is_active": True
            },
            {
                "name": "Bank 3 (Foreign)",
                "slug": "bank003",
                "api_url": "http://simulator:8000",
                "is_active": True
            },
        ]

        for t in tenants:
            tenant, created = Tenant.objects.get_or_create(
                slug=t['slug'],
                defaults={
                    "name": t['name'],
                    "api_url": t['api_url'],
                    "is_active": t['is_active']
                }
            )
            if created:
                self.stdout.write(self.style.SUCCESS(f'Created tenant: {t["name"]}'))
            else:
                self.stdout.write(f'Tenant {t["name"]} already exists')