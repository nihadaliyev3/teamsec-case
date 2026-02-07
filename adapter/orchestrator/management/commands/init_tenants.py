from django.core.management.base import BaseCommand
from orchestrator.models import Tenant


def _tenant_id_from_slug(slug: str) -> str:
    """bank001 -> BANK001"""
    return slug.upper().replace("-", "_")


class Command(BaseCommand):
    help = "Initialize default tenants with generated API keys (hashed)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--regenerate-keys",
            action="store_true",
            help="Regenerate API keys for existing tenants (new keys printed once)",
        )

    def handle(self, *args, **options):
        tenants_config = [
            {"name": "Bank 1 (Standard)", "slug": "bank001", "api_url": "http://external-bank:8000/api/data"},
            {"name": "Bank 2 (High Volume)", "slug": "bank002", "api_url": "http://external-bank:8000/api/data"},
            {"name": "Bank 3 (Foreign)", "slug": "bank003", "api_url": "http://external-bank:8000/api/data"},
        ]

        regenerate = options.get("regenerate_keys", False)

        for t in tenants_config:
            tenant_id = _tenant_id_from_slug(t["slug"])
            tenant, created = Tenant.objects.get_or_create(
                tenant_id=tenant_id,
                defaults={
                    "name": t["name"],
                    "slug": t["slug"],
                    "api_url": t["api_url"],
                    "is_active": True,
                },
            )
            if created:
                raw_token = Tenant.generate_api_token()
                tenant.set_api_token_hash(raw_token)
                self.stdout.write(
                    self.style.SUCCESS(f"Created tenant: {t['name']} (tenant_id={tenant_id})")
                )
                self.stdout.write(
                    self.style.WARNING(f"  API Key (save it, won't be shown again): {raw_token}")
                )
            elif regenerate:
                raw_token = Tenant.generate_api_token()
                tenant.set_api_token_hash(raw_token)
                self.stdout.write(self.style.WARNING(f"Regenerated key for {t['name']}: {raw_token}"))
            else:
                self.stdout.write(f"Tenant {t['name']} already exists (use --regenerate-keys to rotate)")
