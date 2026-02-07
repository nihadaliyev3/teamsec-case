"""
API config - reads from environment (same vars as adapter).
Use .env locally; Docker injects via env_file. Never commit .env.
"""
import os

from dotenv import load_dotenv

load_dotenv()


PROJECT_NAME = os.environ.get("PROJECT_NAME", "TeamSec Fintech Gateway")

# ClickHouse (Data Warehouse) - same vars as adapter
DWH_HOST = os.environ.get("DWH_HOST", "db_dwh")
DWH_PORT = int(os.environ.get("DWH_PORT", "8123"))

# Postgres - same vars as adapter
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
DB_HOST = os.environ.get("DB_HOST", "db_ops")
DB_PORT = os.environ.get("DB_PORT", "5432")

# Redis - same as adapter
REDIS_URL = os.environ.get("REDIS_URL")

# Adapter service (for proxying sync trigger)
ADAPTER_URL = os.environ.get("ADAPTER_URL", "http://adapter:8000")


def get_database_url() -> str:
    return f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DB_HOST}:{DB_PORT}/{POSTGRES_DB}"


# Settings object for backwards compatibility (routes, main, infrastructure)
class Settings:
    PROJECT_NAME = PROJECT_NAME
    DWH_HOST = DWH_HOST
    DWH_PORT = DWH_PORT
    POSTGRES_USER = POSTGRES_USER
    POSTGRES_PASSWORD = POSTGRES_PASSWORD
    POSTGRES_DB = POSTGRES_DB
    DB_HOST = DB_HOST
    DB_PORT = DB_PORT
    REDIS_URL = REDIS_URL
    ADAPTER_URL = ADAPTER_URL

    @property
    def DATABASE_URL(self) -> str:
        return get_database_url()


settings = Settings()
