from celery import Celery
import clickhouse_connect
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .config import settings

# 1. ClickHouse Client (Singleton)
class ClickHouse:
    _instance = None
    @classmethod
    def get(cls):
        if not cls._instance:
            cls._instance = clickhouse_connect.get_client(
                host=settings.DWH_HOST,
                port=settings.DWH_PORT, 
                compress=True
            )
        return cls._instance

# 2. Celery Producer (For Triggering Syncs)
# We don't need the whole worker, just a client to send messages.
celery_app = Celery('gateway', broker=settings.REDIS_URL)

# 3. Postgres Engine (For Reading Reports)
pg_engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(pg_engine, class_=AsyncSession, expire_on_commit=False)

async def get_pg_session():
    async with AsyncSessionLocal() as session:
        yield session