import clickhouse_connect
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class ClickHouseClient:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.client = None

    def connect(self):
        if not self.client:
            try:
                self.client = clickhouse_connect.get_client(
                    host=settings.CLICKHOUSE_HOST,
                    port=settings.CLICKHOUSE_PORT,
                    username=settings.CLICKHOUSE_USER,
                    password=settings.CLICKHOUSE_PASSWORD,
                    database=settings.CLICKHOUSE_DB
                )
            except Exception as e:
                logger.error(f"ClickHouse connection failed: {e}")
                raise ConnectionError(f"ClickHouse unavailable: {e}")
        return self.client

    def init_tables(self):
        client = self.connect()

        # 1. Unified Credits Table
        credits_sql = """
        CREATE TABLE IF NOT EXISTS credits_all (
            tenant_id String,
            loan_type String,
            loan_account_number String,
            customer_id String,
            customer_type String,
            
            -- Loan Details
            loan_product_type String,      
            loan_status_code String,
            loan_status_flag String,      
            days_past_due Int32,
            final_maturity_date Date,
            
            -- Installment Info
            total_installment_count Int32,
            outstanding_installment_count Int32,
            paid_installment_count Int32,
            installment_frequency Int32,
            grace_period_months Int32,
            
            -- Dates
            first_payment_date Date,
            loan_start_date Date,
            loan_closing_date Nullable(Date),
            
            -- Financials
            original_loan_amount Decimal(18, 4),
            outstanding_principal_balance Decimal(18, 4),
            nominal_interest_rate Decimal(10, 6),
            total_interest_amount Decimal(18, 4),
            kkdf_rate Decimal(10, 6),
            kkdf_amount Decimal(18, 4),
            bsmv_rate Decimal(10, 6),
            bsmv_amount Decimal(18, 4),
            
            -- Ratings & Risk
            internal_rating Nullable(String),
            internal_credit_rating Nullable(String), 
            external_rating Nullable(String),
            default_probability Nullable(Decimal(10, 6)), 
            risk_class Nullable(String),
            
            -- Demographics / Segments
            sector_code Nullable(String),
            customer_segment Nullable(String),    
            customer_province_code Nullable(String),
            customer_district_code Nullable(String), 
            customer_region_code Nullable(String), 
            insurance_included Nullable(String),

            inserted_at DateTime DEFAULT now()
        ) 
        ENGINE = MergeTree()
        PARTITION BY (tenant_id, loan_type)
        ORDER BY (loan_account_number)
        """

        # 2. Unified Payments Table
        payments_sql = """
        CREATE TABLE IF NOT EXISTS payments_all (
            tenant_id String,
            loan_type String,
            loan_account_number String,
            installment_number Int32,
            actual_payment_date Nullable(Date),
            scheduled_payment_date Date,
            installment_amount Decimal(18, 4),
            principal_component Decimal(18, 4),
            interest_component Decimal(18, 4),
            kkdf_component Decimal(18, 4),
            bsmv_component Decimal(18, 4),
            installment_status String,
            
            remaining_principal Decimal(18, 4),
            remaining_interest Decimal(18, 4),
            remaining_kkdf Decimal(18, 4),  -- ADDED
            remaining_bsmv Decimal(18, 4),  -- ADDED

            inserted_at DateTime DEFAULT now()
        ) 
        ENGINE = MergeTree()
        PARTITION BY (tenant_id, loan_type)
        ORDER BY (loan_account_number, installment_number)
        """

        client.command(credits_sql)
        client.command(payments_sql)
        logger.info("ClickHouse: Unified tables initialized with COMPLETE schema.")

    def prepare_staging(self, tenant_id, loan_type, base_table):
        """
        Creates an empty staging table for the specific sync.
        base_table: 'credits_all' or 'payments_all'
        """
        stg_name = f"stg_{tenant_id.lower()}_{loan_type.lower()}_{base_table.split('_')[0]}"
        self.connect().command(f"DROP TABLE IF EXISTS {stg_name}")
        self.connect().command(f"CREATE TABLE {stg_name} AS {base_table}")
        return stg_name

    def swap_partition(self, tenant_id, loan_type, stg_table, base_table):
        """
        Replaces ONLY the partition for this tenant/loan_type in the main table.
        """
        sql = f"""
        ALTER TABLE {base_table} 
        REPLACE PARTITION ('{tenant_id}', '{loan_type}') 
        FROM {stg_table}
        """
        self.connect().command(sql)
        self.connect().command(f"DROP TABLE {stg_table}")
        logger.info(f"Atomic swap complete: {tenant_id} {loan_type} -> {base_table}")

ch_client = ClickHouseClient.get_instance()