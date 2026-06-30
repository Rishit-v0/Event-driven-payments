from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    stripe_api_key: str = "sk_test_xxx"

    order_db_url: str = "postgresql+asyncpg://app:app@localhost:5432/order_db"
    payment_db_url: str = "postgresql+asyncpg://app:app@localhost:5432/payment_db"
    inventory_db_url: str = "postgresql+asyncpg://app:app@localhost:5432/inventory_db"

    mock_payments: bool = False


settings = Settings()