from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://relay:relay@db:5432/relay"
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic: str = "relay.events.v1"
    api_key: str = ""
    max_payload_bytes: int = 65536
    max_attempts: int = 6
    production: bool = False
    allow_private_targets: bool = False
    allowed_hosts: str = ""

    def validate_security(self) -> None:
        if len(self.api_key) < 24:
            raise ValueError("API_KEY must contain at least 24 characters")
        if self.production and self.allow_private_targets:
            raise ValueError("private webhook targets are forbidden in production")
        if self.production and not self.allowed_hosts:
            raise ValueError("ALLOWED_HOSTS is required in production")


settings = Settings()
