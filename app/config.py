from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # DO Spaces — no defaults, must be set in environment
    DO_SPACES_KEY: str
    DO_SPACES_SECRET: str
    DO_SPACES_ENDPOINT: str
    DO_SPACES_BUCKET: str
    DO_SPACES_REGION: str

    # Buffer
    BUFFER_FLUSH_INTERVAL_SECONDS: int = 30
    BUFFER_MAX_SIZE: int = 5000
    MAX_FLUSH_RETRIES: int = 3

    # Logging
    LOG_LEVEL: str = "INFO"


settings = Settings()
