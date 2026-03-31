from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    airtable_api_key: str = ""
    airtable_base_id: str = ""
    airtable_table_name: str = "experiment_queue"
    assignment_field_name: str = "assignment"

    # Google Calendar
    google_experiment_calendar_id: str = ""
    google_tech_calendar_id: str = ""
    google_service_account_file: str = "service-account-credentials.json"
    calendar_timezone: str = "America/Los_Angeles"

    # AWS
    aws_region: str = "us-east-1"

    # Logging
    log_level: str = "INFO"


settings = Settings()
