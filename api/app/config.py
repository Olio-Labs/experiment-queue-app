import json
import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Keys in the Secrets Manager JSON that map to Settings fields.
_SECRET_FIELD_NAMES = frozenset(
    {
        "airtable_api_key",
        "airtable_base_id",
        "google_experiment_calendar_id",
        "google_tech_calendar_id",
        "google_service_account_json",
    }
)


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    In production (ECS), set ``AWS_SECRET_NAME`` to a Secrets Manager
    secret ID.  The secret should be a JSON object whose keys match
    the field names listed in ``_SECRET_FIELD_NAMES``.  Values from
    the secret override environment variables / .env defaults.

    For local development, use a ``.env`` file as before.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    airtable_api_key: str = ""
    airtable_base_id: str = ""
    airtable_table_name: str = "experiment_queue"
    assignment_field_name: str = "assignment"

    # Table names
    experiments_copy_testing_table_name: str = "experiments"
    experiment_planner_copy_testing_table_name: str = "experiment_planner"
    boxes_table_name: str = "boxes"
    cages_table_name: str = "cages"

    # Google Calendar
    google_experiment_calendar_id: str = ""
    google_tech_calendar_id: str = ""
    google_service_account_file: str = "service-account-credentials.json"
    google_service_account_json: str = ""
    calendar_timezone: str = "America/Los_Angeles"

    # AWS
    aws_region: str = "us-west-2"
    aws_secret_name: str = ""

    # Logging
    log_level: str = "INFO"


def _load_secrets(s: Settings) -> Settings:
    """Overlay Secrets Manager values onto *s* when running in AWS."""
    if not s.aws_secret_name:
        return s

    import boto3

    client = boto3.client("secretsmanager", region_name=s.aws_region)
    try:
        resp = client.get_secret_value(SecretId=s.aws_secret_name)
        secret: dict[str, str] = json.loads(resp["SecretString"])
    except Exception:
        logger.exception("Failed to load secrets from %s", s.aws_secret_name)
        return s

    updates: dict[str, str] = {}
    for key, value in secret.items():
        field = key.lower()
        if field in _SECRET_FIELD_NAMES and value:
            updates[field] = value

    if updates:
        # Pydantic v2: model_copy creates a new instance with overrides.
        s = s.model_copy(update=updates)
        logger.info(
            "Loaded %d secret(s) from %s",
            len(updates),
            s.aws_secret_name,
        )

    return s


settings = _load_secrets(Settings())
