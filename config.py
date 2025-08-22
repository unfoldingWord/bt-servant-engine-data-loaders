from __future__ import annotations

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# Load variables from a local .env into the process environment (no error if missing).
load_dotenv()


class Config(BaseSettings):
    """Pydantic-based settings loaded from process environment."""

    openai_api_key: str = Field(default="", validation_alias="OPENAI_API_KEY")
    data_loaders_log_level: str = Field(default="info", validation_alias="DATA_LOADERS_LOG_LEVEL")
    servant_api_base_url: str = Field(default="", validation_alias="SERVANT_API_BASE_URL")
    servant_api_token: str = Field(default="", validation_alias="SERVANT_API_TOKEN")


# Single shared instance to import elsewhere
config = Config()
