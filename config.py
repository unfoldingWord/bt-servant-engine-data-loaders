from pydantic_settings import BaseSettings
from pydantic import Field


class Config(BaseSettings):
    OPENAI_API_KEY: str = Field(..., env="OPENAI_API_KEY")
    DATA_LOADERS_LOG_LEVEL: str = Field(default="info", env="DATA_LOADERS_LOG_LEVEL")

    class Config:
        env_file = ".env"


# Create a single instance to import elsewhere
config = Config()
