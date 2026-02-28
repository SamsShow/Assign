from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_host: str = os.getenv("DB_HOST", "172.105.61.195")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_name: str = os.getenv("DB_NAME", "dedup_infollion")
    db_user: str = os.getenv("DB_USER", "intern")
    db_password: str = os.getenv("DB_PASSWORD", "")

    openrouter_api_key: str = os.getenv("OPENROUTER_API_KEY", "")
    openrouter_model: str = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")
    openrouter_site_url: str = os.getenv("OPENROUTER_SITE_URL", "")
    openrouter_app_name: str = os.getenv("OPENROUTER_APP_NAME", "company-dedupe-assignment")

    auto_duplicate_threshold: float = float(os.getenv("AUTO_DUPLICATE_THRESHOLD", "93.0"))
    probable_threshold: float = float(os.getenv("PROBABLE_THRESHOLD", "78.0"))
    min_block_token_len: int = int(os.getenv("MIN_BLOCK_TOKEN_LEN", "3"))


def get_settings() -> Settings:
    settings = Settings()
    if not settings.db_password:
        raise ValueError("DB_PASSWORD is required. Set it in environment or .env file.")
    return settings
