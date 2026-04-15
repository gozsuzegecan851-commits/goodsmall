import os


def _to_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


class Settings:
    project_name: str = os.getenv("PROJECT_NAME", "goodsmall")
    database_url: str = os.getenv("DATABASE_URL", "postgresql://goodsmall:goodsmall123@postgres:5432/goodsmall")
    backend_public_url: str = os.getenv("BACKEND_PUBLIC_URL", "http://localhost:8002")
    usdt_tron_api_base: str = os.getenv("USDT_TRON_API_BASE", "https://api.trongrid.io")
    trongrid_api_key: str = os.getenv("TRONGRID_API_KEY", "")
    usdt_trc20_contract: str = os.getenv("USDT_TRC20_CONTRACT", "")
    usdt_poll_seconds: int = int(os.getenv("USDT_POLL_SECONDS", "20"))
    logistics_provider: str = os.getenv("LOGISTICS_PROVIDER", "kuaidi100")
    kuaidi100_key: str = os.getenv("KUAIDI100_KEY", "")
    kuaidi100_customer: str = os.getenv("KUAIDI100_CUSTOMER", "")
    logistics_sync_seconds: int = int(os.getenv("LOGISTICS_SYNC_SECONDS", "1800"))
    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "")
    admin_session_secret: str = os.getenv("ADMIN_SESSION_SECRET", "")
    admin_session_days: int = max(1, int(os.getenv("ADMIN_SESSION_DAYS", "7")))
    internal_api_token: str = os.getenv("INTERNAL_API_TOKEN", "")
    payment_expire_minutes: int = max(5, int(os.getenv("PAYMENT_EXPIRE_MINUTES", "30")))
    payment_amount_offset_enabled: bool = _to_bool("PAYMENT_AMOUNT_OFFSET_ENABLED", True)
    payment_amount_offset_step: str = os.getenv("PAYMENT_AMOUNT_OFFSET_STEP", "0.001")
    payment_amount_offset_slots: int = max(1, int(os.getenv("PAYMENT_AMOUNT_OFFSET_SLOTS", "20")))
    bot_profile_auto_sync_enabled: bool = _to_bool("BOT_PROFILE_AUTO_SYNC_ENABLED", False)
    bot_profile_auto_sync_seconds: int = max(60, int(os.getenv("BOT_PROFILE_AUTO_SYNC_SECONDS", "3600")))
    bot_profile_auto_sync_scope: str = os.getenv("BOT_PROFILE_AUTO_SYNC_SCOPE", "enabled").strip().lower() or "enabled"
    bot_profile_auto_sync_bot_type: str = os.getenv("BOT_PROFILE_AUTO_SYNC_BOT_TYPE", "all").strip().lower() or "all"


settings = Settings()
