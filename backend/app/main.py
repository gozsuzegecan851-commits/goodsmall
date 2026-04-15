from pathlib import Path
import asyncio
import contextlib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from .config import settings
from .deps import (
    admin_login_configured,
    admin_request_authorized,
    get_internal_api_token_from_request,
    internal_api_token_matches,
)
from .db import Base, engine, SessionLocal
from .routes_public import router as public_router
from .routes_admin import router as admin_router, sync_bot_profiles_batch_once
from .jobs.usdt_watcher import poll_usdt_once
from .jobs.logistics_sync import sync_logistics_once
from .services.admin_user_service import ensure_bootstrap_admin

_MISSING_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c6360606060000000050001a5f645400000000049454e44ae426082"
)


class SafeStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            normalized = str(path or "").lstrip("/")
            if status_code == 404 and normalized.startswith("uploads/"):
                return Response(content=_MISSING_PNG, media_type="image/png", status_code=200)
            raise


app = FastAPI(title="goodsmall backend v1")

UPLOAD_DIR = Path(__file__).resolve().parent / "static" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", SafeStaticFiles(directory=str(UPLOAD_DIR.parent)), name="static")

_usdt_loop_task: asyncio.Task | None = None
_logistics_loop_task: asyncio.Task | None = None
_bot_profile_auto_sync_task: asyncio.Task | None = None

ADMIN_PUBLIC_PATHS = {
    "/admin/auth/check",
    "/admin/login",
}


@app.middleware("http")
async def admin_guard(request: Request, call_next):
    path = request.url.path or ""
    normalized = str(path or "").rstrip("/") or "/"

    if not normalized.startswith("/admin"):
        return await call_next(request)

    raw_internal_token = get_internal_api_token_from_request(request)
    if raw_internal_token and internal_api_token_matches(raw_internal_token):
        return await call_next(request)

    if normalized in ADMIN_PUBLIC_PATHS:
        return await call_next(request)

    if not admin_login_configured():
        return await call_next(request)

    if admin_request_authorized(request):
        return await call_next(request)

    if normalized == "/admin/ui":
        return RedirectResponse(url="/admin/login", status_code=302)

    return JSONResponse({"detail": "后台未授权，请先登录后台"}, status_code=401)


def ensure_schema():
    sqls = [
        "ALTER TABLE payment_addresses ADD COLUMN IF NOT EXISTS qr_image TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_remark TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS buyer_remark TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS stock_reserved BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS supplier_code VARCHAR(64) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS bot_name VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS bot_alias VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS bot_short_description VARCHAR(120) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS bot_description TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS avatar_image TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS telegram_username VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS last_profile_sync_at TIMESTAMP NULL",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS last_profile_sync_error TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS supplier_code VARCHAR(64) NOT NULL DEFAULT ''",
        "ALTER TABLE shipment_import_batches ADD COLUMN IF NOT EXISTS supplier_code VARCHAR(64) NOT NULL DEFAULT ''",
        "ALTER TABLE shipments ADD COLUMN IF NOT EXISTS sync_status VARCHAR(32) NOT NULL DEFAULT 'pending'",
        "ALTER TABLE shipments ADD COLUMN IF NOT EXISTS sync_error TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE shipments ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMP NULL",
        "ALTER TABLE product_import_batches ADD COLUMN IF NOT EXISTS warning_rows INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE product_import_batches ADD COLUMN IF NOT EXISTS created_rows INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE product_import_batches ADD COLUMN IF NOT EXISTS updated_rows INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE product_import_batches ADD COLUMN IF NOT EXISTS check_images BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS bot_type VARCHAR(32) NOT NULL DEFAULT 'buyer'",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS run_status VARCHAR(32) NOT NULL DEFAULT 'stopped'",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS status_text VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS instance_id VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMP NULL",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS started_at TIMESTAMP NULL",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMP NULL",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE bot_runtime_states ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NULL",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS telegram_chat_id VARCHAR(64) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS telegram_username VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS telegram_first_name VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS telegram_last_name VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS session_status VARCHAR(32) NOT NULL DEFAULT 'open'",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS unread_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS last_message_text TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS last_message_type VARCHAR(32) NOT NULL DEFAULT 'text'",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS first_customer_message_at TIMESTAMP NULL",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS last_customer_message_at TIMESTAMP NULL",
        "ALTER TABLE customer_sessions ADD COLUMN IF NOT EXISTS last_operator_reply_at TIMESTAMP NULL",
        "ALTER TABLE customer_messages ADD COLUMN IF NOT EXISTS sender_name VARCHAR(128) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_messages ADD COLUMN IF NOT EXISTS message_type VARCHAR(32) NOT NULL DEFAULT 'text'",
        "ALTER TABLE customer_messages ADD COLUMN IF NOT EXISTS content_json TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE customer_messages ADD COLUMN IF NOT EXISTS telegram_message_id VARCHAR(64) NOT NULL DEFAULT ''",
        "ALTER TABLE customer_messages ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NULL",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS keyword VARCHAR(255)",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS match_type VARCHAR(32)",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS match_mode VARCHAR(32)",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS remark TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NULL",
        "ALTER TABLE chat_keyword_blocks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NULL",
        "ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS base_amount NUMERIC(18, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE payment_orders ADD COLUMN IF NOT EXISTS amount_offset NUMERIC(18, 6) NOT NULL DEFAULT 0",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_mode VARCHAR(32) NOT NULL DEFAULT 'none'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_items_json TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS text_mode VARCHAR(32) NOT NULL DEFAULT 'caption_first'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS replace_start_welcome BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS fallback_mode VARCHAR(32) NOT NULL DEFAULT 'text_only'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_cache_json TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_normalize_status VARCHAR(32) NOT NULL DEFAULT 'pending'",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_normalize_error TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE announcement_configs ADD COLUMN IF NOT EXISTS media_cache_updated_at TIMESTAMP NULL",
        "UPDATE chat_keyword_blocks SET keyword = COALESCE(keyword, '')",
        "UPDATE chat_keyword_blocks SET match_type = COALESCE(NULLIF(match_type, ''), NULLIF(match_mode, ''), 'exact')",
        "UPDATE chat_keyword_blocks SET match_mode = COALESCE(NULLIF(match_mode, ''), NULLIF(match_type, ''), 'exact')",
        "UPDATE chat_keyword_blocks SET remark = COALESCE(remark, '')",
        "UPDATE chat_keyword_blocks SET is_active = COALESCE(is_active, TRUE)",
        "UPDATE chat_keyword_blocks SET created_at = COALESCE(created_at, NOW())",
        "UPDATE chat_keyword_blocks SET updated_at = COALESCE(updated_at, NOW())",
        "UPDATE payment_orders SET base_amount = COALESCE(NULLIF(base_amount, 0), expected_amount)",
        "UPDATE payment_orders SET amount_offset = COALESCE(amount_offset, 0)",
        "UPDATE announcement_configs SET media_mode = CASE WHEN COALESCE(media_mode, '') <> '' THEN media_mode WHEN COALESCE(media_url, '') <> '' AND COALESCE(media_type, '') = 'video' THEN 'single_video' ELSE 'none' END",
        "UPDATE announcement_configs SET media_items_json = COALESCE(NULLIF(media_items_json, ''), '[]')",
        "UPDATE announcement_configs SET text_mode = COALESCE(NULLIF(text_mode, ''), 'caption_first')",
        "UPDATE announcement_configs SET replace_start_welcome = COALESCE(replace_start_welcome, TRUE)",
        "UPDATE announcement_configs SET fallback_mode = COALESCE(NULLIF(fallback_mode, ''), 'text_only')",
        "UPDATE announcement_configs SET media_cache_json = COALESCE(NULLIF(media_cache_json, ''), '[]')",
        "UPDATE announcement_configs SET media_normalize_status = COALESCE(NULLIF(media_normalize_status, ''), 'pending')",
        "UPDATE announcement_configs SET media_normalize_error = COALESCE(media_normalize_error, '')",
    ]
    with engine.begin() as conn:
        for sql in sqls:
            conn.execute(text(sql))

    index_sqls = [
        "CREATE INDEX IF NOT EXISTS ix_orders_pay_delivery_created ON orders (pay_status, delivery_status, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_orders_supplier_created ON orders (supplier_code, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_orders_bot_user_created ON orders (bot_code, telegram_user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_orders_tracking_no ON orders (tracking_no)",
        "CREATE INDEX IF NOT EXISTS ix_payment_orders_status_created ON payment_orders (confirm_status, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_payment_orders_address_status_created ON payment_orders (receive_address, confirm_status, created_at DESC)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_orders_txid_nonempty ON payment_orders (txid) WHERE txid <> ''",
        "CREATE INDEX IF NOT EXISTS ix_shipments_status_sync ON shipments (ship_status, last_sync_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_shipments_tracking_no ON shipments (tracking_no)",
        "CREATE INDEX IF NOT EXISTS ix_customer_sessions_bot_status_updated ON customer_sessions (bot_code, session_status, updated_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_customer_sessions_unread_updated ON customer_sessions (unread_count, updated_at DESC)",
        "CREATE INDEX IF NOT EXISTS ix_customer_messages_session_created ON customer_messages (session_id, created_at DESC)",
    ]
    with engine.begin() as conn:
        for sql in index_sqls:
            conn.execute(text(sql))


async def usdt_poll_loop():
    while True:
        db = SessionLocal()
        try:
            result = await asyncio.to_thread(poll_usdt_once, db)
            if result.get("confirmed"):
                print(f"[usdt-watcher] confirmed={result['confirmed']} checked={result['checked']}")
        except Exception as e:
            print(f"[usdt-watcher] loop error: {e}")
        finally:
            db.close()
        await asyncio.sleep(max(5, int(settings.usdt_poll_seconds or 20)))


async def logistics_poll_loop():
    while True:
        db = SessionLocal()
        try:
            result = await asyncio.to_thread(sync_logistics_once, db)
            if result.get("updated") or result.get("signed"):
                print(f"[logistics-sync] updated={result['updated']} signed={result['signed']}")
        except Exception as e:
            print(f"[logistics-sync] loop error: {e}")
        finally:
            db.close()
        await asyncio.sleep(max(300, int(settings.logistics_sync_seconds or 1800)))


async def bot_profile_auto_sync_loop():
    initial_wait = min(15, max(5, int(settings.bot_profile_auto_sync_seconds or 3600)))
    await asyncio.sleep(initial_wait)
    while True:
        db = SessionLocal()
        try:
            result = await asyncio.to_thread(
                sync_bot_profiles_batch_once,
                db,
                str(settings.bot_profile_auto_sync_scope or "enabled"),
                str(settings.bot_profile_auto_sync_bot_type or "all"),
                None,
                "auto_timer",
            )
            if result.get("total"):
                print(
                    f"[bot-profile-auto-sync] total={result.get('total', 0)} success={result.get('success_count', 0)} failed={result.get('failed_count', 0)}"
                )
        except Exception as e:
            print(f"[bot-profile-auto-sync] loop error: {e}")
        finally:
            db.close()
        await asyncio.sleep(max(60, int(settings.bot_profile_auto_sync_seconds or 3600)))


@app.on_event("startup")
async def on_startup():
    global _usdt_loop_task, _logistics_loop_task, _bot_profile_auto_sync_task
    Base.metadata.create_all(bind=engine)
    ensure_schema()
    db = SessionLocal()
    try:
        ensure_bootstrap_admin(db)
    finally:
        db.close()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _usdt_loop_task = asyncio.create_task(usdt_poll_loop())
    _logistics_loop_task = asyncio.create_task(logistics_poll_loop())
    if settings.bot_profile_auto_sync_enabled:
        _bot_profile_auto_sync_task = asyncio.create_task(bot_profile_auto_sync_loop())


@app.on_event("shutdown")
async def on_shutdown():
    global _usdt_loop_task, _logistics_loop_task, _bot_profile_auto_sync_task
    if _usdt_loop_task:
        _usdt_loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _usdt_loop_task
        _usdt_loop_task = None
    if _logistics_loop_task:
        _logistics_loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _logistics_loop_task
        _logistics_loop_task = None
    if _bot_profile_auto_sync_task:
        _bot_profile_auto_sync_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _bot_profile_auto_sync_task
        _bot_profile_auto_sync_task = None


@app.get("/health")
def health():
    return {"ok": True, "service": "goodsmall-backend"}


@app.post("/jobs/usdt/poll")
def run_usdt_job():
    db = SessionLocal()
    try:
        return poll_usdt_once(db)
    finally:
        db.close()


@app.post("/jobs/logistics/sync")
def run_logistics_job():
    db = SessionLocal()
    try:
        return sync_logistics_once(db)
    finally:
        db.close()


app.include_router(public_router)
app.include_router(admin_router)
