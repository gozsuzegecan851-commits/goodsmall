from __future__ import annotations

import base64
import hashlib
import hmac
import time
from collections.abc import Generator

from fastapi import Header, HTTPException, Request, Response

from .config import settings
from .db import SessionLocal
from .models import AdminUser
from .services.admin_user_service import (
    authenticate_admin_user,
    get_active_admin_user_by_username,
    get_admin_user_by_username,
)

MASK_KEEP = 4
MASK_MIDDLE = "********"
ADMIN_SESSION_COOKIE = "goodsmall_admin_session"
LEGACY_ADMIN_PASSWORD_COOKIE = "goodsmall_admin_password"


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def mask_secret(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) <= MASK_KEEP * 2:
        return raw[:1] + MASK_MIDDLE + raw[-1:]
    return f"{raw[:MASK_KEEP]}{MASK_MIDDLE}{raw[-MASK_KEEP:]}"


def is_masked_secret(raw_value: str, stored_value: str) -> bool:
    raw = str(raw_value or "").strip()
    stored = str(stored_value or "").strip()
    if not raw or not stored:
        return False
    return raw == mask_secret(stored)


def _session_secret() -> str:
    return (
        str(settings.admin_session_secret or "").strip()
        or str(settings.internal_api_token or "").strip()
        or str(settings.admin_password or "").strip()
        or str(settings.project_name or "goodsmall").strip()
    )


def _sign_session_payload(payload: str) -> str:
    secret = _session_secret().encode("utf-8")
    return hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()


def build_admin_session_value(username: str) -> str:
    user = str(username or "").strip() or str(settings.admin_username or "admin").strip() or "admin"
    expire_at = int(time.time()) + int(settings.admin_session_days or 7) * 86400
    payload = f"{user}|{expire_at}"
    signature = _sign_session_payload(payload)
    raw = f"{payload}|{signature}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def parse_admin_session_value(raw_value: str) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8")
        username, expire_at_str, signature = decoded.split("|", 2)
        payload = f"{username}|{expire_at_str}"
        if not hmac.compare_digest(signature, _sign_session_payload(payload)):
            return ""
        if int(expire_at_str) < int(time.time()):
            return ""
        return str(username or "").strip()
    except Exception:
        return ""


def set_admin_session_cookie(response: Response, username: str) -> None:
    response.set_cookie(
        key=ADMIN_SESSION_COOKIE,
        value=build_admin_session_value(username),
        max_age=int(settings.admin_session_days or 7) * 86400,
        httponly=True,
        samesite="lax",
        path="/",
    )
    response.delete_cookie(LEGACY_ADMIN_PASSWORD_COOKIE, path="/")


def clear_admin_session_cookie(response: Response) -> None:
    response.delete_cookie(ADMIN_SESSION_COOKIE, path="/")
    response.delete_cookie(LEGACY_ADMIN_PASSWORD_COOKIE, path="/")


def get_admin_session_username(request: Request) -> str:
    cookie_value = request.cookies.get(ADMIN_SESSION_COOKIE)
    if cookie_value:
        return parse_admin_session_value(cookie_value)
    return ""


def get_admin_password_from_request(request: Request) -> str:
    header_value = request.headers.get("x-admin-password")
    if header_value:
        return str(header_value).strip()
    cookie_value = request.cookies.get(LEGACY_ADMIN_PASSWORD_COOKIE)
    if cookie_value:
        return str(cookie_value).strip()
    return ""


def get_internal_api_token_from_request(request: Request) -> str:
    header_value = request.headers.get("x-internal-api-token")
    if header_value:
        return str(header_value).strip()
    return ""


def admin_password_matches(raw_password: str) -> bool:
    configured = str(settings.admin_password or "").strip()
    if not configured:
        return True
    return str(raw_password or "").strip() == configured


def admin_login_configured() -> bool:
    if str(settings.admin_password or "").strip():
        return True
    db = SessionLocal()
    try:
        return db.query(AdminUser.id).limit(1).first() is not None
    except Exception:
        return False
    finally:
        db.close()


def admin_login_credentials_match(username: str, raw_password: str) -> bool:
    db = SessionLocal()
    try:
        if authenticate_admin_user(db, username, raw_password):
            return True
    finally:
        db.close()
    configured_password = str(settings.admin_password or "").strip()
    configured_username = str(settings.admin_username or "admin").strip() or "admin"
    if not configured_password:
        return not admin_login_configured()
    return str(username or "").strip().lower() == configured_username.lower() and str(raw_password or "").strip() == configured_password


def internal_api_token_matches(raw_token: str) -> bool:
    configured = str(settings.internal_api_token or "").strip()
    if not configured:
        return True
    return str(raw_token or "").strip() == configured


def get_current_admin_profile(request: Request) -> dict:
    session_username = get_admin_session_username(request)
    if session_username:
        db = SessionLocal()
        try:
            user = get_active_admin_user_by_username(db, session_username)
            if user:
                return {
                    "username": user.username,
                    "display_name": user.display_name or user.username,
                    "role": user.role or "operator",
                    "is_superadmin": (user.role or "operator") == "superadmin",
                    "source": "session_db",
                }
        finally:
            db.close()
    legacy_password = get_admin_password_from_request(request)
    if str(settings.admin_password or "").strip() and admin_password_matches(legacy_password):
        username = str(settings.admin_username or "admin").strip() or "admin"
        db = SessionLocal()
        try:
            user = get_admin_user_by_username(db, username)
            role = user.role if user else "superadmin"
            display_name = (user.display_name if user else "") or username
        finally:
            db.close()
        return {
            "username": username,
            "display_name": display_name,
            "role": role or "superadmin",
            "is_superadmin": (role or "superadmin") == "superadmin",
            "source": "legacy_password",
        }
    return {}


def get_current_admin_username(request: Request) -> str:
    profile = get_current_admin_profile(request)
    return str(profile.get("username") or "")


def admin_request_authorized(request: Request) -> bool:
    if not admin_login_configured():
        return True
    if get_current_admin_profile(request):
        return True
    return False


def require_admin_api(request: Request, x_admin_password: str | None = Header(default=None)) -> bool:
    if not admin_login_configured():
        return True
    if admin_request_authorized(request):
        return True
    candidate = str(x_admin_password or "").strip() or get_admin_password_from_request(request)
    if candidate and admin_password_matches(candidate):
        return True
    raise HTTPException(status_code=401, detail="后台未授权，请先登录后台")


def require_internal_api(request: Request, x_internal_api_token: str | None = Header(default=None)) -> bool:
    configured = str(settings.internal_api_token or "").strip()
    if not configured:
        return True
    candidate = str(x_internal_api_token or "").strip() or get_internal_api_token_from_request(request)
    if candidate != configured:
        raise HTTPException(status_code=401, detail="内部接口未授权")
    return True
