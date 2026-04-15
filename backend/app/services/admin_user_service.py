from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from ..config import settings
from ..models import AdminUser

USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,32}$")
PASSWORD_MIN_LEN = 6
PBKDF2_ROUNDS = 390000


def normalize_username(value: str) -> str:
    return str(value or "").strip().lower()


def validate_username(value: str) -> str:
    username = normalize_username(value)
    if not USERNAME_RE.match(username):
        raise ValueError("账号只能使用 3-32 位字母、数字、下划线、点或中横线")
    return username


def validate_password(value: str) -> str:
    raw = str(value or "")
    if len(raw) < PASSWORD_MIN_LEN:
        raise ValueError(f"密码至少需要 {PASSWORD_MIN_LEN} 位")
    return raw


def _pbkdf2(password: str, salt: bytes, rounds: int = PBKDF2_ROUNDS) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)


def build_password_hash(password: str) -> str:
    raw = validate_password(password)
    salt = os.urandom(16)
    digest = _pbkdf2(raw, salt)
    return "pbkdf2_sha256${}${}${}".format(
        PBKDF2_ROUNDS,
        base64.urlsafe_b64encode(salt).decode("utf-8"),
        base64.urlsafe_b64encode(digest).decode("utf-8"),
    )


def verify_password(password: str, password_hash: str) -> bool:
    raw = str(password or "")
    stored = str(password_hash or "")
    if not stored:
        return False
    if stored.startswith("pbkdf2_sha256$"):
        try:
            _, rounds_s, salt_b64, digest_b64 = stored.split("$", 3)
            salt = base64.urlsafe_b64decode(salt_b64.encode("utf-8"))
            expected = base64.urlsafe_b64decode(digest_b64.encode("utf-8"))
            actual = _pbkdf2(raw, salt, int(rounds_s))
            return hmac.compare_digest(actual, expected)
        except Exception:
            return False
    return raw == stored


def admin_user_to_dict(user: AdminUser) -> dict[str, Any]:
    return {
        "id": user.id,
        "username": user.username,
        "display_name": user.display_name or user.username,
        "role": user.role or "operator",
        "is_active": bool(user.is_active),
        "last_login_at": user.last_login_at.isoformat(sep=" ") if user.last_login_at else None,
        "created_at": user.created_at.isoformat(sep=" ") if user.created_at else None,
        "updated_at": user.updated_at.isoformat(sep=" ") if user.updated_at else None,
    }


def get_admin_user_by_username(db: Session, username: str) -> AdminUser | None:
    normalized = normalize_username(username)
    if not normalized:
        return None
    return db.query(AdminUser).filter(AdminUser.username == normalized).first()


def get_active_admin_user_by_username(db: Session, username: str) -> AdminUser | None:
    normalized = normalize_username(username)
    if not normalized:
        return None
    return db.query(AdminUser).filter(AdminUser.username == normalized, AdminUser.is_active.is_(True)).first()


def admin_user_count(db: Session) -> int:
    return int(db.query(AdminUser.id).count() or 0)


def list_admin_users(db: Session) -> list[dict[str, Any]]:
    rows = db.query(AdminUser).order_by(AdminUser.role.desc(), AdminUser.created_at.asc(), AdminUser.id.asc()).all()
    return [admin_user_to_dict(x) for x in rows]


def ensure_bootstrap_admin(db: Session) -> AdminUser | None:
    username = normalize_username(settings.admin_username or "admin") or "admin"
    raw_password = str(settings.admin_password or "").strip()
    if not raw_password:
        return None
    user = get_admin_user_by_username(db, username)
    if user:
        changed = False
        if not user.password_hash:
            user.password_hash = build_password_hash(raw_password)
            changed = True
        if not user.display_name:
            user.display_name = "超级管理员"
            changed = True
        if not user.role:
            user.role = "superadmin"
            changed = True
        if not user.is_active:
            user.is_active = True
            changed = True
        if changed:
            user.updated_at = datetime.utcnow()
            db.add(user)
            db.commit()
            db.refresh(user)
        return user
    user = AdminUser(
        username=username,
        display_name="超级管理员",
        password_hash=build_password_hash(raw_password),
        role="superadmin",
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate_admin_user(db: Session, username: str, raw_password: str) -> AdminUser | None:
    user = get_active_admin_user_by_username(db, username)
    if user and verify_password(raw_password, user.password_hash):
        return user
    return None


def touch_admin_login(db: Session, user: AdminUser) -> None:
    user.last_login_at = datetime.utcnow()
    user.updated_at = datetime.utcnow()
    db.add(user)
    db.commit()
    db.refresh(user)


def require_superadmin_user(db: Session, username: str) -> AdminUser:
    user = get_active_admin_user_by_username(db, username)
    if not user:
        raise ValueError("当前管理员不存在或已停用")
    if (user.role or "operator") != "superadmin":
        raise ValueError("只有超级管理员可以管理后台账号")
    return user


def create_admin_user(db: Session, *, username: str, password: str, display_name: str = "", role: str = "operator", is_active: bool = True, created_by: str = "") -> AdminUser:
    username = validate_username(username)
    validate_password(password)
    role = str(role or "operator").strip().lower()
    if role not in {"superadmin", "operator"}:
        raise ValueError("角色只支持 superadmin 或 operator")
    if get_admin_user_by_username(db, username):
        raise ValueError("该管理员账号已存在")
    user = AdminUser(
        username=username,
        display_name=str(display_name or "").strip() or username,
        password_hash=build_password_hash(password),
        role=role,
        is_active=bool(is_active),
        created_by=normalize_username(created_by),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def update_admin_user(db: Session, *, user_id: int, display_name: str = "", role: str = "operator", is_active: bool = True, operator_username: str = "") -> AdminUser:
    user = db.query(AdminUser).filter(AdminUser.id == int(user_id)).first()
    if not user:
        raise ValueError("管理员账号不存在")
    role = str(role or user.role or "operator").strip().lower()
    if role not in {"superadmin", "operator"}:
        raise ValueError("角色只支持 superadmin 或 operator")
    target_active = bool(is_active)
    if not target_active and normalize_username(operator_username) == user.username:
        raise ValueError("不能停用当前登录账号")
    if (user.role or "operator") == "superadmin" and role != "superadmin":
        super_count = db.query(AdminUser.id).filter(AdminUser.role == "superadmin", AdminUser.is_active.is_(True)).count()
        if super_count <= 1:
            raise ValueError("至少保留一个启用中的超级管理员")
    if (user.role or "operator") == "superadmin" and not target_active:
        super_count = db.query(AdminUser.id).filter(AdminUser.role == "superadmin", AdminUser.is_active.is_(True)).count()
        if super_count <= 1:
            raise ValueError("至少保留一个启用中的超级管理员")
    user.display_name = str(display_name or "").strip() or user.username
    user.role = role
    user.is_active = target_active
    user.updated_at = datetime.utcnow()
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def set_admin_user_password(db: Session, *, user_id: int, new_password: str) -> AdminUser:
    user = db.query(AdminUser).filter(AdminUser.id == int(user_id)).first()
    if not user:
        raise ValueError("管理员账号不存在")
    user.password_hash = build_password_hash(new_password)
    user.updated_at = datetime.utcnow()
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def delete_admin_user(db: Session, *, user_id: int, operator_username: str = "") -> None:
    user = db.query(AdminUser).filter(AdminUser.id == int(user_id)).first()
    if not user:
        raise ValueError("管理员账号不存在")
    if normalize_username(operator_username) == user.username:
        raise ValueError("不能删除当前登录账号")
    if (user.role or "operator") == "superadmin":
        super_count = db.query(AdminUser.id).filter(AdminUser.role == "superadmin", AdminUser.is_active.is_(True)).count()
        if super_count <= 1 and user.is_active:
            raise ValueError("至少保留一个启用中的超级管理员")
    db.delete(user)
    db.commit()
