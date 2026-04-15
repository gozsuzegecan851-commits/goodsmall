from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from .models import GlobalFolderLinkConfig

DEFAULT_MANUAL_HINT_TEXT = "已导入商城文件夹。机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。"
ALLOWED_BOT_TYPES = {"buyer", "session", "shipping"}


def normalize_bot_types(values: Any) -> list[str]:
    if isinstance(values, str):
        try:
            parsed = json.loads(values)
            if isinstance(parsed, list):
                values = parsed
            else:
                values = [x.strip() for x in values.replace("，", ",").split(",")]
        except Exception:
            values = [x.strip() for x in values.replace("，", ",").split(",")]
    if isinstance(values, (list, tuple, set)):
        rows = [str(x).strip().lower() for x in values]
    else:
        rows = []
    rows = [x for x in rows if x in ALLOWED_BOT_TYPES]
    return rows or ["buyer", "session", "shipping"]


def validate_folder_link_url(url: str) -> tuple[bool, str]:
    value = str(url or "").strip()
    if not value:
        return False, "共享文件夹链接不能为空"
    if value.startswith("https://t.me/addlist/") or value.startswith("http://t.me/addlist/") or value.startswith("tg://addlist"):
        return True, ""
    return False, "共享文件夹链接格式不正确，请填写 t.me/addlist/... 或 tg://addlist?..."


def ensure_global_folder_link_config(db: Session) -> GlobalFolderLinkConfig:
    item = db.query(GlobalFolderLinkConfig).order_by(GlobalFolderLinkConfig.id.asc()).first()
    if item:
        return item
    item = GlobalFolderLinkConfig(
        is_enabled=False,
        primary_button_text="添加到商城文件夹",
        folder_link_url="",
        show_settings_button=True,
        settings_button_text="打开文件夹设置",
        settings_button_url="tg://settings/folders",
        show_manual_hint_button=True,
        manual_hint_button_text="如何手动加入机器人",
        manual_hint_text=DEFAULT_MANUAL_HINT_TEXT,
        apply_to_bot_types=json.dumps(["buyer", "session", "shipping"], ensure_ascii=False),
        apply_to_all_bots=True,
        status="unknown",
        check_mode="weak",
        check_interval_minutes=60,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def folder_link_to_dict(item: GlobalFolderLinkConfig | None) -> dict:
    if not item:
        return {
            "is_enabled": False,
            "primary_button_text": "添加到商城文件夹",
            "folder_link_url": "",
            "show_settings_button": True,
            "settings_button_text": "打开文件夹设置",
            "settings_button_url": "tg://settings/folders",
            "show_manual_hint_button": True,
            "manual_hint_button_text": "如何手动加入机器人",
            "manual_hint_text": DEFAULT_MANUAL_HINT_TEXT,
            "apply_to_bot_types": ["buyer", "session", "shipping"],
            "apply_to_all_bots": True,
            "status": "unknown",
            "last_checked_at": "",
            "last_check_error": "",
            "check_mode": "weak",
            "check_interval_minutes": 60,
        }
    return {
        "id": item.id,
        "is_enabled": bool(item.is_enabled),
        "primary_button_text": str(item.primary_button_text or "添加到商城文件夹"),
        "folder_link_url": str(item.folder_link_url or ""),
        "show_settings_button": bool(item.show_settings_button),
        "settings_button_text": str(item.settings_button_text or "打开文件夹设置"),
        "settings_button_url": str(item.settings_button_url or "tg://settings/folders"),
        "show_manual_hint_button": bool(item.show_manual_hint_button),
        "manual_hint_button_text": str(item.manual_hint_button_text or "如何手动加入机器人"),
        "manual_hint_text": str(item.manual_hint_text or DEFAULT_MANUAL_HINT_TEXT),
        "apply_to_bot_types": normalize_bot_types(item.apply_to_bot_types),
        "apply_to_all_bots": bool(item.apply_to_all_bots),
        "status": str(item.status or "unknown"),
        "last_checked_at": item.last_checked_at.isoformat() if getattr(item, "last_checked_at", None) else "",
        "last_check_error": str(item.last_check_error or ""),
        "check_mode": str(item.check_mode or "weak"),
        "check_interval_minutes": int(item.check_interval_minutes or 60),
    }


def is_folder_link_enabled_for_bot(item: GlobalFolderLinkConfig | None, bot_type: str, bot_code: str = "") -> bool:
    if not item or not item.is_enabled:
        return False
    if item.apply_to_all_bots:
        return True
    return str(bot_type or "").strip().lower() in normalize_bot_types(item.apply_to_bot_types)


def update_folder_link_config_from_payload(item: GlobalFolderLinkConfig, payload: dict) -> GlobalFolderLinkConfig:
    item.is_enabled = bool(payload.get("is_enabled", False))
    item.primary_button_text = str(payload.get("primary_button_text") or "添加到商城文件夹").strip() or "添加到商城文件夹"
    item.folder_link_url = str(payload.get("folder_link_url") or "").strip()
    item.show_settings_button = bool(payload.get("show_settings_button", True))
    item.settings_button_text = str(payload.get("settings_button_text") or "打开文件夹设置").strip() or "打开文件夹设置"
    item.settings_button_url = str(payload.get("settings_button_url") or "tg://settings/folders").strip() or "tg://settings/folders"
    item.show_manual_hint_button = bool(payload.get("show_manual_hint_button", True))
    item.manual_hint_button_text = str(payload.get("manual_hint_button_text") or "如何手动加入机器人").strip() or "如何手动加入机器人"
    item.manual_hint_text = str(payload.get("manual_hint_text") or DEFAULT_MANUAL_HINT_TEXT).strip() or DEFAULT_MANUAL_HINT_TEXT
    item.apply_to_all_bots = bool(payload.get("apply_to_all_bots", True))
    item.apply_to_bot_types = json.dumps(normalize_bot_types(payload.get("apply_to_bot_types")), ensure_ascii=False)
    item.check_mode = str(payload.get("check_mode") or "weak").strip().lower() or "weak"
    if item.check_mode not in {"none", "weak", "telegram_user_api"}:
        item.check_mode = "weak"
    try:
        item.check_interval_minutes = max(5, int(payload.get("check_interval_minutes") or 60))
    except Exception:
        item.check_interval_minutes = 60
    item.updated_at = datetime.utcnow()
    return item

# ===== compatibility aliases for old routes_admin.py =====

def get_or_create_folder_link_config(db: Session) -> GlobalFolderLinkConfig:
    return ensure_global_folder_link_config(db)


def folder_link_config_to_dict(item: GlobalFolderLinkConfig | None) -> dict:
    return folder_link_to_dict(item)


def get_folder_link_runtime_config(db: Session, bot_type: str = "", bot_code: str = "") -> dict:
    item = ensure_global_folder_link_config(db)
    data = folder_link_to_dict(item)
    enabled = is_folder_link_enabled_for_bot(item, bot_type, bot_code)
    data["enabled_for_current_bot"] = enabled
    if not enabled:
        data["is_enabled"] = False
    data["bot_type"] = str(bot_type or "")
    data["bot_code"] = str(bot_code or "")
    return data
