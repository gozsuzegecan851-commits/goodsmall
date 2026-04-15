from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx

ADDLIST_RE = re.compile(r"(?:https?://t\.me/addlist/)([A-Za-z0-9_\-]+)")


def _normalize_check_url(url: str) -> tuple[str, str]:
    value = str(url or "").strip()
    if value.startswith("tg://addlist"):
        parsed = urlparse(value)
        slug = parse_qs(parsed.query).get("slug", [""])[0].strip()
        return (f"https://t.me/addlist/{slug}" if slug else value, slug)
    match = ADDLIST_RE.search(value)
    if match:
        return value, match.group(1)
    return value, ""


def check_folder_link_status(url: str, mode: str = "weak") -> dict:
    value, slug = _normalize_check_url(url)
    checked_at = datetime.utcnow().isoformat()
    if not value:
        return {"ok": False, "status": "invalid", "error": "共享文件夹链接为空", "checked_at": checked_at}
    if not slug:
        return {"ok": False, "status": "invalid", "error": "链接格式不正确，未识别到 addlist slug", "checked_at": checked_at}

    mode = str(mode or "weak").strip().lower()
    if mode == "none":
        return {"ok": True, "status": "unknown", "error": "", "checked_at": checked_at}

    if mode == "telegram_user_api":
        checker = os.getenv("FOLDER_LINK_CHECKER_URL", "").strip()
        if checker:
            try:
                with httpx.Client(timeout=15.0) as client:
                    res = client.post(checker, json={"url": value, "slug": slug})
                data = res.json() if res.headers.get("content-type", "").startswith("application/json") else {}
                if res.status_code >= 400:
                    return {"ok": False, "status": "error", "error": data.get("error") or f"检测服务返回 {res.status_code}", "checked_at": checked_at}
                return {"ok": bool(data.get("ok", True)), "status": str(data.get("status") or ("ready" if data.get("ok", True) else "error")), "error": str(data.get("error") or ""), "checked_at": checked_at}
            except Exception as exc:
                return {"ok": False, "status": "error", "error": f"调用 Telegram 用户检测服务失败：{exc}", "checked_at": checked_at}
        return {"ok": False, "status": "error", "error": "未配置 Telegram 用户 API 检测服务，当前回退为弱检测前请改为 weak 模式", "checked_at": checked_at}

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            res = client.get(value)
        if res.status_code >= 400:
            return {"ok": False, "status": "expired", "error": f"链接访问失败：HTTP {res.status_code}", "checked_at": checked_at}
        body = res.text[:5000]
        low = body.lower()
        if "invite link expired" in low or "link expired" in low or "invalid" in low and 'addlist' in low:
            return {"ok": False, "status": "expired", "error": "共享文件夹链接可能已失效或过期", "checked_at": checked_at}
        return {"ok": True, "status": "ready", "error": "", "checked_at": checked_at}
    except Exception as exc:
        return {"ok": False, "status": "error", "error": f"弱检测失败：{exc}", "checked_at": checked_at}


def should_recheck(last_checked_at, interval_minutes: int) -> bool:
    if not last_checked_at:
        return True
    try:
        return datetime.utcnow() - last_checked_at >= timedelta(minutes=max(5, int(interval_minutes or 60)))
    except Exception:
        return True

# ===== compatibility alias for old routes_admin.py =====

def check_folder_link_config(db, cfg) -> dict:
    result = check_folder_link_status(
        str(getattr(cfg, "folder_link_url", "") or ""),
        str(getattr(cfg, "check_mode", "weak") or "weak"),
    )
    try:
        cfg.status = str(result.get("status") or "unknown")
        cfg.last_check_error = str(result.get("error") or "")
        checked_at = result.get("checked_at")
        cfg.last_checked_at = datetime.fromisoformat(checked_at) if checked_at else datetime.utcnow()
    except Exception:
        cfg.last_checked_at = datetime.utcnow()
    return result
