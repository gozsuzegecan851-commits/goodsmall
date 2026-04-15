
import json
import re
import threading
import time
import unicodedata
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import func, inspect, or_, text
from sqlalchemy.orm import Session

from ..db import engine
from ..models import BotConfig, ChatKeywordBlock, CustomerMessage, CustomerSession, Order


SYSTEM_BLOCK_KEYWORDS = {
    "商品分类", "🛍 商品分类",
    "我的地址", "📍 我的地址",
    "我的订单", "📦 我的订单",
    "支付帮助", "💳 支付帮助",
    "返回分类", "🔙 返回分类",
    "返回", "back", "BACK",
}

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False
_KEYWORD_CACHE_LOCK = threading.Lock()
_ACTIVE_KEYWORD_CACHE: list[tuple[str, str]] | None = None
_ACTIVE_KEYWORD_CACHE_AT = 0.0
_ACTIVE_KEYWORD_CACHE_TTL = 5.0
_CHAT_KEYWORD_COLUMNS: set[str] | None = None


def _get_chat_keyword_columns(force: bool = False) -> set[str]:
    global _CHAT_KEYWORD_COLUMNS
    if _CHAT_KEYWORD_COLUMNS is not None and not force:
        return set(_CHAT_KEYWORD_COLUMNS)
    try:
        inspector = inspect(engine)
        cols = {str(col.get("name") or "") for col in inspector.get_columns("chat_keyword_blocks")}
    except Exception:
        cols = set()
    _CHAT_KEYWORD_COLUMNS = set(cols)
    return set(cols)


def _ensure_chat_keyword_blocks_table(force: bool = False) -> None:
    global _SCHEMA_READY, _CHAT_KEYWORD_COLUMNS
    if _SCHEMA_READY and not force:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY and not force:
            return
        try:
            ChatKeywordBlock.__table__.create(bind=engine, checkfirst=True)
        except Exception:
            pass

        ddl_map = {
            "keyword": "VARCHAR(255) NOT NULL DEFAULT ''",
            "match_type": "VARCHAR(32) NULL",
            "match_mode": "VARCHAR(32) NULL",
            "is_active": "BOOLEAN NOT NULL DEFAULT TRUE",
            "remark": "TEXT NOT NULL DEFAULT ''",
            "created_at": "TIMESTAMP NULL",
            "updated_at": "TIMESTAMP NULL",
        }
        try:
            inspector = inspect(engine)
            if not inspector.has_table("chat_keyword_blocks"):
                _CHAT_KEYWORD_COLUMNS = set()
                _SCHEMA_READY = True
                return
            existing = {str(col.get("name") or "") for col in inspector.get_columns("chat_keyword_blocks")}
            with engine.begin() as conn:
                for column_name, ddl in ddl_map.items():
                    if column_name not in existing:
                        conn.execute(text(f"ALTER TABLE chat_keyword_blocks ADD COLUMN {column_name} {ddl}"))
                cols_now = _get_chat_keyword_columns(force=True) or (set(existing) | set(ddl_map.keys()))
                conn.execute(text("UPDATE chat_keyword_blocks SET keyword = COALESCE(keyword, '')"))
                if "match_type" in cols_now and "match_mode" in cols_now:
                    conn.execute(text("UPDATE chat_keyword_blocks SET match_type = COALESCE(NULLIF(match_type, ''), NULLIF(match_mode, ''), 'exact')"))
                    conn.execute(text("UPDATE chat_keyword_blocks SET match_mode = COALESCE(NULLIF(match_mode, ''), NULLIF(match_type, ''), 'exact')"))
                elif "match_type" in cols_now:
                    conn.execute(text("UPDATE chat_keyword_blocks SET match_type = COALESCE(NULLIF(match_type, ''), 'exact')"))
                elif "match_mode" in cols_now:
                    conn.execute(text("UPDATE chat_keyword_blocks SET match_mode = COALESCE(NULLIF(match_mode, ''), 'exact')"))
                conn.execute(text("UPDATE chat_keyword_blocks SET remark = COALESCE(remark, '')"))
                conn.execute(text("UPDATE chat_keyword_blocks SET is_active = COALESCE(is_active, TRUE)"))
                conn.execute(text("UPDATE chat_keyword_blocks SET created_at = COALESCE(created_at, NOW())"))
                conn.execute(text("UPDATE chat_keyword_blocks SET updated_at = COALESCE(updated_at, NOW())"))
                for sql in [
                    "ALTER TABLE chat_keyword_blocks ALTER COLUMN keyword SET DEFAULT ''",
                    "ALTER TABLE chat_keyword_blocks ALTER COLUMN remark SET DEFAULT ''",
                    "ALTER TABLE chat_keyword_blocks ALTER COLUMN is_active SET DEFAULT TRUE",
                    "ALTER TABLE chat_keyword_blocks ALTER COLUMN match_type SET DEFAULT 'exact'",
                    "ALTER TABLE chat_keyword_blocks ALTER COLUMN match_mode SET DEFAULT 'exact'",
                ]:
                    try:
                        conn.execute(text(sql))
                    except Exception:
                        pass
            _CHAT_KEYWORD_COLUMNS = _get_chat_keyword_columns(force=True)
        except Exception:
            # Best effort only. Real query errors should surface to callers.
            pass
        _SCHEMA_READY = True

def _invalidate_keyword_cache() -> None:
    global _ACTIVE_KEYWORD_CACHE, _ACTIVE_KEYWORD_CACHE_AT
    with _KEYWORD_CACHE_LOCK:
        _ACTIVE_KEYWORD_CACHE = None
        _ACTIVE_KEYWORD_CACHE_AT = 0.0


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _to_json(payload: dict[str, Any] | None) -> str:
    try:
        return json.dumps(payload or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def _build_display_name(first_name: Any = '', last_name: Any = '', username: Any = '', user_id: Any = '') -> str:
    first = _safe_str(first_name)
    last = _safe_str(last_name)
    username = _safe_str(username)
    user_id = _safe_str(user_id)
    full = ' '.join(x for x in [first, last] if x)
    if full:
        return full
    if first:
        return first
    if last:
        return last
    if username:
        return username
    return user_id



def _normalize_page_value(value: Any, default: int = 1) -> int:
    try:
        page = int(value or default)
    except Exception:
        page = default
    return page if page > 0 else default


def _normalize_page_size_value(value: Any, default: int = 30, minimum: int = 10, maximum: int = 100) -> int:
    try:
        size = int(value or default)
    except Exception:
        size = default
    size = max(minimum, size)
    return min(size, maximum)


def _paged_payload(rows: list[dict[str, Any]], total: int, page: int, page_size: int) -> dict[str, Any]:
    total = max(int(total or 0), 0)
    page = _normalize_page_value(page, 1)
    page_size = _normalize_page_size_value(page_size, default=page_size or 30)
    total_pages = max(1, (total + page_size - 1) // page_size) if total else 1
    if page > total_pages:
        page = total_pages
    return {
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def _session_to_dict(item: CustomerSession) -> dict[str, Any]:
    display_name = _build_display_name(item.telegram_first_name, item.telegram_last_name, item.telegram_username, item.telegram_user_id)
    return {
        "id": item.id,
        "bot_code": item.bot_code or "",
        "telegram_user_id": item.telegram_user_id or "",
        "telegram_chat_id": item.telegram_chat_id or "",
        "telegram_username": item.telegram_username or "",
        "telegram_first_name": item.telegram_first_name or "",
        "telegram_last_name": item.telegram_last_name or "",
        "display_name": display_name,
        "session_status": item.session_status or "open",
        "unread_count": int(item.unread_count or 0),
        "last_message_text": item.last_message_text or "",
        "last_message_type": item.last_message_type or "text",
        "first_customer_message_at": item.first_customer_message_at.isoformat() if item.first_customer_message_at else "",
        "last_customer_message_at": item.last_customer_message_at.isoformat() if item.last_customer_message_at else "",
        "last_operator_reply_at": item.last_operator_reply_at.isoformat() if item.last_operator_reply_at else "",
        "created_at": item.created_at.isoformat() if item.created_at else "",
        "updated_at": item.updated_at.isoformat() if item.updated_at else "",
    }


def _message_to_dict(item: CustomerMessage) -> dict[str, Any]:
    payload = {}
    try:
        payload = json.loads(item.content_json or "{}")
    except Exception:
        payload = {}
    return {
        "id": item.id,
        "session_id": item.session_id,
        "bot_code": item.bot_code or "",
        "telegram_user_id": item.telegram_user_id or "",
        "direction": item.direction or "customer",
        "sender_name": item.sender_name or "",
        "message_type": item.message_type or "text",
        "content_text": item.content_text or "",
        "content_json": payload,
        "telegram_message_id": item.telegram_message_id or "",
        "created_at": item.created_at.isoformat() if item.created_at else "",
    }


def _normalize_match_text(value: Any) -> str:
    raw = _safe_str(value)
    if not raw:
        return ""
    raw = unicodedata.normalize("NFKC", raw)
    raw = raw.replace("️", "").replace("​", "").replace("‌", "").replace("‍", "")
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip().lower()


def _plain_match_text(value: Any) -> str:
    raw = _normalize_match_text(value)
    if not raw:
        return ""
    keep: list[str] = []
    for ch in raw:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            keep.append(ch)
    return "".join(keep)


def _message_type_is_text(message_type: str) -> bool:
    mt = _normalize_match_text(message_type)
    return mt in {"text", "contenttype.text"}


def _keyword_matches(message_text: str, keyword: str, match_type: str = "exact") -> bool:
    value = _safe_str(message_text)
    key = _safe_str(keyword)
    if not value or not key:
        return False
    value_norm = _normalize_match_text(value)
    key_norm = _normalize_match_text(key)
    value_plain = _plain_match_text(value)
    key_plain = _plain_match_text(key)
    if match_type == "contains":
        return (
            (key in value)
            or (key_norm and key_norm in value_norm)
            or (key_plain and key_plain in value_plain)
        )
    return (
        value == key
        or value_norm == key_norm
        or (key_plain and value_plain == key_plain)
    )


def _active_keyword_pairs(db: Session | None) -> list[tuple[str, str]]:
    _ensure_chat_keyword_blocks_table()
    if db is None:
        return []
    global _ACTIVE_KEYWORD_CACHE, _ACTIVE_KEYWORD_CACHE_AT
    now = time.monotonic()
    with _KEYWORD_CACHE_LOCK:
        if _ACTIVE_KEYWORD_CACHE is not None and (now - _ACTIVE_KEYWORD_CACHE_AT) < _ACTIVE_KEYWORD_CACHE_TTL:
            return list(_ACTIVE_KEYWORD_CACHE)
    rows = db.query(ChatKeywordBlock).filter(ChatKeywordBlock.is_active == True).all()
    pairs = [(_safe_str(row.keyword), _safe_str(getattr(row, "match_type", "") or getattr(row, "match_mode", "")) or "exact") for row in rows if _safe_str(row.keyword)]
    with _KEYWORD_CACHE_LOCK:
        _ACTIVE_KEYWORD_CACHE = pairs
        _ACTIVE_KEYWORD_CACHE_AT = now
    return list(pairs)


def _allocate_chat_keyword_block_id(db: Session) -> int:
    max_id = db.query(func.max(ChatKeywordBlock.id)).scalar() or 0
    return int(max_id) + 1


def _find_latest_visible_message(db: Session, session_id: int) -> CustomerMessage | None:
    rows = (
        db.query(CustomerMessage)
        .filter(CustomerMessage.session_id == session_id)
        .order_by(CustomerMessage.id.desc())
        .limit(200)
        .all()
    )
    for row in rows:
        if row.direction == "operator":
            return row
        if not should_block_customer_message(db, row.content_text or "", row.message_type or "text"):
            return row
    return None


def _hydrate_session_preview(db: Session, item: CustomerSession) -> dict[str, Any] | None:
    data = _session_to_dict(item)
    if should_block_customer_message(db, item.last_message_text or "", item.last_message_type or "text"):
        latest = _find_latest_visible_message(db, item.id)
        if latest is None:
            return None
        data["last_message_text"] = latest.content_text or ""
        data["last_message_type"] = latest.message_type or "text"
    return data


def list_keyword_blocks(db: Session) -> list[dict[str, Any]]:
    _ensure_chat_keyword_blocks_table()
    rows = db.query(ChatKeywordBlock).order_by(ChatKeywordBlock.updated_at.desc(), ChatKeywordBlock.id.desc()).all()
    return [{
        "id": row.id,
        "keyword": row.keyword or "",
        "match_type": row.match_type or row.match_mode or "exact",
        "is_active": bool(row.is_active),
        "remark": row.remark or "",
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
    } for row in rows]


def get_effective_keyword_blocks(db: Session) -> dict[str, Any]:
    custom = [r for r in list_keyword_blocks(db) if r.get("is_active")]
    system = sorted(SYSTEM_BLOCK_KEYWORDS)
    return {"system_keywords": system, "custom_keywords": custom}


def _normalize_match_type_input(value: Any) -> str:
    raw = _safe_str(value)
    mapping = {
        "": "exact",
        "exact": "exact",
        "contains": "contains",
        "完全匹配": "exact",
        "包含匹配": "contains",
    }
    mt = mapping.get(raw.lower() if isinstance(raw, str) else raw)
    if mt in {"exact", "contains"}:
        return mt
    if raw in mapping:
        return mapping[raw]
    raise ValueError("match_type 仅支持 exact / contains")



def save_keyword_block(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_chat_keyword_blocks_table()
    block_id = int(payload.get("id") or 0)
    keyword = _safe_str(payload.get("keyword"))
    if not keyword:
        raise ValueError("屏蔽关键词不能为空")
    match_type = _normalize_match_type_input(payload.get("match_type"))
    is_active = bool(payload.get("is_active", True))
    remark = _safe_str(payload.get("remark"))
    now = datetime.utcnow()
    cols = _get_chat_keyword_columns()
    if not cols:
        cols = {"id", "keyword", "match_type", "is_active", "remark", "created_at", "updated_at"}

    params = {
        "id": block_id,
        "keyword": keyword,
        "match_type": match_type,
        "match_mode": match_type,
        "is_active": is_active,
        "remark": remark,
        "created_at": now,
        "updated_at": now,
    }

    try:
        if block_id > 0:
            exists = db.query(ChatKeywordBlock.id).filter(ChatKeywordBlock.id == block_id).first()
            if not exists:
                raise ValueError("屏蔽词不存在")
            set_parts = [
                "keyword = :keyword",
                "is_active = :is_active",
                "remark = :remark",
                "updated_at = :updated_at",
            ]
            if "match_type" in cols:
                set_parts.append("match_type = :match_type")
            if "match_mode" in cols:
                set_parts.append("match_mode = :match_mode")
            db.execute(text(f"UPDATE chat_keyword_blocks SET {', '.join(set_parts)} WHERE id = :id"), params)
        else:
            block_id = _allocate_chat_keyword_block_id(db)
            params["id"] = block_id
            insert_cols = ["id", "keyword", "is_active", "remark", "created_at", "updated_at"]
            insert_vals = [":id", ":keyword", ":is_active", ":remark", ":created_at", ":updated_at"]
            if "match_type" in cols:
                insert_cols.append("match_type")
                insert_vals.append(":match_type")
            if "match_mode" in cols:
                insert_cols.append("match_mode")
                insert_vals.append(":match_mode")
            sql = f"INSERT INTO chat_keyword_blocks ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)})"
            db.execute(text(sql), params)

        if "match_type" in cols and "match_mode" in cols:
            db.execute(
                text(
                    "UPDATE chat_keyword_blocks "
                    "SET match_type = COALESCE(NULLIF(match_type, ''), NULLIF(match_mode, ''), 'exact'), "
                    "    match_mode = COALESCE(NULLIF(match_mode, ''), NULLIF(match_type, ''), 'exact'), "
                    "    updated_at = :updated_at "
                    "WHERE id = :id"
                ),
                {"id": block_id, "updated_at": datetime.utcnow()},
            )
        elif "match_type" in cols:
            db.execute(
                text(
                    "UPDATE chat_keyword_blocks "
                    "SET match_type = COALESCE(NULLIF(match_type, ''), 'exact'), updated_at = :updated_at "
                    "WHERE id = :id"
                ),
                {"id": block_id, "updated_at": datetime.utcnow()},
            )
        elif "match_mode" in cols:
            db.execute(
                text(
                    "UPDATE chat_keyword_blocks "
                    "SET match_mode = COALESCE(NULLIF(match_mode, ''), 'exact'), updated_at = :updated_at "
                    "WHERE id = :id"
                ),
                {"id": block_id, "updated_at": datetime.utcnow()},
            )
        db.commit()
    except ValueError:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise RuntimeError(str(e))

    _ensure_chat_keyword_blocks_table(force=True)
    row = db.query(ChatKeywordBlock).filter(ChatKeywordBlock.id == block_id).first()
    _invalidate_keyword_cache()
    return {"ok": True, "row": {
        "id": row.id,
        "keyword": row.keyword or "",
        "match_type": row.match_type or match_type or "exact",
        "is_active": bool(row.is_active),
        "remark": row.remark or "",
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
    } if row else None}

def toggle_keyword_block(db: Session, block_id: int) -> dict[str, Any]:
    _ensure_chat_keyword_blocks_table()
    row = db.query(ChatKeywordBlock).filter(ChatKeywordBlock.id == block_id).first()
    if not row:
        raise ValueError("屏蔽词不存在")
    row.is_active = not bool(row.is_active)
    db.commit()
    db.refresh(row)
    _invalidate_keyword_cache()
    return {"ok": True, "is_active": bool(row.is_active)}


def delete_keyword_block(db: Session, block_id: int) -> dict[str, Any]:
    _ensure_chat_keyword_blocks_table()
    row = db.query(ChatKeywordBlock).filter(ChatKeywordBlock.id == block_id).first()
    if not row:
        raise ValueError("屏蔽词不存在")
    db.delete(row)
    db.commit()
    _invalidate_keyword_cache()
    return {"ok": True}


def should_block_customer_message(db: Session | None, text: str, message_type: str = "text") -> bool:
    if not _message_type_is_text(message_type):
        return False
    value = _safe_str(text)
    if not value:
        return False
    if value.startswith('/'):
        return True
    for keyword in SYSTEM_BLOCK_KEYWORDS:
        if _keyword_matches(value, keyword, "exact"):
            return True
    for keyword, match_type in _active_keyword_pairs(db):
        if _keyword_matches(value, keyword, match_type or "exact"):
            return True
    return False


def get_or_create_session(
    db: Session,
    bot_code: str,
    telegram_user_id: str,
    telegram_chat_id: str = "",
    username: str = "",
    first_name: str = "",
    last_name: str = "",
) -> CustomerSession:
    item = db.query(CustomerSession).filter(
        CustomerSession.bot_code == bot_code,
        CustomerSession.telegram_user_id == telegram_user_id,
    ).first()
    if not item:
        item = CustomerSession(
            bot_code=bot_code,
            telegram_user_id=telegram_user_id,
            telegram_chat_id=telegram_chat_id,
            telegram_username=username,
            telegram_first_name=first_name,
            telegram_last_name=last_name,
            session_status="open",
            unread_count=0,
        )
        db.add(item)
        db.flush()
    if telegram_chat_id:
        item.telegram_chat_id = telegram_chat_id
    if username:
        item.telegram_username = username
    if first_name:
        item.telegram_first_name = first_name
    if last_name:
        item.telegram_last_name = last_name
    return item


def record_customer_event(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    bot_code = _safe_str(payload.get("bot_code"))
    telegram_user_id = _safe_str(payload.get("telegram_user_id"))
    if not bot_code or not telegram_user_id:
        raise ValueError("bot_code 和 telegram_user_id 不能为空")
    text = _safe_str(payload.get("content_text"))
    message_type = _safe_str(payload.get("message_type")) or "text"
    if (_safe_str(payload.get("direction")) or "customer") != "operator" and should_block_customer_message(db, text, message_type):
        return {"ok": True, "blocked": True, "reason": "keyword_block", "content_text": text}
    session = get_or_create_session(
        db,
        bot_code=bot_code,
        telegram_user_id=telegram_user_id,
        telegram_chat_id=_safe_str(payload.get("telegram_chat_id")),
        username=_safe_str(payload.get("telegram_username")),
        first_name=_safe_str(payload.get("telegram_first_name")),
        last_name=_safe_str(payload.get("telegram_last_name")),
    )
    now = datetime.utcnow()
    if payload.get("direction") == "operator":
        session.last_operator_reply_at = now
        session.unread_count = 0
    else:
        session.first_customer_message_at = session.first_customer_message_at or now
        session.last_customer_message_at = now
        session.unread_count = int(session.unread_count or 0) + 1
        session.session_status = "open"
    session.last_message_text = text[:1000]
    session.last_message_type = message_type
    session.updated_at = now
    msg = CustomerMessage(
        session_id=session.id,
        bot_code=bot_code,
        telegram_user_id=telegram_user_id,
        direction=_safe_str(payload.get("direction")) or "customer",
        sender_name=_safe_str(payload.get("sender_name")) or _build_display_name(session.telegram_first_name, session.telegram_last_name, session.telegram_username, telegram_user_id),
        message_type=message_type,
        content_text=text,
        content_json=_to_json(payload.get("content_json") if isinstance(payload.get("content_json"), dict) else {"raw": payload.get("content_json")}),
        telegram_message_id=_safe_str(payload.get("telegram_message_id")),
    )
    db.add(msg)
    db.commit()
    db.refresh(session)
    return {"ok": True, "session": _session_to_dict(session), "message": _message_to_dict(msg)}


def get_chat_overview(db: Session) -> dict[str, Any]:
    total = int(db.query(func.count(CustomerSession.id)).scalar() or 0)
    open_count = int(db.query(func.count(CustomerSession.id)).filter(CustomerSession.session_status == "open").scalar() or 0)
    closed_count = int(db.query(func.count(CustomerSession.id)).filter(CustomerSession.session_status == "closed").scalar() or 0)
    unread_count = int(db.query(func.coalesce(func.sum(CustomerSession.unread_count), 0)).scalar() or 0)
    return {
        "session_count": total,
        "open_count": open_count,
        "closed_count": closed_count,
        "unread_count": unread_count,
    }


def _session_search_blobs(db: Session, item: CustomerSession) -> list[str]:
    blobs = [
        _safe_str(item.bot_code),
        _safe_str(item.telegram_user_id),
        _safe_str(item.telegram_username),
        _safe_str(item.telegram_first_name),
        _safe_str(item.telegram_last_name),
        _build_display_name(item.telegram_first_name, item.telegram_last_name, item.telegram_username, item.telegram_user_id),
        _safe_str(item.last_message_text),
    ]
    orders = (
        db.query(Order)
        .filter(Order.bot_code == item.bot_code, Order.telegram_user_id == item.telegram_user_id)
        .order_by(Order.id.desc())
        .limit(10)
        .all()
    )
    for order in orders:
        blobs.extend([
            _safe_str(order.order_no),
            _safe_str(order.customer_name),
            _safe_str(order.customer_phone),
            _safe_str(order.tracking_no),
        ])
    return [x for x in blobs if x]


def _session_matches_query(db: Session, item: CustomerSession, q: str) -> bool:
    query = _normalize_match_text(q)
    if not query:
        return True
    blobs = _session_search_blobs(db, item)
    plain_query = _plain_match_text(query)
    for raw in blobs:
        value = _normalize_match_text(raw)
        if query in value:
            return True
        plain_value = _plain_match_text(raw)
        if plain_query and plain_query in plain_value:
            return True
    return False


def list_sessions(
    db: Session,
    bot_code: str = "",
    status: str = "open",
    q: str = "",
    only_unread: bool = False,
    limit: int = 30,
    page: int = 1,
    page_size: int | None = None,
) -> dict[str, Any]:
    page = _normalize_page_value(page, 1)
    requested_size = page_size if page_size is not None else limit
    page_size = _normalize_page_size_value(requested_size, default=30, minimum=10, maximum=100)

    rows = db.query(CustomerSession)
    if bot_code:
        rows = rows.filter(CustomerSession.bot_code == bot_code)
    if status and status != "all":
        rows = rows.filter(CustomerSession.session_status == status)
    if only_unread:
        rows = rows.filter(CustomerSession.unread_count > 0)

    query_text = _safe_str(q)
    if query_text:
        pattern = f"%{query_text}%"
        order_exists = (
            db.query(Order.id)
            .filter(
                Order.bot_code == CustomerSession.bot_code,
                Order.telegram_user_id == CustomerSession.telegram_user_id,
                or_(
                    Order.order_no.ilike(pattern),
                    Order.customer_name.ilike(pattern),
                    Order.customer_phone.ilike(pattern),
                    Order.tracking_no.ilike(pattern),
                ),
            )
            .exists()
        )
        rows = rows.filter(
            or_(
                CustomerSession.bot_code.ilike(pattern),
                CustomerSession.telegram_user_id.ilike(pattern),
                CustomerSession.telegram_username.ilike(pattern),
                CustomerSession.telegram_first_name.ilike(pattern),
                CustomerSession.telegram_last_name.ilike(pattern),
                CustomerSession.last_message_text.ilike(pattern),
                order_exists,
            )
        )

    total = int(rows.order_by(None).count() or 0)
    ordered = rows.order_by(CustomerSession.updated_at.desc(), CustomerSession.id.desc())

    result: list[dict[str, Any]] = []
    fetch_offset = max(0, (page - 1) * page_size)
    fetch_size = min(max(page_size * 2, page_size), 200)
    attempts = 0
    while len(result) < page_size and attempts < 4:
        chunk = ordered.offset(fetch_offset).limit(fetch_size).all()
        if not chunk:
            break
        for item in chunk:
            hydrated = _hydrate_session_preview(db, item)
            if hydrated is not None:
                result.append(hydrated)
            if len(result) >= page_size:
                break
        if len(chunk) < fetch_size:
            break
        fetch_offset += fetch_size
        attempts += 1

    return _paged_payload(result[:page_size], total, page, page_size)


def get_session_detail(db: Session, session_id: int, mark_read: bool = False) -> dict[str, Any]:
    item = db.query(CustomerSession).filter(CustomerSession.id == session_id).first()
    if not item:
        raise ValueError("会话不存在")
    if mark_read:
        item.unread_count = 0
        db.commit()
        db.refresh(item)
    messages = db.query(CustomerMessage).filter(CustomerMessage.session_id == item.id).order_by(CustomerMessage.id.asc()).limit(200).all()
    filtered_messages = []
    for row in messages:
        if row.direction != "operator" and should_block_customer_message(db, row.content_text or "", row.message_type or "text"):
            continue
        filtered_messages.append(_message_to_dict(row))
    session_data = _hydrate_session_preview(db, item) or _session_to_dict(item)
    return {"session": session_data, "messages": filtered_messages}


def mark_session_read(db: Session, session_id: int) -> dict[str, Any]:
    item = db.query(CustomerSession).filter(CustomerSession.id == session_id).first()
    if not item:
        raise ValueError("会话不存在")
    item.unread_count = 0
    db.commit()
    db.refresh(item)
    return {"ok": True, "session": _session_to_dict(item)}


def close_session(db: Session, session_id: int) -> dict[str, Any]:
    item = db.query(CustomerSession).filter(CustomerSession.id == session_id).first()
    if not item:
        raise ValueError("会话不存在")
    item.session_status = "closed"
    item.unread_count = 0
    db.commit()
    db.refresh(item)
    return {"ok": True, "session": _session_to_dict(item)}


def reopen_session(db: Session, session_id: int) -> dict[str, Any]:
    item = db.query(CustomerSession).filter(CustomerSession.id == session_id).first()
    if not item:
        raise ValueError("会话不存在")
    item.session_status = "open"
    db.commit()
    db.refresh(item)
    return {"ok": True, "session": _session_to_dict(item)}


def _send_customer_message(bot_token: str, chat_id: str, text: str) -> dict[str, Any]:
    if not bot_token:
        raise RuntimeError("原始商城机器人 token 不存在")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    with httpx.Client(timeout=20) as client:
        resp = client.post(url, json={"chat_id": chat_id, "text": text})
        data = resp.json()
        if resp.status_code >= 400 or not data.get("ok"):
            raise RuntimeError((data.get("description") if isinstance(data, dict) else None) or resp.text[:200])
        return data.get("result") or {}


def send_session_reply(db: Session, session_id: int, text: str, operator_name: str = "session_bot") -> dict[str, Any]:
    text = _safe_str(text)
    if not text:
        raise ValueError("回复内容不能为空")
    item = db.query(CustomerSession).filter(CustomerSession.id == session_id).first()
    if not item:
        raise ValueError("会话不存在")
    bot = db.query(BotConfig).filter(BotConfig.bot_code == item.bot_code).first()
    if not bot or not bot.bot_token:
        raise ValueError("原始商城机器人不存在或未配置 token")
    result = _send_customer_message(bot.bot_token, item.telegram_chat_id or item.telegram_user_id, text)
    now = datetime.utcnow()
    item.last_operator_reply_at = now
    item.updated_at = now
    item.unread_count = 0
    item.session_status = "open"
    item.last_message_text = text[:1000]
    item.last_message_type = "text"
    msg = CustomerMessage(
        session_id=item.id,
        bot_code=item.bot_code,
        telegram_user_id=item.telegram_user_id,
        direction="operator",
        sender_name=operator_name or "session_bot",
        message_type="text",
        content_text=text,
        content_json=_to_json({"operator_name": operator_name or "session_bot"}),
        telegram_message_id=str(result.get("message_id") or ""),
    )
    db.add(msg)
    db.commit()
    db.refresh(item)
    return {"ok": True, "session": _session_to_dict(item), "message": _message_to_dict(msg)}
