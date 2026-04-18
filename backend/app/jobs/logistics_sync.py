from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Order, Shipment, ShipmentTrace
from ..services.logistics_kdzs_service import (
    kdzs_configured,
    kdzs_trace_batch_subscribe,
    kdzs_trace_search,
    kdzs_trace_subscribe,
    normalize_kdzs_trace_summary,
)
from ..services.order_service import mark_order_shipped_state, mark_order_signed_state

STATE_MAP = {
    "0": "shipped",
    "1": "shipped",
    "2": "shipped",
    "3": "signed",
    "4": "returned",
    "5": "shipped",
    "8": "shipped",
    "14": "returned",
}


def _is_real_value(value: str) -> bool:
    return bool(value) and not str(value).startswith('replace_with_')


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue
    return None


def _friendly_kdzs_error(exc: BaseException) -> str:
    raw = str(exc).strip()
    low = raw.lower()
    if "快递助手未配置" in raw or "KDZS_APP" in raw:
        return "快递助手未配置，请在环境中设置 KDZS_APP_KEY / KDZS_APP_SECRET"
    if "timeout" in low or "timed out" in low:
        return "物流查询超时，请稍后重试"
    if "401" in raw or "403" in raw:
        return "快递助手鉴权失败，请检查密钥与网关配置"
    if "resolve" in low and "gateway" in low:
        return "无法连接快递助手网关，请检查 KDZS_GATEWAY_URL 或网络"
    if len(raw) > 200:
        return "物流同步失败，请稍后重试"
    return raw if len(raw) < 120 else "物流同步失败，请稍后重试"


def _friendly_kdzs_message(summary_msg: str) -> str:
    s = (summary_msg or "").strip()
    return s if s and len(s) < 200 else "快递助手返回异常，请稍后重试"


def _query_kuaidi100(courier_code: str, tracking_no: str) -> dict[str, Any]:
    param = {
        "com": courier_code.strip().lower(),
        "num": tracking_no.strip(),
        "resultv2": "4",
        "show": "0",
        "order": "desc",
        "lang": "zh",
    }
    param_text = json.dumps(param, ensure_ascii=False, separators=(",", ":"))
    sign_raw = f"{param_text}{settings.kuaidi100_key}{settings.kuaidi100_customer}"
    sign = hashlib.md5(sign_raw.encode('utf-8')).hexdigest().upper()
    with httpx.Client(timeout=20) as client:
        resp = client.post(
            "https://poll.kuaidi100.com/poll/query.do",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "customer": settings.kuaidi100_customer,
                "sign": sign,
                "param": param_text,
            },
        )
        resp.raise_for_status()
        return resp.json()


def _upsert_trace(db: Session, shipment_id: int, trace: dict[str, Any]) -> None:
    trace_time = _parse_time(trace.get('ftime') or trace.get('time'))
    trace_text = str(trace.get('context') or '').strip()
    trace_status = str(trace.get('status') or trace.get('statusCode') or '').strip()
    exists = (
        db.query(ShipmentTrace)
        .filter(
            ShipmentTrace.shipment_id == shipment_id,
            ShipmentTrace.trace_text == trace_text,
            ShipmentTrace.trace_status == trace_status,
        )
        .first()
    )
    if exists:
        if trace_time and not exists.trace_time:
            exists.trace_time = trace_time
        return
    db.add(
        ShipmentTrace(
            shipment_id=shipment_id,
            trace_time=trace_time,
            trace_status=trace_status,
            trace_text=trace_text,
            raw_json=json.dumps(trace, ensure_ascii=False),
        )
    )


def _mirror_order_from_shipment(db: Session, shipment: Shipment) -> None:
    order = db.query(Order).filter(Order.id == shipment.order_id).first()
    if not order:
        return
    order.courier_company = shipment.courier_company
    order.courier_code = shipment.courier_code
    order.tracking_no = shipment.tracking_no
    if shipment.ship_status == 'signed':
        mark_order_signed_state(db, order, signed_at=shipment.signed_at or shipment.last_trace_time or datetime.utcnow())
    elif order.delivery_status == 'not_shipped':
        mark_order_shipped_state(db, order, shipped_at=order.shipped_at or shipment.last_trace_time or datetime.utcnow())


def try_kdzs_subscribe_shipment(db: Session, shipment: Shipment) -> dict[str, Any]:
    """单票订阅（录单后触发）；返回人话 message，不写第三方堆栈。"""
    if not kdzs_configured():
        return {"ok": True, "skipped": True, "message": "快递助手未配置，已跳过订阅"}
    if (shipment.subscribe_status or "") == "subscribed":
        return {"ok": True, "skipped": True, "message": "已订阅"}
    if not shipment.courier_code or not shipment.tracking_no:
        return {"ok": False, "message": "缺少快递编码或单号，无法订阅"}
    tn = shipment.tracking_no.strip()
    cc = shipment.courier_code.strip()
    try:
        raw = kdzs_trace_subscribe(tn, cc)
        norm = normalize_kdzs_trace_summary(raw, tracking_no=tn, courier_code=cc, for_subscribe=True)
        shipment.subscribe_status = "subscribed" if norm.get("subscribe_ok") else "failed"
        if norm.get("subscribe_ok"):
            return {"ok": True, "message": "已向快递助手提交订阅"}
        return {"ok": False, "message": _friendly_kdzs_message(norm.get("message") or "订阅失败")}
    except Exception as e:
        shipment.subscribe_status = "failed"
        return {"ok": False, "message": _friendly_kdzs_error(e)}


def subscribe_pending_shipments_kdzs(db: Session, limit: int = 80) -> dict[str, Any]:
    """导入后批量订阅未订阅运单。"""
    if not kdzs_configured():
        return {"ok": True, "skipped": True, "message": "快递助手未配置"}
    rows = (
        db.query(Shipment)
        .filter(Shipment.tracking_no != '')
        .filter(
            or_(
                Shipment.subscribe_status.is_(None),
                Shipment.subscribe_status == '',
                Shipment.subscribe_status.in_(('none', 'failed')),
            )
        )
        .order_by(Shipment.id.desc())
        .limit(limit)
        .all()
    )
    if not rows:
        return {"ok": True, "count": 0, "message": "无待订阅运单"}
    items = [{"tracking_no": r.tracking_no, "courier_code": r.courier_code} for r in rows]
    try:
        raw = kdzs_trace_batch_subscribe(items)
        norm = normalize_kdzs_trace_summary(raw, for_subscribe=True)
        ok = bool(norm.get("subscribe_ok"))
        for r in rows:
            r.subscribe_status = "subscribed" if ok else "failed"
        return {
            "ok": ok,
            "count": len(rows),
            "message": "批量订阅已完成" if ok else _friendly_kdzs_message(norm.get("message") or "批量订阅失败"),
        }
    except Exception as e:
        for r in rows:
            r.subscribe_status = "failed"
        return {"ok": False, "count": len(rows), "message": _friendly_kdzs_error(e)}


def _sync_one_shipment_kdzs(db: Session, shipment: Shipment) -> dict[str, Any]:
    tn = str(shipment.tracking_no or "").strip()
    cc = str(shipment.courier_code or "").strip()

    # 订阅为主：尚未订阅则先尝试单票订阅
    if (shipment.subscribe_status or "") != "subscribed":
        try:
            raw_sub = kdzs_trace_subscribe(tn, cc)
            sub_norm = normalize_kdzs_trace_summary(raw_sub, tracking_no=tn, courier_code=cc, for_subscribe=True)
            shipment.subscribe_status = "subscribed" if sub_norm.get("subscribe_ok") else "failed"
        except Exception:
            shipment.subscribe_status = "failed"

    # 查询补偿：刷新摘要（不写全量轨迹表）
    try:
        raw = kdzs_trace_search(tn, cc)
        summary = normalize_kdzs_trace_summary(raw, tracking_no=tn, courier_code=cc, for_subscribe=False)
        if not summary.get("ok"):
            shipment.sync_status = 'error'
            shipment.sync_error = _friendly_kdzs_message(summary.get("message") or "")
            shipment.last_sync_at = datetime.utcnow()
            return {"ok": False, "message": shipment.sync_error}

        shipment.last_trace_text = str(summary.get("latest_trace_text") or "")
        shipment.last_trace_time = summary.get("latest_trace_time")
        shipment.ship_status = str(summary.get("status_code") or "shipped")
        shipment.provider_name = 'kdzs'
        shipment.raw_json = json.dumps(summary.get("raw_payload") or raw, ensure_ascii=False)
        shipment.sync_status = 'synced'
        shipment.sync_error = ''
        shipment.last_sync_at = datetime.utcnow()
        if shipment.ship_status == 'signed' and not shipment.signed_at:
            sa = summary.get("signed_at")
            shipment.signed_at = sa if isinstance(sa, datetime) else (shipment.last_trace_time or datetime.utcnow())
        _mirror_order_from_shipment(db, shipment)
        return {
            "ok": True,
            "shipment_id": shipment.id,
            "tracking_no": shipment.tracking_no,
            "ship_status": shipment.ship_status,
            "status_text": summary.get("status_text") or "",
            "last_trace_text": shipment.last_trace_text,
            "recent_traces": summary.get("recent_traces") or [],
            "provider": "kdzs",
            "message": "同步成功",
        }
    except Exception as e:
        shipment.sync_status = 'error'
        shipment.sync_error = _friendly_kdzs_error(e)
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": shipment.sync_error}


def sync_one_shipment(db: Session, shipment: Shipment) -> dict[str, Any]:
    if not shipment.courier_code or not shipment.tracking_no:
        shipment.sync_status = 'error'
        shipment.sync_error = '缺少快递编码或快递单号'
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": shipment.sync_error}

    if kdzs_configured():
        return _sync_one_shipment_kdzs(db, shipment)

    if settings.logistics_provider != 'kuaidi100':
        shipment.sync_status = 'error'
        shipment.sync_error = f"暂不支持的物流提供商: {settings.logistics_provider}"
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": shipment.sync_error}
    if not (_is_real_value(settings.kuaidi100_key) and _is_real_value(settings.kuaidi100_customer)):
        shipment.sync_status = 'error'
        shipment.sync_error = '快递100凭证未配置，物流同步跳过'
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "skipped": True, "message": shipment.sync_error}

    try:
        data = _query_kuaidi100(shipment.courier_code, shipment.tracking_no)
        raw_state = str(data.get('state') or '')
        traces = data.get('data') or []
        shipment.last_trace_text = str((traces[0] or {}).get('context') or '') if traces else ''
        shipment.last_trace_time = _parse_time((traces[0] or {}).get('ftime') or (traces[0] or {}).get('time')) if traces else None
        shipment.ship_status = STATE_MAP.get(raw_state, 'shipped')
        shipment.provider_name = 'kuaidi100'
        shipment.raw_json = json.dumps(data, ensure_ascii=False)
        shipment.sync_status = 'synced'
        shipment.sync_error = ''
        shipment.last_sync_at = datetime.utcnow()
        if raw_state == '3' and not shipment.signed_at:
            shipment.signed_at = shipment.last_trace_time or datetime.utcnow()
        for tr in traces:
            _upsert_trace(db, shipment.id, tr)
        _mirror_order_from_shipment(db, shipment)
        return {
            "ok": True,
            "shipment_id": shipment.id,
            "tracking_no": shipment.tracking_no,
            "ship_status": shipment.ship_status,
            "trace_count": len(traces),
            "last_trace_text": shipment.last_trace_text,
            "provider": "kuaidi100",
            "message": "同步成功",
        }
    except Exception as e:
        shipment.sync_status = 'error'
        shipment.sync_error = _friendly_kdzs_error(e)
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": shipment.sync_error}


def sync_logistics_once(db: Session) -> dict:
    rows = (
        db.query(Shipment)
        .filter(Shipment.tracking_no != '')
        .filter(Shipment.ship_status != 'signed')
        .order_by(Shipment.last_sync_at.asc().nullsfirst(), Shipment.id.asc())
        .all()
    )
    result = {
        "ok": True,
        "provider": "kdzs" if kdzs_configured() else (settings.logistics_provider or "kuaidi100"),
        "checked": len(rows),
        "updated": 0,
        "signed": 0,
        "skipped": False,
        "message": "",
    }
    kuaidi_ready = settings.logistics_provider == 'kuaidi100' and _is_real_value(settings.kuaidi100_key) and _is_real_value(settings.kuaidi100_customer)
    if not kdzs_configured() and not kuaidi_ready:
        result["skipped"] = True
        result["message"] = "快递助手与快递100均未就绪，后台同步跳过"
        return result
    for shipment in rows:
        try:
            item_result = sync_one_shipment(db, shipment)
            if item_result.get('ok'):
                result['updated'] += 1
                if item_result.get('ship_status') == 'signed':
                    result['signed'] += 1
        except Exception as e:
            result['message'] = _friendly_kdzs_error(e)
    db.commit()
    if not result['message']:
        result['message'] = f"checked={result['checked']} updated={result['updated']} signed={result['signed']}"
    return result
