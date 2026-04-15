from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Order, Shipment, ShipmentTrace

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


def sync_one_shipment(db: Session, shipment: Shipment) -> dict[str, Any]:
    if not shipment.courier_code or not shipment.tracking_no:
        shipment.sync_status = 'error'
        shipment.sync_error = '缺少快递编码或快递单号'
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": shipment.sync_error}
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
        order = db.query(Order).filter(Order.id == shipment.order_id).first()
        if order:
            order.courier_company = shipment.courier_company
            order.courier_code = shipment.courier_code
            order.tracking_no = shipment.tracking_no
            if shipment.ship_status == 'signed':
                mark_order_signed_state(db, order, signed_at=shipment.signed_at or shipment.last_trace_time or datetime.utcnow())
            elif order.delivery_status == 'not_shipped':
                mark_order_shipped_state(db, order, shipped_at=order.shipped_at or shipment.last_trace_time or datetime.utcnow())
        return {
            "ok": True,
            "shipment_id": shipment.id,
            "tracking_no": shipment.tracking_no,
            "ship_status": shipment.ship_status,
            "trace_count": len(traces),
            "last_trace_text": shipment.last_trace_text,
        }
    except Exception as e:
        shipment.sync_status = 'error'
        shipment.sync_error = str(e)
        shipment.last_sync_at = datetime.utcnow()
        return {"ok": False, "message": str(e)}


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
        "provider": settings.logistics_provider,
        "checked": len(rows),
        "updated": 0,
        "signed": 0,
        "skipped": False,
        "message": "",
    }
    if settings.logistics_provider == 'kuaidi100' and not (_is_real_value(settings.kuaidi100_key) and _is_real_value(settings.kuaidi100_customer)):
        result["skipped"] = True
        result["message"] = "快递100凭证未配置，物流同步跳过"
        return result
    for shipment in rows:
        try:
            item_result = sync_one_shipment(db, shipment)
            if item_result.get('ok'):
                result['updated'] += 1
                if item_result.get('ship_status') == 'signed':
                    result['signed'] += 1
        except Exception as e:
            result['message'] = str(e)
    db.commit()
    if not result['message']:
        result['message'] = f"checked={result['checked']} updated={result['updated']} signed={result['signed']}"
    return result
