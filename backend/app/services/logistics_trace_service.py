"""实时物流轨迹查询（主动查询，不写库、不订阅）。"""

from __future__ import annotations

from typing import Any

import httpx
from sqlalchemy.orm import Session

from ..config import settings
from ..jobs.logistics_sync import STATE_MAP, _query_kuaidi100
from ..models import Order, Shipment


def _normalize_owner(value: str | None) -> str:
    return str(value or "").strip()


def _is_real_value(value: str | None) -> bool:
    return bool(value) and not str(value).startswith("replace_with_")


def _status_text(code: str) -> str:
    mapping = {
        "not_shipped": "待发货",
        "shipped": "运输中",
        "signed": "已签收",
        "returned": "退回",
        "pending": "待揽收",
    }
    return mapping.get(code, code or "运输中")


def _trace_time(tr: dict[str, Any]) -> str:
    return str(tr.get("ftime") or tr.get("time") or "").strip()


def _trace_context(tr: dict[str, Any]) -> str:
    return str(tr.get("context") or "").strip()


def query_order_trace(db: Session, order_id: int, customer_id: str | None = None) -> dict[str, Any]:
    """
    根据订单 ID 查询实时物流轨迹。
    - 优先使用最新一条 Shipment 的 courier_code / tracking_no
    - 否则回退到 Order 上的 courier_code / tracking_no
    """
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise ValueError("订单不存在")

    if customer_id is not None:
        owner = _normalize_owner(customer_id)
        if owner and _normalize_owner(order.telegram_user_id) != owner:
            raise ValueError("无权查看该订单")

    shipment = db.query(Shipment).filter(Shipment.order_id == order.id).order_by(Shipment.id.desc()).first()

    tracking_no = (shipment.tracking_no if shipment and shipment.tracking_no else order.tracking_no) or ""
    tracking_no = str(tracking_no).strip()
    courier_code = (shipment.courier_code if shipment and shipment.courier_code else order.courier_code) or ""
    courier_code = str(courier_code).strip().lower()
    courier_name = (shipment.courier_company if shipment and shipment.courier_company else order.courier_company) or ""
    courier_name = str(courier_name).strip()

    if not tracking_no:
        raise ValueError("暂未发货，暂无物流单号")
    if not courier_code:
        raise ValueError("物流信息不完整，暂无法查询物流")

    if settings.logistics_provider != "kuaidi100":
        raise ValueError("物流查询暂不可用，请稍后再试")

    if not (_is_real_value(settings.kuaidi100_key) and _is_real_value(settings.kuaidi100_customer)):
        raise ValueError("物流查询暂不可用，请稍后再试")

    try:
        data = _query_kuaidi100(courier_code, tracking_no)
    except httpx.TimeoutException:
        raise ValueError("物流查询稍有延迟，请稍后重试") from None
    except httpx.RequestError:
        raise ValueError("物流查询稍有延迟，请稍后重试") from None
    except httpx.HTTPStatusError:
        raise ValueError("物流查询稍有延迟，请稍后重试") from None

    if data.get("result") is False:
        raise ValueError("已录入快递单号，暂未查询到轨迹，请稍后再试")

    raw_state = str(data.get("state") or "")
    traces = data.get("data") or []
    if not isinstance(traces, list):
        traces = []

    status_code = STATE_MAP.get(raw_state, "shipped")

    latest: dict[str, str] = {"time": "", "context": ""}
    if traces:
        t0 = traces[0] or {}
        latest = {"time": _trace_time(t0), "context": _trace_context(t0)}

    recent: list[dict[str, str]] = []
    for tr in traces[:3]:
        if not isinstance(tr, dict):
            continue
        recent.append({"time": _trace_time(tr), "context": _trace_context(tr)})

    code_out = courier_code or ""
    return {
        "order_id": order.id,
        "tracking_no": tracking_no,
        "courier_code": code_out,
        "shipper_code": code_out,
        "courier_name": courier_name,
        "status_code": status_code,
        "status_text": _status_text(status_code),
        "latest_trace": latest,
        "recent_traces": recent,
    }
