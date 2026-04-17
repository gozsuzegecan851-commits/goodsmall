from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from ..models import Order, PaymentOrder
from .order_service import mark_order_paid_state


def _to_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def simulate_payment_finalize_enabled() -> bool:
    return _to_bool("ADMIN_SIMULATE_PAYMENT_ENABLED", False)


def get_order_and_latest_payment_for_update(db: Session, order_id: int) -> tuple[Order, PaymentOrder]:
    order = (
        db.query(Order)
        .filter(Order.id == order_id)
        .with_for_update()
        .first()
    )
    if not order:
        raise ValueError("订单不存在")

    payment = (
        db.query(PaymentOrder)
        .filter(PaymentOrder.order_id == order.id)
        .order_by(PaymentOrder.id.desc())
        .with_for_update()
        .first()
    )
    if not payment:
        raise ValueError("该订单暂无支付单")

    return order, payment


def _build_simulated_txid(db: Session) -> str:
    while True:
        txid = f"SIM-{datetime.utcnow():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:12].upper()}"
        exists = db.query(PaymentOrder.id).filter(PaymentOrder.txid == txid).first()
        if not exists:
            return txid


def finalize_payment_success(
    db: Session,
    order: Order,
    payment: PaymentOrder,
    *,
    paid_amount: Decimal,
    txid: str,
    from_address: str,
    to_address: str,
    paid_at: datetime,
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    payment.paid_amount = paid_amount
    payment.txid = txid
    payment.from_address = from_address
    payment.to_address = to_address
    payment.confirm_status = "confirmed"
    payment.paid_at = paid_at
    payment.raw_json = json.dumps(raw_payload, ensure_ascii=False)
    payment.updated_at = datetime.utcnow()

    mark_order_paid_state(db, order, paid_at=paid_at)

    return {
        "txid": payment.txid,
        "paid_amount": str(payment.paid_amount),
        "paid_at": payment.paid_at.isoformat() if payment.paid_at else "",
        "confirm_status": payment.confirm_status,
    }


def simulate_payment_success(
    db: Session,
    order: Order,
    payment: PaymentOrder,
    *,
    operator: str,
) -> dict[str, Any]:
    if str(order.order_status or "").strip().lower() == "cancelled":
        raise ValueError("已取消订单不允许模拟支付成功")
    if str(order.pay_status or "").strip().lower() == "paid":
        raise ValueError("订单已支付，无需模拟确认")
    if str(payment.confirm_status or "").strip().lower() in {"confirmed", "paid", "success"}:
        raise ValueError("支付单已确认，无需模拟确认")

    to_address = str(payment.receive_address or "").strip()
    if not to_address:
        raise ValueError("支付单缺少收款地址")

    now = datetime.utcnow()
    txid = _build_simulated_txid(db)
    paid_amount = Decimal(str(payment.expected_amount or 0))
    raw_payload = {
        "source": "admin_simulated",
        "action": "simulate_payment_success",
        "operator": str(operator or "").strip() or "unknown",
        "simulated_at": now.isoformat(),
        "txid": txid,
        "paid_amount": str(paid_amount),
        "receive_address": to_address,
    }

    return finalize_payment_success(
        db,
        order,
        payment,
        paid_amount=paid_amount,
        txid=txid,
        from_address="SIMULATED_ADMIN",
        to_address=to_address,
        paid_at=now,
        raw_payload=raw_payload,
    )
