from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN

import httpx
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Order, PaymentOrder
from ..services.order_service import mark_order_expired_state, mark_order_paid_state
from ..services.payment_confirm_rule_service import match_tx_for_payment, validate_confirm_runtime


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_decimal(value: Decimal | str | int | float) -> Decimal:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        dec = Decimal("0")
    return dec.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)


def _ms_to_dt(ms: int | str | None) -> datetime | None:
    if ms in (None, ""):
        return None
    try:
        ts = int(ms) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _parse_amount(tx: dict) -> Decimal:
    raw_value = tx.get("value", "0")
    token_info = tx.get("token_info") or {}
    try:
        raw_dec = Decimal(str(raw_value))
    except Exception:
        raw_dec = Decimal("0")
    text = str(raw_value)
    if "." in text:
        return _normalize_decimal(raw_dec)
    try:
        decimals = int(token_info.get("decimals") or 0)
    except Exception:
        decimals = 0
    if decimals > 0:
        raw_dec = raw_dec / (Decimal(10) ** decimals)
    return _normalize_decimal(raw_dec)


def _fetch_address_trc20(address: str, min_timestamp_ms: int) -> list[dict]:
    headers = {}
    headers["TRON-PRO-API-KEY"] = settings.trongrid_api_key
    params = {
        "only_to": "true",
        "only_confirmed": "true",
        "limit": 50,
        "min_timestamp": min_timestamp_ms,
    }
    url = f"{settings.usdt_tron_api_base.rstrip('/')}/v1/accounts/{address}/transactions/trc20"
    with httpx.Client(timeout=20, headers=headers) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data") or []


def _confirm_payment(db: Session, payment: PaymentOrder, order: Order, tx: dict, amount: Decimal) -> None:
    payment.paid_amount = amount
    payment.txid = str(tx.get("transaction_id") or tx.get("id") or "")
    payment.from_address = str(tx.get("from") or "")
    payment.to_address = str(tx.get("to") or payment.receive_address or "")
    payment.confirm_status = "confirmed"
    payment.paid_at = _ms_to_dt(tx.get("block_timestamp")) or _now_utc()
    payment.raw_json = json.dumps(tx, ensure_ascii=False)

    mark_order_paid_state(db, order, paid_at=payment.paid_at or _now_utc())
    order.updated_at = _now_utc()


def _expire_payment(db: Session, payment: PaymentOrder, order: Order | None) -> None:
    payment.confirm_status = 'expired'
    payment.updated_at = _now_utc()
    if order and order.pay_status != 'paid' and (order.delivery_status or 'not_shipped') not in {'shipped', 'signed'}:
        mark_order_expired_state(db, order)
        order.updated_at = _now_utc()


def poll_usdt_once(db: Session) -> dict:
    now = _now_utc()
    result = {
        "ok": True,
        "provider": "trongrid",
        "api_base": settings.usdt_tron_api_base,
        "checked": 0,
        "confirmed": 0,
        "expired": 0,
        "skipped": False,
        "message": "",
    }

    ok, reason = validate_confirm_runtime(settings.trongrid_api_key, settings.usdt_trc20_contract)
    if not ok:
        result["message"] = reason
        return result

    pending_rows = (
        db.query(PaymentOrder)
        .filter(PaymentOrder.confirm_status == "pending")
        .order_by(PaymentOrder.id.asc())
        .all()
    )

    result["checked"] = len(pending_rows)

    for payment in pending_rows:
        order = db.query(Order).filter(Order.id == payment.order_id).first()
        if not order:
            payment.confirm_status = "failed"
            continue

        if payment.expired_at and payment.expired_at < now:
            _expire_payment(db, payment, order)
            result["expired"] += 1
            continue

        start_time = payment.created_at or order.created_at or now
        min_ts = int(start_time.replace(tzinfo=timezone.utc).timestamp() * 1000)
        try:
            txs = _fetch_address_trc20(payment.receive_address, min_ts)
        except Exception as e:
            result["message"] = f"查询失败: {e}"
            continue

        expected_amount = _normalize_decimal(payment.expected_amount)
        for tx in txs:
            paid_amount = _parse_amount(tx)
            matched, txid, _reason = match_tx_for_payment(
                tx,
                receive_address=payment.receive_address,
                expected_amount=expected_amount,
                paid_amount=paid_amount,
                expected_contract=settings.usdt_trc20_contract,
            )
            if not matched:
                continue
            tx_used = (
                db.query(PaymentOrder)
                .filter(PaymentOrder.txid == txid, PaymentOrder.id != payment.id)
                .first()
            )
            if tx_used:
                continue
            _confirm_payment(db, payment, order, tx, paid_amount)
            result["confirmed"] += 1
            break

    db.commit()
    if not result["message"]:
        result["message"] = f"checked={result['checked']} confirmed={result['confirmed']} expired={result['expired']}"
    return result
