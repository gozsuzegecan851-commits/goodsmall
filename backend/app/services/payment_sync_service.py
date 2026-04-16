from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from sqlalchemy.orm import Session

from ..models import Order, PaymentOrder
from .order_service import mark_order_paid_state
from .payment_confirm_rule_service import match_tx_for_payment, validate_confirm_runtime


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def _to_decimal(value: Any, decimals: Any = None) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")

    try:
        if "." in text:
            return Decimal(text)
    except Exception:
        pass

    try:
        raw = Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")

    try:
        dec = int(decimals) if decimals is not None and str(decimals).strip() != "" else None
    except Exception:
        dec = None

    if dec is not None and dec >= 0 and "." not in text:
        return raw / (Decimal(10) ** dec)
    return raw


def _short_reason(item: dict[str, Any]) -> str:
    txid = str(item.get("transaction_id") or item.get("txid") or "").strip()
    amount = str(item.get("_normalized_amount") or "").strip()
    return f"txid={txid[:12]} amount={amount}" if txid or amount else "未命中匹配记录"


def refresh_payment_order_status(db: Session, order: Order, payment: PaymentOrder) -> dict[str, Any]:
    api_key = _env("TRONGRID_API_KEY")
    if not api_key:
        raise RuntimeError("未配置 TRONGRID_API_KEY")

    base_url = _env("TRONGRID_BASE_URL", "https://api.trongrid.io").rstrip("/")
    contract = _env("USDT_TRC20_CONTRACT")
    ok, reason = validate_confirm_runtime(api_key, contract)
    if not ok:
        raise RuntimeError(reason)

    lookback_minutes = int(_env("TRON_PAY_LOOKBACK_MINUTES", "30") or "30")
    receive_address = str(payment.receive_address or "").strip()
    if not receive_address:
        raise RuntimeError("支付单缺少收款地址")

    if str(payment.confirm_status or "").lower() in {"confirmed", "paid", "success"} or str(order.pay_status or "").lower() == "paid":
        return {
            "matched": True,
            "status": "confirmed",
            "reason": "支付已确认，无需重复刷新",
            "txid": str(payment.txid or ""),
        }

    expected_amount = _to_decimal(payment.expected_amount)
    min_ts_dt = (payment.created_at or datetime.utcnow()) - timedelta(minutes=max(1, lookback_minutes))
    min_ts = int(min_ts_dt.timestamp() * 1000)

    url = f"{base_url}/v1/accounts/{receive_address}/transactions/trc20"
    params = {
        "only_to": "true",
        "only_confirmed": "true",
        "limit": "50",
        "min_timestamp": str(min_ts),
        "order_by": "block_timestamp,desc",
        "contract_address": contract,
    }
    headers = {"TRON-PRO-API-KEY": api_key}

    with httpx.Client(timeout=30.0, headers=headers) as client:
        resp = client.get(url, params=params)
    if resp.status_code >= 400:
        raise RuntimeError(f"TronGrid 查询失败: HTTP {resp.status_code} {resp.text[:200]}")

    payload = resp.json()
    rows = payload.get("data") or []
    matched = None
    matched_amount = Decimal("0")
    matched_txid = ""
    last_reason = "链上暂未查到匹配金额的已确认 TRC20 入账"

    for item in rows:
        token_info = item.get("token_info") or {}
        amount = _to_decimal(item.get("value"), token_info.get("decimals"))
        ok, txid, reason = match_tx_for_payment(
            item,
            receive_address=receive_address,
            expected_amount=expected_amount,
            paid_amount=amount,
            expected_contract=contract,
        )
        item["_normalized_amount"] = str(amount)
        if not ok:
            last_reason = reason or _short_reason(item)
            continue

        tx_used = (
            db.query(PaymentOrder)
            .filter(PaymentOrder.txid == txid, PaymentOrder.id != payment.id)
            .first()
        )
        if tx_used:
            last_reason = f"txid 已被其他支付单占用: {txid[:12]}"
            continue

        matched = item
        matched_amount = amount
        matched_txid = txid
        break

    if not matched:
        payment.raw_json = json.dumps(payload, ensure_ascii=False)
        payment.updated_at = datetime.utcnow()
        db.flush()
        return {
            "matched": False,
            "status": str(payment.confirm_status or "pending"),
            "reason": last_reason,
        }

    payment.paid_amount = matched_amount
    payment.txid = matched_txid
    payment.from_address = str(matched.get("from") or matched.get("from_address") or matched.get("fromAddress") or "").strip()
    payment.to_address = str(matched.get("to") or matched.get("to_address") or matched.get("toAddress") or receive_address).strip()
    payment.confirm_status = "confirmed"
    payment.paid_at = datetime.utcnow()
    payment.raw_json = json.dumps(matched, ensure_ascii=False)
    payment.updated_at = datetime.utcnow()

    mark_order_paid_state(db, order, paid_at=payment.paid_at)
    db.flush()

    return {
        "matched": True,
        "status": "confirmed",
        "reason": _short_reason(matched),
        "txid": payment.txid,
        "paid_amount": str(payment.paid_amount),
    }
