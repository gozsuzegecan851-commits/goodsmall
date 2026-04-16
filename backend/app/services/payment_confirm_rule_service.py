from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any


AMOUNT_EPSILON = Decimal("0.000001")


def is_real_api_key(value: str) -> bool:
    return bool(value) and not str(value).startswith("replace_with_")


def validate_confirm_runtime(api_key: str, contract: str) -> tuple[bool, str]:
    normalized_api_key = str(api_key or "").strip()
    if not normalized_api_key:
        return False, "未配置 TRONGRID_API_KEY"
    if not is_real_api_key(normalized_api_key):
        return False, "TRONGRID_API_KEY 仍为占位值"

    normalized_contract = str(contract or "").strip()
    if not normalized_contract:
        return False, "未配置 USDT_TRC20_CONTRACT"

    return True, ""


def normalize_amount(value: Decimal | str | int | float | None) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        amount = Decimal("0")
    return amount.quantize(AMOUNT_EPSILON, rounding=ROUND_DOWN)


def extract_txid(tx: dict[str, Any]) -> str:
    return str(tx.get("transaction_id") or tx.get("txid") or tx.get("id") or "").strip()


def match_tx_for_payment(
    tx: dict[str, Any],
    *,
    receive_address: str,
    expected_amount: Decimal | str | int | float,
    paid_amount: Decimal | str | int | float,
    expected_contract: str,
) -> tuple[bool, str, str]:
    txid = extract_txid(tx)
    normalized_paid_amount = normalize_amount(paid_amount)

    if not txid:
        return False, txid, "txid 为空"

    receive = str(receive_address or "").strip().lower()
    to_addr = str(tx.get("to") or tx.get("to_address") or tx.get("toAddress") or "").strip().lower()
    if not receive or not to_addr or to_addr != receive:
        return False, txid, f"收款地址不匹配: to={to_addr or '-'}"

    contract = str(expected_contract or "").strip().lower()
    if not contract:
        return False, txid, "未配置 USDT_TRC20_CONTRACT"
    token_info = tx.get("token_info") or {}
    tx_contract = str(token_info.get("address") or "").strip().lower()
    if tx_contract != contract:
        return False, txid, f"合约不匹配: contract={tx_contract or '-'}"

    want_amount = normalize_amount(expected_amount)
    if normalized_paid_amount != want_amount:
        return False, txid, f"金额不匹配: expected={want_amount} got={normalized_paid_amount}"

    return True, txid, ""
