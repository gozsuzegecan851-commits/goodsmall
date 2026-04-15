from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN

from sqlalchemy.orm import Session

from ..config import settings
from ..models import PaymentAddress, PaymentOrder, Order


def _normalize_qr_image(url: str) -> tuple[str, str]:
    raw = (url or "").strip()
    if not raw:
        return "", ""
    if raw.startswith("/"):
        public = f"{settings.backend_public_url.rstrip('/')}{raw}"
        return raw, public
    for old in ("http://localhost:8001", "http://127.0.0.1:8001", "http://localhost:8002", "http://127.0.0.1:8002"):
        if raw.startswith(old):
            suffix = raw[len(old):]
            if suffix.startswith("/"):
                public = f"{settings.backend_public_url.rstrip('/')}{suffix}"
                return suffix, public
    return raw, raw


def _normalize_amount(value: Decimal | str | int | float) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        amount = Decimal('0')
    return amount.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)


def _offset_candidates(base_amount: Decimal) -> list[tuple[Decimal, Decimal]]:
    if not settings.payment_amount_offset_enabled:
        return [(base_amount, Decimal('0'))]
    try:
        step = _normalize_amount(settings.payment_amount_offset_step)
    except Exception:
        step = Decimal('0.001000')
    if step <= 0:
        return [(base_amount, Decimal('0'))]
    slots = max(1, int(settings.payment_amount_offset_slots or 1))
    candidates: list[tuple[Decimal, Decimal]] = [(base_amount, Decimal('0'))]
    for idx in range(1, slots + 1):
        offset = _normalize_amount(step * idx)
        candidates.append((_normalize_amount(base_amount + offset), offset))
    return candidates


def _pick_payment_address(db: Session) -> PaymentAddress | None:
    return (
        db.query(PaymentAddress)
        .filter(PaymentAddress.is_active == True)
        .order_by(PaymentAddress.last_used_at.asc().nullsfirst(), PaymentAddress.sort_order.asc(), PaymentAddress.id.asc())
        .first()
    )


def _pick_expected_amount(db: Session, address: PaymentAddress, order: Order) -> tuple[Decimal, Decimal]:
    base_amount = _normalize_amount(order.payable_amount)
    now = datetime.utcnow()
    active_rows = (
        db.query(PaymentOrder.expected_amount)
        .filter(
            PaymentOrder.receive_address == address.address,
            PaymentOrder.confirm_status == 'pending',
            PaymentOrder.expired_at > now,
        )
        .all()
    )
    occupied = {_normalize_amount(row[0]) for row in active_rows if row and row[0] is not None}
    for amount, offset in _offset_candidates(base_amount):
        if amount not in occupied:
            return amount, offset
    raise ValueError('当前收款地址的待支付金额占位已满，请稍后重试或增加收款地址')


def serialize_payment(payment: PaymentOrder, address: PaymentAddress | None = None) -> dict:
    qr_image, qr_image_public = _normalize_qr_image(address.qr_image if address else "")
    return {
        "order_id": payment.order_id,
        "pay_method": payment.pay_method,
        "receive_address": payment.receive_address,
        "expected_amount": str(payment.expected_amount),
        "base_amount": str(getattr(payment, 'base_amount', payment.expected_amount)),
        "amount_offset": str(getattr(payment, 'amount_offset', Decimal('0'))),
        "paid_amount": str(payment.paid_amount),
        "confirm_status": payment.confirm_status,
        "txid": payment.txid or "",
        "from_address": payment.from_address or "",
        "to_address": payment.to_address or "",
        "expired_at": payment.expired_at.isoformat() if payment.expired_at else "",
        "qr_image": qr_image,
        "qr_image_public": qr_image_public,
        "address_label": address.address_label if address else "",
    }


def get_latest_payment_order(db: Session, order_id: int) -> PaymentOrder | None:
    return (
        db.query(PaymentOrder)
        .filter(PaymentOrder.order_id == order_id)
        .order_by(PaymentOrder.id.desc())
        .first()
    )


def create_payment_order(db: Session, order_id: int) -> PaymentOrder:
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise ValueError("订单不存在")
    if (order.order_status or '').strip() == 'cancelled':
        raise ValueError('订单已关闭，不能再创建支付单')
    if order.pay_status == "paid":
        latest = get_latest_payment_order(db, order_id)
        if latest:
            return latest
        raise ValueError("订单已支付")

    existing = (
        db.query(PaymentOrder)
        .filter(
            PaymentOrder.order_id == order_id,
            PaymentOrder.confirm_status == "pending",
        )
        .order_by(PaymentOrder.id.desc())
        .first()
    )
    if existing and existing.expired_at and existing.expired_at > datetime.utcnow():
        return existing

    address = _pick_payment_address(db)
    if not address:
        raise ValueError("没有可用收款地址")

    expected_amount, amount_offset = _pick_expected_amount(db, address, order)
    payment = PaymentOrder(
        order_id=order.id,
        receive_address=address.address,
        expected_amount=expected_amount,
        base_amount=_normalize_amount(order.payable_amount),
        amount_offset=amount_offset,
        expired_at=datetime.utcnow() + timedelta(minutes=settings.payment_expire_minutes),
    )
    address.last_used_at = datetime.utcnow()
    db.add(payment)
    db.commit()
    db.refresh(payment)
    return payment
