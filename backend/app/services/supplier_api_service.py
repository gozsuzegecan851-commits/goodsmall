from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from ..models import Order, OrderItem, Supplier, OrderFulfillment, ProductSupplierMap


def _safe_decimal(v: Any) -> str:
    try:
        return str(Decimal(v or 0))
    except Exception:
        return '0'


def _resolve_supplier_item_meta(db: Session, product_id: int | None, supplier_id: int | None) -> dict[str, Any]:
    if not product_id or not supplier_id:
        return {"supplier_sku": ""}
    mapping = db.query(ProductSupplierMap).filter(
        ProductSupplierMap.product_id == product_id,
        ProductSupplierMap.supplier_id == supplier_id,
        ProductSupplierMap.is_active == True,
    ).order_by(ProductSupplierMap.is_default.desc(), ProductSupplierMap.priority.asc(), ProductSupplierMap.id.asc()).first()
    return {"supplier_sku": mapping.supplier_sku if mapping else ""}


def build_supplier_payload(db: Session, order: Order, supplier: Supplier) -> dict[str, Any]:
    fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.order_id == order.id, OrderFulfillment.supplier_id == supplier.id).order_by(OrderFulfillment.id.desc()).first()
    items = []
    item_rows = db.query(OrderItem).filter(OrderItem.order_id == order.id).order_by(OrderItem.id.asc()).all()
    for row in item_rows:
        meta = _resolve_supplier_item_meta(db, row.product_id, supplier.id)
        items.append({
            "product_name": row.product_name,
            "product_id": row.product_id,
            "sku_code": row.sku_code or "",
            "supplier_sku": meta.get("supplier_sku", ""),
            "qty": int(row.qty or 0),
            "unit_price": _safe_decimal(row.unit_price),
            "subtotal": _safe_decimal(row.subtotal),
        })

    receiver = {
        "name": order.customer_name or "",
        "phone": order.customer_phone or "",
        "province": order.province or "",
        "city": order.city or "",
        "district": order.district or "",
        "address_detail": order.address_detail or "",
        "postal_code": order.postal_code or "",
        "full_address": " ".join([x for x in [order.province, order.city, order.district, order.address_detail] if x]),
    }

    payload = {
        "mode": "skeleton",
        "supplier_code": supplier.supplier_code,
        "supplier_name": supplier.supplier_name,
        "template_type": supplier.template_type or "standard",
        "supplier_order_no": (fulfillment.supplier_order_no if fulfillment else "") or "",
        "order": {
            "id": order.id,
            "order_no": order.order_no,
            "payment_method": order.payment_method,
            "pay_status": order.pay_status,
            "payable_amount": _safe_decimal(order.payable_amount),
            "buyer_remark": order.buyer_remark or "",
            "seller_remark": order.seller_remark or "",
            "created_at": order.created_at.isoformat() if order.created_at else "",
            "paid_at": order.paid_at.isoformat() if order.paid_at else "",
        },
        "receiver": receiver,
        "items": items,
        "meta": {
            "api_base": supplier.api_base or "",
            "supplier_type": supplier.supplier_type or "manual",
            "shipping_bot_code": supplier.shipping_bot_code or "",
        },
    }
    return payload


def push_order_to_supplier(db: Session, order: Order, supplier: Supplier, fulfillment: OrderFulfillment | None = None, operator_name: str = 'admin') -> dict[str, Any]:
    payload = build_supplier_payload(db, order, supplier)
    if fulfillment is None:
        fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.order_id == order.id, OrderFulfillment.supplier_id == supplier.id).order_by(OrderFulfillment.id.desc()).first()
    if fulfillment is None:
        fulfillment = OrderFulfillment(order_id=order.id, supplier_id=supplier.id)
        db.add(fulfillment)
        db.flush()
    if not fulfillment.assigned_at:
        fulfillment.assigned_at = datetime.utcnow()
    fulfillment.fulfillment_status = 'pushed'
    fulfillment.sync_status = 'pending'
    preview = {
        "action": "push_order",
        "operator_name": operator_name,
        "message": "当前为供应链 API 对接骨架，暂不真实请求外部供应链。",
        "endpoint_preview": (supplier.api_base.rstrip('/') + '/orders') if supplier.api_base else '',
        "requested_at": datetime.utcnow().isoformat(),
        "payload": payload,
    }
    fulfillment.raw_json = json.dumps(preview, ensure_ascii=False)
    order.supplier_code = supplier.supplier_code or order.supplier_code
    return {
        "ok": True,
        "mode": "skeleton",
        "supplier_code": supplier.supplier_code,
        "supplier_name": supplier.supplier_name,
        "template_type": supplier.template_type or 'standard',
        "payload": payload,
        "endpoint_preview": preview['endpoint_preview'],
        "message": preview['message'],
    }


def pull_supplier_status(db: Session, fulfillment: OrderFulfillment, supplier: Supplier) -> dict[str, Any]:
    now = datetime.utcnow().isoformat()
    last = {}
    if fulfillment.raw_json:
        try:
            last = json.loads(fulfillment.raw_json)
        except Exception:
            last = {}
    data = {
        "ok": True,
        "mode": "skeleton",
        "supplier_code": supplier.supplier_code,
        "supplier_name": supplier.supplier_name,
        "supplier_order_no": fulfillment.supplier_order_no or '',
        "fulfillment_status": fulfillment.fulfillment_status or 'pushed',
        "sync_status": fulfillment.sync_status or 'pending',
        "message": "当前为供应链 API 状态查询骨架，暂不真实请求外部供应链。",
        "checked_at": now,
        "last_push_preview": last.get('payload') if isinstance(last, dict) else None,
    }
    fulfillment.sync_status = 'synced'
    fulfillment.sync_error = ''
    fulfillment.updated_at = datetime.utcnow()
    fulfillment.raw_json = json.dumps({"last_pull_preview": data, "last_push": last}, ensure_ascii=False)
    return data
