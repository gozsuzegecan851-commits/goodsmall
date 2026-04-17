from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from .deps import get_db
from . import schemas
from .models import (
    ProductCategory,
    Product,
    ShippingAddress,
    Order,
    OrderItem,
    Shipment,
    ShipmentTrace,
    PaymentAddress,
    PaymentOrder,
)
from .services.order_service import create_order
from .services.payment_usdt import (
    PaymentAllocationConflictError,
    PaymentAmountPoolExhaustedError,
    create_payment_order,
    get_latest_payment_order,
    serialize_payment,
)
from .jobs.logistics_sync import sync_one_shipment

router = APIRouter()


def _normalize_owner(value: str | None) -> str:
    return str(value or "").strip()


def _require_order_owner(order: Order, telegram_user_id: str | None) -> str:
    owner = _normalize_owner(telegram_user_id)
    if not owner:
        raise HTTPException(status_code=400, detail="telegram_user_id 不能为空")
    if _normalize_owner(order.telegram_user_id) != owner:
        raise HTTPException(status_code=403, detail="无权查看该订单")
    return owner


def normalize_payment_status(order: Order, payment: PaymentOrder | None) -> str:
    if not payment:
        return "pending"
    if payment.confirm_status in {"confirmed", "paid", "success"}:
        return "confirmed"
    if order.pay_status == "paid":
        return "confirmed"
    return payment.confirm_status or "pending"


def payment_status_text(status: str) -> str:
    mapping = {
        "pending": "待支付",
        "confirmed": "已确认",
        "expired": "已过期",
        "failed": "失败",
    }
    return mapping.get(status, status or "待支付")


def delivery_status_text(status: str) -> str:
    mapping = {
        "not_shipped": "待发货",
        "shipped": "运输中",
        "signed": "已签收",
    }
    return mapping.get(status, status or "待发货")


def pay_status_text(status: str) -> str:
    mapping = {
        "pending": "待支付",
        "paid": "已支付",
        "failed": "失败",
    }
    return mapping.get(status, status or "待支付")


@router.get("/health")
def health():
    return {"ok": True}



def product_public_sku_to_dict(s) -> dict:
    return {
        "id": s.id,
        "sku_code": s.sku_code or "",
        "sku_name": s.sku_name or "",
        "spec_text": s.spec_text or "",
        "price_cny": str(s.price_cny),
        "original_price_cny": str(s.original_price_cny),
        "stock_qty": int(s.stock_qty or 0),
        "weight_gram": int(s.weight_gram or 0),
        "unit_text": s.unit_text or "件",
        "cover_image": s.cover_image or "",
        "is_active": bool(s.is_active),
        "sort_order": int(s.sort_order or 100),
    }

def pick_public_default_sku(skus):
    if not skus:
        return None
    active = [s for s in skus if bool(s.is_active)]
    rows = active or skus
    rows = sorted(rows, key=lambda x: (int(x.sort_order or 100), int(x.id or 0)))
    return rows[0] if rows else None

def product_public_dict(item: Product, include_skus: bool = True) -> dict:
    skus = sorted(list(getattr(item, 'skus', []) or []), key=lambda x: (int(x.sort_order or 100), int(x.id or 0)))
    default_sku = pick_public_default_sku(skus)
    price = default_sku.price_cny if default_sku else item.price_cny
    original_price = default_sku.original_price_cny if default_sku else item.original_price_cny
    stock_qty = default_sku.stock_qty if default_sku else item.stock_qty
    sku_code = default_sku.sku_code if default_sku and default_sku.sku_code else item.sku_code
    unit_text = default_sku.unit_text if default_sku and default_sku.unit_text else (item.unit_text or "件")
    weight_gram = default_sku.weight_gram if default_sku else item.weight_gram
    data = {
        "id": item.id,
        "category_id": item.category_id,
        "name": item.name,
        "subtitle": item.subtitle or "",
        "sku_code": sku_code or "",
        "cover_image": item.cover_image or "",
        "gallery_images_json": item.gallery_images_json or "[]",
        "price_cny": str(price),
        "original_price_cny": str(original_price),
        "stock_qty": int(stock_qty or 0),
        "weight_gram": int(weight_gram or 0),
        "unit_text": unit_text or "件",
        "description": item.description or "",
        "detail_html": item.detail_html or "",
    }
    if include_skus:
        data["sku_list"] = [product_public_sku_to_dict(s) for s in skus if bool(s.is_active)]
    return data

@router.get("/catalog/categories")
def catalog_categories(db: Session = Depends(get_db)):
    rows = (
        db.query(ProductCategory)
        .filter(ProductCategory.is_active == True)
        .order_by(ProductCategory.sort_order.asc(), ProductCategory.id.asc())
        .all()
    )
    return [
        {
            "id": r.id,
            "name": r.name,
            "cover_image": r.cover_image,
            "sort_order": r.sort_order,
        }
        for r in rows
    ]


@router.get("/catalog/products")
def catalog_products(category_id: int | None = None, db: Session = Depends(get_db)):
    q = db.query(Product).join(ProductCategory, ProductCategory.id == Product.category_id).filter(
        Product.is_active == True,
        ProductCategory.is_active == True,
    )
    if category_id:
        q = q.filter(Product.category_id == category_id)
    rows = q.order_by(Product.sort_order.asc(), Product.id.asc()).all()
    return [product_public_dict(r, include_skus=True) for r in rows]

@router.get("/catalog/products/{product_id}")
def catalog_product_detail(product_id: int, db: Session = Depends(get_db)):
    item = db.query(Product).filter(Product.id == product_id, Product.is_active == True).first()
    if not item:
        raise HTTPException(status_code=404, detail="商品不存在")
    return product_public_dict(item, include_skus=True)

@router.get("/addresses")
def list_addresses(telegram_user_id: str, db: Session = Depends(get_db)):
    rows = (
        db.query(ShippingAddress)
        .filter(ShippingAddress.telegram_user_id == telegram_user_id)
        .order_by(ShippingAddress.is_default.desc(), ShippingAddress.id.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "receiver_name": r.receiver_name,
            "receiver_phone": r.receiver_phone,
            "province": r.province,
            "city": r.city,
            "district": r.district,
            "address_detail": r.address_detail,
            "postal_code": r.postal_code,
            "is_default": r.is_default,
        }
        for r in rows
    ]


@router.post("/addresses")
def save_address(payload: schemas.AddressIn, db: Session = Depends(get_db)):
    item = db.query(ShippingAddress).filter(ShippingAddress.id == payload.id).first() if payload.id else ShippingAddress()
    if payload.id and not item:
        raise HTTPException(status_code=404, detail="地址不存在")
    if payload.id and item and str(item.telegram_user_id or "") != str(payload.telegram_user_id or ""):
        raise HTTPException(status_code=403, detail="无权修改该地址")
    is_new = not payload.id
    if is_new:
        db.add(item)

    data = payload.model_dump()
    for k, v in data.items():
        if k != "id":
            setattr(item, k, v)

    db.flush()

    if payload.is_default:
        db.query(ShippingAddress).filter(
            ShippingAddress.telegram_user_id == payload.telegram_user_id,
            ShippingAddress.id != item.id,
        ).update({"is_default": False}, synchronize_session=False)
        item.is_default = True
    else:
        existing_count = (
            db.query(ShippingAddress)
            .filter(ShippingAddress.telegram_user_id == payload.telegram_user_id)
            .count()
        )
        if existing_count == 1:
            item.is_default = True

    db.commit()
    db.refresh(item)
    return {"ok": True, "id": item.id}


@router.post("/addresses/{address_id}/default")
def set_default_address(address_id: int, telegram_user_id: str = Query(...), db: Session = Depends(get_db)):
    item = db.query(ShippingAddress).filter(ShippingAddress.id == address_id).first()
    if not item or item.telegram_user_id != telegram_user_id:
        raise HTTPException(status_code=404, detail="地址不存在")
    db.query(ShippingAddress).filter(
        ShippingAddress.telegram_user_id == telegram_user_id,
        ShippingAddress.id != item.id,
    ).update({"is_default": False}, synchronize_session=False)
    item.is_default = True
    db.commit()
    return {"ok": True}


@router.delete("/addresses/{address_id}")
def delete_address(address_id: int, telegram_user_id: str = Query(...), db: Session = Depends(get_db)):
    item = db.query(ShippingAddress).filter(ShippingAddress.id == address_id).first()
    if not item or item.telegram_user_id != telegram_user_id:
        raise HTTPException(status_code=404, detail="地址不存在")
    was_default = bool(item.is_default)
    db.delete(item)
    db.commit()
    if was_default:
        next_item = (
            db.query(ShippingAddress)
            .filter(ShippingAddress.telegram_user_id == telegram_user_id)
            .order_by(ShippingAddress.id.desc())
            .first()
        )
        if next_item:
            next_item.is_default = True
            db.commit()
    return {"ok": True}


@router.post("/orders/create")
def public_create_order(payload: schemas.OrderCreateIn, db: Session = Depends(get_db)):
    try:
        order = create_order(db, payload)
        return {"ok": True, "order_id": order.id, "order_no": order.order_no}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/orders")
def public_orders(telegram_user_id: str, db: Session = Depends(get_db)):
    rows = db.query(Order).filter(Order.telegram_user_id == telegram_user_id).order_by(Order.id.desc()).all()
    return [
        {
            "id": r.id,
            "order_no": r.order_no,
            "payable_amount": str(r.payable_amount),
            "pay_status": r.pay_status,
            "pay_status_text": pay_status_text(r.pay_status),
            "order_status": r.order_status,
            "delivery_status": r.delivery_status,
            "delivery_status_text": delivery_status_text(r.delivery_status),
            "tracking_no": r.tracking_no,
            "courier_company": r.courier_company,
            "created_at": r.created_at.isoformat() if r.created_at else "",
            "paid_at": r.paid_at.isoformat() if r.paid_at else "",
            "shipped_at": r.shipped_at.isoformat() if r.shipped_at else "",
        }
        for r in rows
    ]


@router.get("/orders/{order_id}")
def public_order_detail(order_id: int, telegram_user_id: str | None = None, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="订单不存在")
    _require_order_owner(order, telegram_user_id)
    items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()
    shipment = db.query(Shipment).filter(Shipment.order_id == order.id).order_by(Shipment.id.desc()).first()
    traces = (
        db.query(ShipmentTrace)
        .filter(ShipmentTrace.shipment_id == shipment.id)
        .order_by(ShipmentTrace.id.desc())
        .limit(5)
        .all()
        if shipment
        else []
    )
    payment = get_latest_payment_order(db, order.id)
    payment_address = None
    if payment:
        payment_address = db.query(PaymentAddress).filter(PaymentAddress.address == payment.receive_address).first()
    display_confirm_status = normalize_payment_status(order, payment)
    return {
        "id": order.id,
        "order_no": order.order_no,
        "payable_amount": str(order.payable_amount),
        "pay_status": order.pay_status,
        "pay_status_text": pay_status_text(order.pay_status),
        "order_status": order.order_status,
        "delivery_status": order.delivery_status,
        "delivery_status_text": delivery_status_text(order.delivery_status),
        "customer_name": order.customer_name,
        "customer_phone": order.customer_phone,
        "province": order.province,
        "city": order.city,
        "district": order.district,
        "address_detail": order.address_detail,
        "postal_code": order.postal_code,
        "courier_company": order.courier_company,
        "tracking_no": order.tracking_no,
        "created_at": order.created_at.isoformat() if order.created_at else "",
        "paid_at": order.paid_at.isoformat() if order.paid_at else "",
        "shipped_at": order.shipped_at.isoformat() if order.shipped_at else "",
        "payment": ({
            **serialize_payment(payment, payment_address),
            "confirm_status": display_confirm_status,
            "confirm_status_text": payment_status_text(display_confirm_status),
            "paid_at": payment.paid_at.isoformat() if payment and payment.paid_at else (order.paid_at.isoformat() if order.paid_at else ""),
        } if payment else None),
        "items": [
            {
                "product_name": i.product_name,
                "qty": i.qty,
                "sku_code": i.sku_code,
                "unit_price": str(i.unit_price),
                "subtotal": str(i.subtotal),
            }
            for i in items
        ],
        "shipment": {
            "courier_company": shipment.courier_company,
            "courier_code": shipment.courier_code,
            "tracking_no": shipment.tracking_no,
            "ship_status": shipment.ship_status,
            "ship_status_text": delivery_status_text(shipment.ship_status),
            "last_trace_text": shipment.last_trace_text,
            "last_trace_time": shipment.last_trace_time.isoformat() if shipment.last_trace_time else "",
        } if shipment else None,
        "traces": [
            {
                "trace_time": t.trace_time.isoformat() if t.trace_time else "",
                "trace_status": t.trace_status,
                "trace_text": t.trace_text,
            }
            for t in traces
        ],
    }


@router.post("/payments/usdt/create")
def public_create_usdt_payment(payload: schemas.PaymentCreateIn, db: Session = Depends(get_db)):
    try:
        order = db.query(Order).filter(Order.id == payload.order_id).first()
        if not order:
            raise ValueError("订单不存在")
        _require_order_owner(order, payload.telegram_user_id)
        payment = create_payment_order(db, payload.order_id)
        addr = db.query(PaymentAddress).filter(PaymentAddress.address == payment.receive_address).first()
        return {
            "ok": True,
            **serialize_payment(payment, addr),
        }
    except PaymentAmountPoolExhaustedError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except PaymentAllocationConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/payments/usdt/{order_id}")
def public_get_usdt_payment(order_id: int, telegram_user_id: str | None = None, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="订单不存在")
    _require_order_owner(order, telegram_user_id)
    payment = get_latest_payment_order(db, order_id)
    if not payment:
        raise HTTPException(status_code=404, detail="支付单不存在")
    addr = db.query(PaymentAddress).filter(PaymentAddress.address == payment.receive_address).first()
    return {"ok": True, **serialize_payment(payment, addr)}


@router.post("/orders/{order_id}/sync-logistics")
def public_sync_order_logistics(order_id: int, telegram_user_id: str | None = None, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="订单不存在")
    _require_order_owner(order, telegram_user_id)
    shipment = db.query(Shipment).filter(Shipment.order_id == order.id).order_by(Shipment.id.desc()).first()
    if not shipment:
        raise HTTPException(status_code=400, detail="订单还未发货")
    result = sync_one_shipment(db, shipment)
    db.commit()
    return result
