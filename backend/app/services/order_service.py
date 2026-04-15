from datetime import datetime
from decimal import Decimal
import uuid
from sqlalchemy.orm import Session
from ..models import Order, OrderItem, Product, ProductSku, ShippingAddress, ProductSupplierMap, Supplier, OrderFulfillment


def _active_supplier_mappings(db: Session, product_id: int):
    return (
        db.query(ProductSupplierMap, Supplier)
        .join(Supplier, Supplier.id == ProductSupplierMap.supplier_id)
        .filter(
            ProductSupplierMap.product_id == product_id,
            ProductSupplierMap.is_active == True,
            Supplier.is_active == True,
        )
        .order_by(ProductSupplierMap.is_default.desc(), ProductSupplierMap.priority.asc(), ProductSupplierMap.id.asc())
        .all()
    )


def resolve_order_supplier(db: Session, product_ids: list[int]):
    if not product_ids:
        return None, '订单无商品，无法指派供应链'
    per_product = []
    for pid in product_ids:
        pairs = _active_supplier_mappings(db, pid)
        if not pairs:
            return None, f'商品 {pid} 未绑定可用供应链'
        ranked = []
        for rank, (mapping, supplier) in enumerate(pairs):
            ranked.append({
                'supplier': supplier,
                'supplier_id': supplier.id,
                'supplier_code': supplier.supplier_code,
                'is_default': bool(mapping.is_default),
                'priority': int(mapping.priority or 100),
                'rank': rank,
            })
        per_product.append(ranked)
    if len(per_product) == 1:
        chosen = per_product[0][0]['supplier']
        return chosen, f'按商品默认供应链自动指派：{chosen.supplier_code}'
    common_ids = set(entry['supplier_id'] for entry in per_product[0])
    for ranked in per_product[1:]:
        common_ids &= {entry['supplier_id'] for entry in ranked}
    if not common_ids:
        return None, '订单商品分属不同供应链，需人工指派'
    candidates = []
    for sid in common_ids:
        default_bonus = 0
        score = 0
        supplier_obj = None
        for ranked in per_product:
            entry = next(e for e in ranked if e['supplier_id'] == sid)
            supplier_obj = entry['supplier']
            default_bonus += 1 if entry['is_default'] else 0
            score += entry['priority'] + entry['rank'] * 100
        candidates.append((sid, default_bonus, score, supplier_obj))
    candidates.sort(key=lambda x: (-x[1], x[2], x[0]))
    chosen = candidates[0][3]
    return chosen, f'按共用供应链自动指派：{chosen.supplier_code}'


def get_latest_fulfillment(db: Session, order_id: int) -> OrderFulfillment | None:
    return db.query(OrderFulfillment).filter(OrderFulfillment.order_id == order_id).order_by(OrderFulfillment.id.desc()).first()


def _find_supplier_by_code(db: Session, supplier_code: str) -> Supplier | None:
    code = str(supplier_code or '').strip()
    if not code:
        return None
    return db.query(Supplier).filter(Supplier.supplier_code == code).first()


def _append_seller_remark(order: Order, text: str) -> None:
    note = str(text or '').strip()
    if not note:
        return
    current = str(order.seller_remark or '').strip()
    order.seller_remark = note if not current else f"{current}\n{note}"


def ensure_order_fulfillment(db: Session, order: Order) -> OrderFulfillment | None:
    fulfillment = get_latest_fulfillment(db, order.id)
    if fulfillment is not None:
        return fulfillment
    supplier = _find_supplier_by_code(db, order.supplier_code)
    if supplier is None:
        return None
    fulfillment = OrderFulfillment(
        order_id=order.id,
        supplier_id=supplier.id,
        fulfillment_status='assigned',
        assigned_at=order.created_at or datetime.utcnow(),
        sync_status='pending',
        sync_error='',
    )
    db.add(fulfillment)
    db.flush()
    return fulfillment


def release_order_stock(db: Session, order: Order, reason: str = '') -> bool:
    if not bool(getattr(order, 'stock_reserved', False)):
        return False
    items = db.query(OrderItem).filter(OrderItem.order_id == order.id).all()
    for item in items:
        if not item.product_id or int(item.qty or 0) <= 0:
            continue
        product = db.query(Product).filter(Product.id == item.product_id).first()
        if not product:
            continue
        product.stock_qty = int(product.stock_qty or 0) + int(item.qty or 0)
    order.stock_reserved = False
    if reason:
        _append_seller_remark(order, reason)
    return True


def mark_order_paid_state(db: Session, order: Order, paid_at: datetime | None = None) -> OrderFulfillment | None:
    now = paid_at or datetime.utcnow()
    order.pay_status = 'paid'
    if (order.order_status or 'created') in {'created', 'pending', '', 'expired'}:
        order.order_status = 'confirmed'
    order.paid_at = now
    fulfillment = ensure_order_fulfillment(db, order)
    if fulfillment:
        current = (fulfillment.fulfillment_status or '').strip()
        if current in {'', 'unassigned', 'cancelled'}:
            fulfillment.fulfillment_status = 'assigned'
        if not fulfillment.assigned_at:
            fulfillment.assigned_at = order.created_at or now
    return fulfillment


def mark_order_pushed_state(db: Session, order: Order, supplier: Supplier | None = None) -> OrderFulfillment | None:
    fulfillment = get_latest_fulfillment(db, order.id)
    if fulfillment is None and supplier is not None:
        fulfillment = upsert_order_fulfillment(db, order, supplier, status='assigned', note='')
    if fulfillment:
        fulfillment.fulfillment_status = 'pushed'
        if not fulfillment.assigned_at:
            fulfillment.assigned_at = order.created_at or datetime.utcnow()
        fulfillment.sync_status = 'pending'
    return fulfillment


def mark_order_shipped_state(db: Session, order: Order, shipped_at: datetime | None = None) -> OrderFulfillment | None:
    now = shipped_at or datetime.utcnow()
    order.delivery_status = 'shipped'
    order.shipped_at = now
    if (order.order_status or '') not in {'completed', 'cancelled'}:
        order.order_status = 'confirmed'
    fulfillment = ensure_order_fulfillment(db, order)
    if fulfillment:
        fulfillment.fulfillment_status = 'shipped'
        fulfillment.shipped_at = now
        if not fulfillment.accepted_at:
            fulfillment.accepted_at = order.paid_at or order.created_at or now
    return fulfillment


def mark_order_signed_state(db: Session, order: Order, signed_at: datetime | None = None) -> OrderFulfillment | None:
    now = signed_at or datetime.utcnow()
    order.delivery_status = 'signed'
    order.order_status = 'completed'
    if not order.completed_at:
        order.completed_at = now
    fulfillment = ensure_order_fulfillment(db, order)
    if fulfillment:
        fulfillment.fulfillment_status = 'fulfilled'
        fulfillment.shipped_at = fulfillment.shipped_at or order.shipped_at or now
    return fulfillment


def mark_order_completed_state(db: Session, order: Order, completed_at: datetime | None = None) -> OrderFulfillment | None:
    now = completed_at or datetime.utcnow()
    order.order_status = 'completed'
    if not order.completed_at:
        order.completed_at = now
    if order.delivery_status == 'not_shipped' and order.shipped_at:
        order.delivery_status = 'shipped'
    fulfillment = ensure_order_fulfillment(db, order)
    if fulfillment:
        fulfillment.fulfillment_status = 'fulfilled'
        fulfillment.shipped_at = fulfillment.shipped_at or order.shipped_at or now
    return fulfillment


def mark_order_cancelled_state(db: Session, order: Order, reason: str = '') -> OrderFulfillment | None:
    order.order_status = 'cancelled'
    if order.pay_status != 'paid':
        order.pay_status = 'pending'
    if (order.delivery_status or '').strip() not in {'shipped', 'signed'}:
        release_order_stock(db, order, reason=reason)
    elif reason:
        _append_seller_remark(order, reason)
    fulfillment = get_latest_fulfillment(db, order.id)
    if fulfillment:
        fulfillment.fulfillment_status = 'cancelled'
        fulfillment.sync_status = fulfillment.sync_status or 'pending'
    return fulfillment


def mark_order_expired_state(db: Session, order: Order) -> OrderFulfillment | None:
    return mark_order_cancelled_state(db, order, reason='支付超时未完成，系统已自动关闭订单并释放库存')


def upsert_order_fulfillment(db: Session, order: Order, supplier: Supplier | None, status: str = 'assigned', note: str = ''):
    fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.order_id == order.id).order_by(OrderFulfillment.id.desc()).first()
    if supplier is None:
        order.supplier_code = ''
        if fulfillment:
            fulfillment.fulfillment_status = 'unassigned'
            fulfillment.sync_status = 'pending'
            fulfillment.sync_error = note or ''
        return None
    order.supplier_code = supplier.supplier_code
    if fulfillment is None:
        fulfillment = OrderFulfillment(order_id=order.id, supplier_id=supplier.id)
        db.add(fulfillment)
    fulfillment.supplier_id = supplier.id
    fulfillment.fulfillment_status = status
    fulfillment.assigned_at = datetime.utcnow()
    if status in {'assigned', 'unassigned'}:
        fulfillment.sync_status = 'pending'
    fulfillment.sync_error = note or ''
    return fulfillment


def _pick_default_sku_for_product(db: Session, product_id: int):
    rows = (
        db.query(ProductSku)
        .filter(ProductSku.product_id == product_id)
        .order_by(ProductSku.sort_order.asc(), ProductSku.id.asc())
        .all()
    )
    if not rows:
        return None
    active = [x for x in rows if bool(x.is_active)]
    return (active or rows)[0]


def _sync_product_mirror_from_skus(db: Session, product: Product) -> None:
    default_sku = _pick_default_sku_for_product(db, product.id)
    if not default_sku:
        return
    product.sku_code = default_sku.sku_code or ""
    product.price_cny = default_sku.price_cny
    product.original_price_cny = default_sku.original_price_cny
    product.stock_qty = int(default_sku.stock_qty or 0)
    product.weight_gram = int(default_sku.weight_gram or 0)
    product.unit_text = default_sku.unit_text or "件"


def create_order(db: Session, payload):
    address = db.query(ShippingAddress).filter(ShippingAddress.id == payload.address_id).first()
    if not address:
        raise ValueError('收货地址不存在')
    if str(address.telegram_user_id or '') != str(payload.telegram_user_id or ''):
        raise ValueError('收货地址不属于当前用户')

    items_payload = []
    goods_amount = Decimal('0')
    product_ids = []

    for item in payload.items:
        qty = int(item.qty or 0)
        if qty <= 0:
            raise ValueError('商品数量必须大于 0')

        product = db.query(Product).filter(Product.id == item.product_id).first()
        if not product or not product.is_active:
            raise ValueError(f'商品不可下单：{item.product_id}')

        sku = None
        sku_id = int(getattr(item, 'sku_id', 0) or 0)
        sku_code_snapshot = product.sku_code or ""
        unit_price = Decimal(product.price_cny)
        cover_image = product.cover_image or ""

        if sku_id > 0:
            sku = (
                db.query(ProductSku)
                .filter(ProductSku.id == sku_id, ProductSku.product_id == product.id)
                .first()
            )
            if not sku or not bool(sku.is_active):
                raise ValueError('所选规格不存在或已停用')
            current_stock = int(sku.stock_qty or 0)
            if current_stock < qty:
                raise ValueError(f'规格库存不足：{product.name}')
            unit_price = Decimal(sku.price_cny)
            sku_code_snapshot = (sku.sku_code or sku.sku_name or '').strip()
            if sku.cover_image:
                cover_image = sku.cover_image
        else:
            current_stock = int(product.stock_qty or 0)
            if current_stock < qty:
                raise ValueError(f'商品库存不足：{product.name}')

        subtotal = unit_price * qty
        goods_amount += subtotal
        items_payload.append({
            'product': product,
            'sku': sku,
            'qty': qty,
            'unit_price': unit_price,
            'subtotal': subtotal,
            'sku_code_snapshot': sku_code_snapshot,
            'cover_image': cover_image,
        })
        product_ids.append(product.id)

    order = Order(
        order_no=f"GS{datetime.utcnow():%Y%m%d%H%M%S}{uuid.uuid4().hex[:6].upper()}",
        bot_code=payload.bot_code,
        telegram_user_id=payload.telegram_user_id,
        customer_name=address.receiver_name,
        customer_phone=address.receiver_phone,
        province=address.province,
        city=address.city,
        district=address.district,
        address_detail=address.address_detail,
        postal_code=address.postal_code,
        goods_amount=goods_amount,
        shipping_fee=Decimal('0'),
        discount_amount=Decimal('0'),
        payable_amount=goods_amount,
        buyer_remark=payload.buyer_remark,
        order_status='created',
        pay_status='pending',
        delivery_status='not_shipped',
        stock_reserved=True,
    )
    db.add(order)
    db.flush()

    for row in items_payload:
        product = row['product']
        sku = row['sku']
        qty = row['qty']
        subtotal = row['subtotal']
        unit_price = row['unit_price']

        if sku is not None:
            sku.stock_qty = int(sku.stock_qty or 0) - qty
            _sync_product_mirror_from_skus(db, product)
        else:
            product.stock_qty = int(product.stock_qty or 0) - qty

        db.add(OrderItem(
            order_id=order.id,
            product_id=product.id,
            product_name=product.name,
            sku_code=row['sku_code_snapshot'],
            cover_image=row['cover_image'],
            qty=qty,
            unit_price=unit_price,
            subtotal=subtotal,
        ))

    supplier, reason = resolve_order_supplier(db, product_ids)
    upsert_order_fulfillment(db, order, supplier, status='assigned' if supplier else 'unassigned', note=reason)
    if reason:
        _append_seller_remark(order, f'供应链路由：{reason}')

    db.commit()
    db.refresh(order)
    return order
