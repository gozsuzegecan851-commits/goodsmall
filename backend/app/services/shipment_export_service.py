from __future__ import annotations

import io
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

import openpyxl
from sqlalchemy.orm import Session

from ..models import Order, OrderItem, Shipment, Supplier, ProductSupplierMap


def _normalize_biz_date(biz_date: str | None, mode: str) -> str:
    if biz_date:
        return biz_date
    base = datetime.utcnow().date()
    if mode == "shipped":
        base = base - timedelta(days=1)
    return base.isoformat()


def _items_rows(db: Session, order_id: int):
    return db.query(OrderItem).filter(OrderItem.order_id == order_id).order_by(OrderItem.id.asc()).all()


def _items_text(db: Session, order_id: int) -> str:
    rows = _items_rows(db, order_id)
    if not rows:
        return ""
    return "；".join(f"{r.product_name} x{r.qty}" for r in rows)


def _supplier_sku_text(db: Session, order: Order, supplier: Supplier | None) -> str:
    if not supplier:
        return ''
    parts = []
    for row in _items_rows(db, order.id):
        if not row.product_id:
            continue
        m = db.query(ProductSupplierMap).filter(
            ProductSupplierMap.product_id == row.product_id,
            ProductSupplierMap.supplier_id == supplier.id,
            ProductSupplierMap.is_active == True,
        ).order_by(ProductSupplierMap.is_default.desc(), ProductSupplierMap.priority.asc(), ProductSupplierMap.id.asc()).first()
        sku = m.supplier_sku if m else ''
        if sku:
            parts.append(f"{sku} x{row.qty}")
    return '；'.join(parts)


def _resolve_supplier(db: Session, supplier_code: str | None) -> Supplier | None:
    if not supplier_code:
        return None
    return db.query(Supplier).filter(Supplier.supplier_code == supplier_code).first()


def _resolve_template_type(db: Session, supplier_code: str | None) -> str:
    supplier = _resolve_supplier(db, supplier_code)
    template_type = (supplier.template_type or 'standard') if supplier else 'standard'
    if template_type == 'standard' and supplier_code:
        code = supplier_code.strip().upper()
        if code == 'A':
            template_type = 'supplier_a'
        elif code == 'B':
            template_type = 'supplier_b'
    return template_type


def _headers_for_template(template_type: str) -> list[str]:
    if template_type == 'supplier_a':
        return [
            '平台订单号', '支付时间', '收件人', '手机号', '省', '市', '区', '详细地址', '供应链SKU', '商品明细', '数量说明', '应付金额', '买家备注', '卖家备注'
        ]
    if template_type == 'supplier_b':
        return [
            '客户单号', '收件人', '联系电话', '完整地址', '商品编码清单', '商品明细', '件数', '金额', '快递公司', '快递编码', '快递单号', '发货时间', '供应链'
        ]
    return [
        '订单号', '支付时间', '发货时间', '收件人', '手机号', '省', '市', '区', '详细地址', '邮编',
        '商品明细', '应付金额', '支付方式', '供应链', '快递公司', '快递编码', '快递单号', '买家备注', '卖家备注'
    ]


def _row_for_template(db: Session, order: Order, template_type: str, supplier: Supplier | None):
    items_text = _items_text(db, order.id)
    supplier_sku_text = _supplier_sku_text(db, order, supplier)
    if template_type == 'supplier_a':
        return [
            order.order_no,
            order.paid_at.strftime('%Y-%m-%d %H:%M:%S') if order.paid_at else '',
            order.customer_name or '',
            order.customer_phone or '',
            order.province or '',
            order.city or '',
            order.district or '',
            order.address_detail or '',
            supplier_sku_text,
            items_text,
            supplier_sku_text or items_text,
            str(order.payable_amount or Decimal('0')),
            order.buyer_remark or '',
            order.seller_remark or '',
        ]
    if template_type == 'supplier_b':
        item_rows = _items_rows(db, order.id)
        total_qty = sum(int(r.qty or 0) for r in item_rows)
        return [
            order.order_no,
            order.customer_name or '',
            order.customer_phone or '',
            ' '.join([x for x in [order.province, order.city, order.district, order.address_detail] if x]),
            supplier_sku_text,
            items_text,
            total_qty,
            str(order.payable_amount or Decimal('0')),
            order.courier_company or '',
            order.courier_code or '',
            order.tracking_no or '',
            order.shipped_at.strftime('%Y-%m-%d %H:%M:%S') if order.shipped_at else '',
            order.supplier_code or '',
        ]
    return [
        order.order_no,
        order.paid_at.strftime('%Y-%m-%d %H:%M:%S') if order.paid_at else '',
        order.shipped_at.strftime('%Y-%m-%d %H:%M:%S') if order.shipped_at else '',
        order.customer_name or '',
        order.customer_phone or '',
        order.province or '',
        order.city or '',
        order.district or '',
        order.address_detail or '',
        order.postal_code or '',
        items_text,
        str(order.payable_amount or Decimal('0')),
        order.payment_method or '',
        order.supplier_code or '',
        order.courier_company or '',
        order.courier_code or '',
        order.tracking_no or '',
        order.buyer_remark or '',
        order.seller_remark or '',
    ]


def _widths_for_headers(headers: list[str]) -> dict[int, int]:
    widths = {}
    for idx, h in enumerate(headers, start=1):
        widths[idx] = 18
        if '地址' in h:
            widths[idx] = 28
        elif '商品' in h or 'SKU' in h:
            widths[idx] = 26
        elif '备注' in h or '轨迹' in h:
            widths[idx] = 24
        elif '时间' in h:
            widths[idx] = 20
        elif '订单号' in h or '单号' in h:
            widths[idx] = 22
    return widths


def build_shipments_workbook(db: Session, mode: Literal['pending', 'shipped'], supplier_code: str | None = None, biz_date: str | None = None, sample_only: bool = False):
    biz_date = _normalize_biz_date(biz_date, mode)
    supplier = _resolve_supplier(db, supplier_code)
    template_type = _resolve_template_type(db, supplier_code)
    q = db.query(Order)
    if mode == 'pending':
        q = q.filter(Order.pay_status == 'paid', Order.delivery_status == 'not_shipped')
    else:
        q = q.filter(Order.delivery_status.in_(['shipped', 'signed']))
    if supplier_code:
        q = q.filter(Order.supplier_code == supplier_code)
    if mode == 'shipped' and biz_date:
        q = q.filter(Order.shipped_at.isnot(None))
        q = q.filter(Order.shipped_at >= datetime.fromisoformat(biz_date + 'T00:00:00'))
        q = q.filter(Order.shipped_at < datetime.fromisoformat(biz_date + 'T23:59:59.999999') + timedelta(microseconds=1))

    rows = [] if sample_only else q.order_by(Order.paid_at.desc().nullslast(), Order.id.desc()).all()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = '待发货清单' if mode == 'pending' else '已发货清单'
    headers = _headers_for_template(template_type)
    ws.append(headers)
    for order in rows:
        ws.append(_row_for_template(db, order, template_type, supplier))

    widths = _widths_for_headers(headers)
    for idx, width in widths.items():
        ws.column_dimensions[openpyxl.utils.get_column_letter(idx)].width = width

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f"shipment_{mode}_{supplier_code or 'all'}_{template_type}_{biz_date}.xlsx"
    return filename, buf
