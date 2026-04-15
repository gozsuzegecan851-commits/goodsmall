from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from ..models import Order, OrderFulfillment, OrderItem, Supplier
from .routing_policy_service import build_route_decision_for_order


def _safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _fulfillment_row(db: Session, fulfillment: OrderFulfillment) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == fulfillment.order_id).first()
    supplier = db.query(Supplier).filter(Supplier.id == fulfillment.supplier_id).first() if fulfillment.supplier_id else None
    return {
        'id': fulfillment.id,
        'order_id': fulfillment.order_id,
        'order_no': order.order_no if order else '',
        'supplier_id': fulfillment.supplier_id,
        'supplier_code': supplier.supplier_code if supplier else '',
        'supplier_name': supplier.supplier_name if supplier else '',
        'pay_status': order.pay_status if order else '',
        'delivery_status': order.delivery_status if order else '',
        'customer_name': order.customer_name if order else '',
        'created_at': order.created_at.isoformat() if order and order.created_at else '',
        'paid_at': order.paid_at.isoformat() if order and order.paid_at else '',
        'manual_review_required': bool(fulfillment.manual_review_required),
        'manual_review_status': fulfillment.manual_review_status or 'none',
        'manual_review_note': fulfillment.manual_review_note or '',
        'manual_review_owner': fulfillment.manual_review_owner or '',
        'manual_review_opened_at': fulfillment.manual_review_opened_at.isoformat() if fulfillment.manual_review_opened_at else '',
        'manual_review_resolved_at': fulfillment.manual_review_resolved_at.isoformat() if fulfillment.manual_review_resolved_at else '',
        'route_action': fulfillment.route_action or '',
        'route_reason': fulfillment.route_reason or '',
        'sync_status': fulfillment.sync_status or '',
    }


def build_manual_review_detail(db: Session, fulfillment: OrderFulfillment) -> dict[str, Any]:
    row = _fulfillment_row(db, fulfillment)
    order = db.query(Order).filter(Order.id == fulfillment.order_id).first()
    items = db.query(OrderItem).filter(OrderItem.order_id == fulfillment.order_id).order_by(OrderItem.id.asc()).all()
    decision = build_route_decision_for_order(db, fulfillment.order_id)
    cached = _safe_json(getattr(fulfillment, 'fallback_summary_json', '{}'))
    row['items'] = [{'product_id': item.product_id, 'product_name': item.product_name or '', 'sku_code': item.sku_code or '', 'qty': int(item.qty or 0), 'subtotal': str(item.subtotal or '')} for item in items]
    row['route_decision'] = {'message': decision.get('message') or '', 'manual_review': bool(decision.get('manual_review')), 'common_candidates': decision.get('common_candidates') or cached.get('common_candidates') or [], 'previews': decision.get('previews') or cached.get('previews') or [], 'reason_lines': decision.get('reason_lines') or cached.get('reason_lines') or []}
    row['seller_remark'] = order.seller_remark if order else ''
    return row


def list_manual_review_workbench(db: Session, status: str = 'open', supplier_code: str = '', limit: int = 200) -> dict[str, Any]:
    q = db.query(OrderFulfillment).filter(OrderFulfillment.manual_review_required == True)
    if status == 'open':
        q = q.filter(OrderFulfillment.manual_review_status.in_(['open', 'blocked']))
    elif status and status != 'all':
        q = q.filter(OrderFulfillment.manual_review_status == status)
    rows = q.order_by(OrderFulfillment.manual_review_opened_at.desc().nullslast(), OrderFulfillment.id.desc()).limit(max(1, min(int(limit or 200), 500))).all()
    data = []
    for item in rows:
        row = _fulfillment_row(db, item)
        if supplier_code and row.get('supplier_code') != supplier_code:
            continue
        data.append(row)
    return {'ok': True, 'count': len(data), 'status': status, 'rows': data, 'summary': {'open': db.query(OrderFulfillment).filter(OrderFulfillment.manual_review_required == True, OrderFulfillment.manual_review_status == 'open').count(), 'blocked': db.query(OrderFulfillment).filter(OrderFulfillment.manual_review_required == True, OrderFulfillment.manual_review_status == 'blocked').count(), 'resolved': db.query(OrderFulfillment).filter(OrderFulfillment.manual_review_required == True, OrderFulfillment.manual_review_status == 'resolved').count()}, 'checked_at': datetime.utcnow().isoformat()}


def assign_manual_review(db: Session, fulfillment: OrderFulfillment, *, target_supplier: Supplier, note: str = '', owner: str = '', force: bool = True) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == fulfillment.order_id).first()
    if not order:
        return {'ok': False, 'message': '订单不存在'}
    fulfillment.supplier_id = target_supplier.id
    fulfillment.fulfillment_status = 'assigned'
    fulfillment.route_action = 'manual_assign'
    fulfillment.route_reason = note or f'人工审核工作台手动指派到 {target_supplier.supplier_code}'
    fulfillment.route_updated_at = datetime.utcnow()
    fulfillment.manual_review_required = False
    fulfillment.manual_review_status = 'resolved'
    fulfillment.manual_review_owner = owner or fulfillment.manual_review_owner or ''
    fulfillment.manual_review_note = note or fulfillment.manual_review_note or ''
    fulfillment.manual_review_resolved_at = datetime.utcnow()
    fulfillment.sync_status = 'pending'
    fulfillment.sync_error = fulfillment.route_reason
    order.supplier_code = target_supplier.supplier_code or ''
    return {'ok': True, 'message': fulfillment.route_reason, 'supplier_code': target_supplier.supplier_code or ''}


def update_manual_review_state(fulfillment: OrderFulfillment, *, action: str, note: str = '', owner: str = '') -> dict[str, Any]:
    action = (action or 'resolved').strip() or 'resolved'
    if action not in {'resolved', 'blocked', 'reopen'}:
        return {'ok': False, 'message': 'action 仅支持 resolved / blocked / reopen'}
    fulfillment.manual_review_owner = owner or fulfillment.manual_review_owner or ''
    if note:
        fulfillment.manual_review_note = note
    if action == 'resolved':
        fulfillment.manual_review_status = 'resolved'
        fulfillment.manual_review_required = False
        fulfillment.manual_review_resolved_at = datetime.utcnow()
        fulfillment.route_action = 'manual_resolved'
        fulfillment.route_reason = note or fulfillment.route_reason or '人工审核已处理'
        fulfillment.sync_status = 'pending' if fulfillment.supplier_id else fulfillment.sync_status
    elif action == 'blocked':
        fulfillment.manual_review_status = 'blocked'
        fulfillment.manual_review_required = True
        fulfillment.manual_review_opened_at = fulfillment.manual_review_opened_at or datetime.utcnow()
        fulfillment.route_action = 'manual_blocked'
        fulfillment.route_reason = note or fulfillment.route_reason or '人工审核暂时阻塞'
        fulfillment.sync_status = 'error'
    else:
        fulfillment.manual_review_status = 'open'
        fulfillment.manual_review_required = True
        fulfillment.manual_review_opened_at = fulfillment.manual_review_opened_at or datetime.utcnow()
        fulfillment.manual_review_resolved_at = None
        fulfillment.route_action = 'manual_reopen'
        fulfillment.route_reason = note or fulfillment.route_reason or '重新打开人工审核'
        fulfillment.sync_status = 'error'
    fulfillment.route_updated_at = datetime.utcnow()
    fulfillment.sync_error = fulfillment.route_reason or ''
    return {'ok': True, 'message': fulfillment.route_reason or action}
