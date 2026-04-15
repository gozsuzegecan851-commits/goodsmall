from __future__ import annotations

from datetime import datetime
from typing import Any
import json

from sqlalchemy.orm import Session

from ..config import settings
from ..models import Order, OrderFulfillment, Shipment, Supplier, SupplierRouteLog
from .routing_policy_service import build_route_decision_for_order, routing_policy_summary


def _now() -> datetime:
    return datetime.utcnow()


def _supplier_health_status(score: int) -> str:
    if score <= int(settings.route_blocked_threshold or 40):
        return "blocked"
    if score <= int(settings.route_degraded_threshold or 70):
        return "degraded"
    if score <= 85:
        return "watch"
    return "normal"


def _order_age_hours(order: Order) -> float:
    ref = order.paid_at or order.created_at or _now()
    return max(0.0, (_now() - ref).total_seconds() / 3600.0)


def _eligible_fulfillments_for_supplier(db: Session, supplier_id: int):
    return db.query(OrderFulfillment, Order).join(Order, Order.id == OrderFulfillment.order_id).filter(OrderFulfillment.supplier_id == supplier_id).filter(Order.delivery_status == 'not_shipped').filter(Order.pay_status.in_(['paid', 'pending', 'created', ''])).order_by(Order.id.asc()).all()


def compute_supplier_route_health(db: Session, supplier: Supplier) -> dict[str, Any]:
    pending_12 = pending_24 = sync_errors = logistics_errors = logistics_returns = manual_review = 0
    for fulfillment, order in _eligible_fulfillments_for_supplier(db, supplier.id):
        age_hours = _order_age_hours(order)
        if age_hours >= 24:
            pending_24 += 1
        elif age_hours >= 12:
            pending_12 += 1
        if (fulfillment.sync_status or '') == 'error':
            sync_errors += 1
        if bool(getattr(fulfillment, 'manual_review_required', False)):
            manual_review += 1
    shipment_rows = db.query(Shipment, Order).join(Order, Order.id == Shipment.order_id).filter(Order.supplier_code == supplier.supplier_code).all()
    for shipment, _order in shipment_rows:
        if (shipment.sync_status or '') == 'error':
            logistics_errors += 1
        if (shipment.ship_status or '') in {'returned', 'error', 'exception'}:
            logistics_returns += 1
    score = max(0, min(100, 100 - pending_12 * 8 - pending_24 * 15 - sync_errors * 10 - logistics_errors * 8 - logistics_returns * 15 - manual_review * 6))
    status = _supplier_health_status(score)
    reasons = []
    if pending_24: reasons.append(f"超24小时未发货 {pending_24}")
    if pending_12: reasons.append(f"超12小时未发货 {pending_12}")
    if sync_errors: reasons.append(f"推单失败 {sync_errors}")
    if logistics_errors: reasons.append(f"物流同步失败 {logistics_errors}")
    if logistics_returns: reasons.append(f"退回/异常件 {logistics_returns}")
    if manual_review: reasons.append(f"人工审核 {manual_review}")
    supplier.route_score = score
    supplier.route_status = status
    supplier.route_rule_note = '；'.join(reasons) if reasons else '状态正常'
    supplier.route_checked_at = _now()
    return {'supplier_code': supplier.supplier_code, 'supplier_name': supplier.supplier_name, 'route_status': status, 'route_score': score, 'pending_12': pending_12, 'pending_24': pending_24, 'sync_errors': sync_errors, 'logistics_errors': logistics_errors, 'logistics_returns': logistics_returns, 'manual_review': manual_review, 'route_rule_note': supplier.route_rule_note or ''}


def recompute_all_supplier_health(db: Session) -> dict[str, Any]:
    rows = db.query(Supplier).order_by(Supplier.supplier_code.asc()).all()
    data = [compute_supplier_route_health(db, row) for row in rows]
    db.flush()
    return {'ok': True, 'count': len(data), 'rows': data, 'checked_at': _now().isoformat()}


def resolve_backup_supplier(db: Session, order: Order, exclude_supplier_id: int | None = None):
    decision = build_route_decision_for_order(db, order.id, exclude_supplier_id=exclude_supplier_id)
    return decision.get('supplier'), decision.get('message') or '未找到备用供应链', decision


def _add_route_log(db: Session, *, supplier_code: str, order_id: int | None, fulfillment_id: int | None, action_type: str, from_supplier_code: str = '', to_supplier_code: str = '', reason: str = '') -> None:
    db.add(SupplierRouteLog(supplier_code=supplier_code or '', order_id=order_id, fulfillment_id=fulfillment_id, action_type=action_type, from_supplier_code=from_supplier_code or '', to_supplier_code=to_supplier_code or '', reason=reason or ''))


def reroute_fulfillment(db: Session, fulfillment: OrderFulfillment, *, target_supplier_code: str = '', force: bool = False) -> dict[str, Any]:
    order = db.query(Order).filter(Order.id == fulfillment.order_id).first()
    if not order:
        return {'ok': False, 'message': '订单不存在'}
    current_supplier = db.query(Supplier).filter(Supplier.id == fulfillment.supplier_id).first() if fulfillment.supplier_id else None
    old_code = current_supplier.supplier_code if current_supplier else ''
    if order.delivery_status != 'not_shipped':
        return {'ok': False, 'message': '订单已发货，不能再切换供应链'}
    if current_supplier and (not force) and (current_supplier.route_status or 'normal') not in {'degraded', 'blocked'}:
        return {'ok': False, 'message': '当前供应链未进入降级/阻断状态，未执行切换'}
    target_supplier, reason, decision = None, '', {}
    if target_supplier_code:
        target_supplier = db.query(Supplier).filter(Supplier.supplier_code == target_supplier_code, Supplier.is_active == True).first()
        if not target_supplier:
            return {'ok': False, 'message': '目标供应链不存在或已停用'}
        reason = f'后台指定切换到 {target_supplier.supplier_code}'
    else:
        target_supplier, reason, decision = resolve_backup_supplier(db, order, exclude_supplier_id=current_supplier.id if current_supplier else None)
    if target_supplier and current_supplier and target_supplier.id == current_supplier.id:
        target_supplier = None
        reason = '备用供应链仍然是当前供应链，无法切换'
    fulfillment.original_supplier_id = fulfillment.original_supplier_id or (current_supplier.id if current_supplier else None)
    fulfillment.route_updated_at = _now()
    if target_supplier and (force or (target_supplier.route_status or 'normal') in {'normal', 'watch'}):
        fulfillment.supplier_id = target_supplier.id
        fulfillment.fulfillment_status = 'assigned'
        fulfillment.sync_status = 'pending'
        fulfillment.route_action = 'switched' if old_code else 'assigned'
        fulfillment.route_reason = reason
        fulfillment.manual_review_required = False
        if fulfillment.manual_review_status in {'open', 'blocked'}:
            fulfillment.manual_review_status = 'resolved'
            fulfillment.manual_review_resolved_at = _now()
        else:
            fulfillment.manual_review_status = 'none'
        fulfillment.sync_error = reason
        fulfillment.fallback_summary_json = json.dumps({'previews': decision.get('previews') or [], 'common_candidates': decision.get('common_candidates') or [], 'manual_review': bool(decision.get('manual_review')), 'reason_lines': decision.get('reason_lines') or []}, ensure_ascii=False)
        order.supplier_code = target_supplier.supplier_code or ''
        _add_route_log(db, supplier_code=old_code or (target_supplier.supplier_code or ''), order_id=order.id, fulfillment_id=fulfillment.id, action_type='switched' if old_code else 'assigned', from_supplier_code=old_code, to_supplier_code=target_supplier.supplier_code or '', reason=reason)
        return {'ok': True, 'action': 'switched' if old_code else 'assigned', 'order_no': order.order_no, 'from_supplier_code': old_code, 'to_supplier_code': target_supplier.supplier_code or '', 'message': reason}
    fulfillment.fulfillment_status = 'manual_review'
    fulfillment.route_action = 'manual_review'
    fulfillment.route_reason = reason or '未找到健康备用供应链，已降级人工审核'
    fulfillment.manual_review_required = True
    fulfillment.manual_review_status = 'open'
    fulfillment.manual_review_opened_at = fulfillment.manual_review_opened_at or _now()
    fulfillment.sync_status = 'error'
    fulfillment.sync_error = fulfillment.route_reason
    if decision:
        fulfillment.fallback_summary_json = json.dumps({'previews': decision.get('previews') or [], 'common_candidates': decision.get('common_candidates') or [], 'manual_review': bool(decision.get('manual_review')), 'reason_lines': decision.get('reason_lines') or []}, ensure_ascii=False)
    _add_route_log(db, supplier_code=old_code, order_id=order.id, fulfillment_id=fulfillment.id, action_type='manual_review', from_supplier_code=old_code, to_supplier_code=target_supplier.supplier_code if target_supplier else '', reason=fulfillment.route_reason)
    return {'ok': True, 'action': 'manual_review', 'order_no': order.order_no, 'from_supplier_code': old_code, 'to_supplier_code': target_supplier.supplier_code if target_supplier else '', 'message': fulfillment.route_reason}


def run_auto_routing(db: Session, supplier_code: str = '', dry_run: bool = False, include_watch: bool = False) -> dict[str, Any]:
    health = recompute_all_supplier_health(db)
    suppliers = {x.supplier_code: x for x in db.query(Supplier).all()}
    affected, manual_review, switched = [], [], []
    q = db.query(OrderFulfillment).order_by(OrderFulfillment.id.asc())
    if supplier_code:
        supplier = suppliers.get(supplier_code)
        q = q.filter(OrderFulfillment.supplier_id == (supplier.id if supplier else -1))
    for fulfillment in q.all():
        supplier = db.query(Supplier).filter(Supplier.id == fulfillment.supplier_id).first() if fulfillment.supplier_id else None
        if supplier and not bool(getattr(supplier, 'auto_route_enabled', True)):
            continue
        if supplier is None:
            if not fulfillment.manual_review_required:
                continue
            result = reroute_fulfillment(db, fulfillment, force=True)
        else:
            status = (supplier.route_status or 'normal').strip()
            if status not in {'degraded', 'blocked'} and not (include_watch and status == 'watch'):
                continue
            order = db.query(Order).filter(Order.id == fulfillment.order_id).first()
            if not order or order.delivery_status != 'not_shipped':
                continue
            result = reroute_fulfillment(db, fulfillment, force=include_watch)
        if not result.get('ok'):
            continue
        affected.append(result)
        if result.get('action') in {'switched', 'assigned'}: switched.append(result)
        if result.get('action') == 'manual_review': manual_review.append(result)
        if dry_run:
            db.rollback(); recompute_all_supplier_health(db); break
    if not dry_run:
        db.flush()
    return {'ok': True, 'dry_run': dry_run, 'checked_suppliers': health.get('count', 0), 'affected_count': len(affected), 'switched_count': len(switched), 'manual_review_count': len(manual_review), 'rows': affected, 'checked_at': _now().isoformat()}


def routing_overview(db: Session) -> dict[str, Any]:
    rows = recompute_all_supplier_health(db).get('rows', [])
    logs = db.query(SupplierRouteLog).order_by(SupplierRouteLog.id.desc()).limit(30).all()
    policy = routing_policy_summary(db)
    manual_review_open = db.query(OrderFulfillment).filter(OrderFulfillment.manual_review_required == True, OrderFulfillment.manual_review_status.in_(['open', 'blocked'])).count()
    return {'ok': True, 'summary': {'normal': sum(1 for x in rows if x.get('route_status') == 'normal'), 'watch': sum(1 for x in rows if x.get('route_status') == 'watch'), 'degraded': sum(1 for x in rows if x.get('route_status') == 'degraded'), 'blocked': sum(1 for x in rows if x.get('route_status') == 'blocked'), 'manual_review_open': manual_review_open}, 'policy_summary': policy.get('policy_summary', {}), 'supplier_list_summary': policy.get('supplier_summary', {}), 'policy_count': policy.get('policy_count', 0), 'fallback_policy_count': policy.get('fallback_policy_count', 0), 'suppliers': rows, 'logs': [{'id': x.id, 'supplier_code': x.supplier_code or '', 'order_id': x.order_id, 'fulfillment_id': x.fulfillment_id, 'action_type': x.action_type or '', 'from_supplier_code': x.from_supplier_code or '', 'to_supplier_code': x.to_supplier_code or '', 'reason': x.reason or '', 'created_at': x.created_at.isoformat() if x.created_at else ''} for x in logs], 'checked_at': _now().isoformat()}
