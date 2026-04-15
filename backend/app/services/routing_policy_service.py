from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from ..models import OrderItem, Product, ProductRoutePolicy, ProductSupplierMap, Supplier

ROUTE_STATUS_WEIGHT = {'normal': 0, 'watch': 1, 'degraded': 2, 'blocked': 3}
LIST_TAG_WEIGHT = {'whitelist': -1000, 'neutral': 0, 'blacklist': 100000}


def _loads_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return [int(x) for x in data if str(x).strip()]
    except Exception:
        return []


def _dumps_ids(ids: list[int] | None) -> str:
    vals = []
    for x in ids or []:
        try:
            vals.append(int(x))
        except Exception:
            continue
    return json.dumps(vals, ensure_ascii=False)


def get_product_policy(db: Session, product_id: int) -> ProductRoutePolicy | None:
    return db.query(ProductRoutePolicy).filter(ProductRoutePolicy.product_id == product_id).first()


def save_product_policy(db: Session, *, payload) -> ProductRoutePolicy:
    item = db.query(ProductRoutePolicy).filter(ProductRoutePolicy.id == payload.id).first() if payload.id else get_product_policy(db, payload.product_id)
    if item is None:
        item = ProductRoutePolicy(product_id=payload.product_id)
        db.add(item)
    item.product_id = payload.product_id
    item.policy_mode = (payload.policy_mode or 'auto').strip() or 'auto'
    item.preferred_supplier_id = payload.preferred_supplier_id
    item.allow_supplier_ids_json = _dumps_ids(payload.allow_supplier_ids)
    item.deny_supplier_ids_json = _dumps_ids(payload.deny_supplier_ids)
    item.fallback_supplier_ids_json = _dumps_ids(getattr(payload, 'fallback_supplier_ids', None) or [])
    item.note = payload.note or ''
    item.is_active = bool(payload.is_active)
    db.flush()
    return item


def product_policy_to_dict(item: ProductRoutePolicy | None, db: Session) -> dict[str, Any]:
    if item is None:
        return {}
    product = db.query(Product).filter(Product.id == item.product_id).first()
    allow_ids = _loads_ids(item.allow_supplier_ids_json)
    deny_ids = _loads_ids(item.deny_supplier_ids_json)
    fallback_ids = _loads_ids(getattr(item, 'fallback_supplier_ids_json', '[]'))
    lookups = list(set(allow_ids + deny_ids + fallback_ids + ([item.preferred_supplier_id] if item.preferred_supplier_id else [])))
    suppliers = {s.id: s for s in db.query(Supplier).filter(Supplier.id.in_(lookups)).all()} if lookups else {}
    return {
        'id': item.id,
        'product_id': item.product_id,
        'product_name': product.name if product else '',
        'policy_mode': item.policy_mode or 'auto',
        'preferred_supplier_id': item.preferred_supplier_id,
        'preferred_supplier_code': suppliers.get(item.preferred_supplier_id).supplier_code if item.preferred_supplier_id and suppliers.get(item.preferred_supplier_id) else '',
        'preferred_supplier_name': suppliers.get(item.preferred_supplier_id).supplier_name if item.preferred_supplier_id and suppliers.get(item.preferred_supplier_id) else '',
        'allow_supplier_ids': allow_ids,
        'allow_supplier_codes': [suppliers[x].supplier_code for x in allow_ids if x in suppliers],
        'deny_supplier_ids': deny_ids,
        'deny_supplier_codes': [suppliers[x].supplier_code for x in deny_ids if x in suppliers],
        'fallback_supplier_ids': fallback_ids,
        'fallback_supplier_codes': [suppliers[x].supplier_code for x in fallback_ids if x in suppliers],
        'note': item.note or '',
        'is_active': bool(item.is_active),
        'updated_at': item.updated_at.isoformat() if item.updated_at else '',
    }


def _global_allowed_supplier(supplier: Supplier) -> tuple[bool, str]:
    tag = (getattr(supplier, 'route_list_tag', 'neutral') or 'neutral').strip()
    if tag == 'blacklist':
        return False, f'{supplier.supplier_code} 命中全局黑名单'
    return True, ''


def _make_candidate(mapping: ProductSupplierMap, supplier: Supplier, *, rank: int, preferred_supplier_id: int | None, allow_ids: set[int], note_parts: list[str], source: str = 'primary') -> dict[str, Any]:
    list_tag = (getattr(supplier, 'route_list_tag', 'neutral') or 'neutral').strip()
    route_status = (getattr(supplier, 'route_status', 'normal') or 'normal').strip()
    score = LIST_TAG_WEIGHT.get(list_tag, 0) + ROUTE_STATUS_WEIGHT.get(route_status, 0) * 100 + (0 if mapping.is_default else 20) + int(mapping.priority or 100) + rank
    local_notes = list(note_parts)
    if list_tag == 'whitelist':
        score -= 40
        local_notes.append('全局白名单优先')
    if preferred_supplier_id and supplier.id == preferred_supplier_id:
        score -= 60
        local_notes.append('商品优先供应链')
    if allow_ids and supplier.id in allow_ids:
        score -= 20
        local_notes.append('命中商品白名单')
    if source == 'fallback':
        score += 80
        local_notes.append('商品兜底候选')
    return {
        'supplier_id': supplier.id,
        'supplier_code': supplier.supplier_code or '',
        'supplier_name': supplier.supplier_name or '',
        'supplier': supplier,
        'route_status': route_status,
        'route_score': int(getattr(supplier, 'route_score', 100) or 0),
        'list_tag': list_tag,
        'priority': int(mapping.priority or 100),
        'is_default': bool(mapping.is_default),
        'score': score,
        'note': '；'.join(local_notes),
        'source': source,
    }


def get_ranked_candidates_for_product(db: Session, product_id: int, *, exclude_supplier_id: int | None = None) -> dict[str, Any]:
    product = db.query(Product).filter(Product.id == product_id).first()
    policy = get_product_policy(db, product_id)
    if policy and bool(policy.is_active) and (policy.policy_mode or 'auto') == 'manual_review':
        return {'product_id': product_id, 'product_name': product.name if product else '', 'policy_mode': 'manual_review', 'candidates': [], 'note': '商品策略要求人工审核', 'manual_review': True, 'used_fallback': False}

    rows = (
        db.query(ProductSupplierMap, Supplier)
        .join(Supplier, Supplier.id == ProductSupplierMap.supplier_id)
        .filter(ProductSupplierMap.product_id == product_id)
        .filter(ProductSupplierMap.is_active == True, Supplier.is_active == True)
        .order_by(ProductSupplierMap.is_default.desc(), ProductSupplierMap.priority.asc(), ProductSupplierMap.id.asc())
        .all()
    )

    allow_ids = set(_loads_ids(policy.allow_supplier_ids_json) if policy and policy.is_active else [])
    deny_ids = set(_loads_ids(policy.deny_supplier_ids_json) if policy and policy.is_active else [])
    fallback_ids = set(_loads_ids(getattr(policy, 'fallback_supplier_ids_json', '[]')) if policy and policy.is_active else [])
    preferred_supplier_id = policy.preferred_supplier_id if policy and policy.is_active else None
    policy_mode = (policy.policy_mode or 'auto').strip() if policy and policy.is_active else 'auto'

    primary_candidates, fallback_candidates, deny_reasons = [], [], []
    for rank, (mapping, supplier) in enumerate(rows):
        if exclude_supplier_id and supplier.id == exclude_supplier_id:
            continue
        allowed, blocked_reason = _global_allowed_supplier(supplier)
        if not allowed:
            deny_reasons.append(blocked_reason)
            continue
        if policy_mode == 'force' and preferred_supplier_id and supplier.id != preferred_supplier_id and supplier.id not in fallback_ids:
            continue
        if policy_mode == 'whitelist_only' and allow_ids and supplier.id not in allow_ids and supplier.id not in fallback_ids:
            continue
        if policy_mode in {'blacklist_only', 'preferred', 'auto', 'force'} and supplier.id in deny_ids:
            continue
        if policy_mode == 'preferred' and allow_ids and supplier.id not in allow_ids and supplier.id != preferred_supplier_id and supplier.id not in fallback_ids:
            continue
        note_parts = []
        if policy_mode == 'force' and preferred_supplier_id and supplier.id == preferred_supplier_id:
            note_parts.append('商品强制路由')
        source = 'fallback' if supplier.id in fallback_ids and (supplier.id != preferred_supplier_id and supplier.id not in allow_ids) else 'primary'
        candidate = _make_candidate(mapping, supplier, rank=rank, preferred_supplier_id=preferred_supplier_id, allow_ids=allow_ids, note_parts=note_parts, source=source)
        (fallback_candidates if source == 'fallback' else primary_candidates).append(candidate)

    used_fallback = False
    candidates = sorted(primary_candidates, key=lambda x: (x['score'], x['supplier_code']))
    if not candidates and fallback_candidates:
        used_fallback = True
        candidates = sorted(fallback_candidates, key=lambda x: (x['score'], x['supplier_code']))

    note = '自动路由'
    if policy and policy.is_active:
        note = f'策略 {policy_mode}'
        if policy.note:
            note += f'：{policy.note}'
    if used_fallback:
        note = (note + '；使用商品级兜底路由').strip('；')
    if not candidates and deny_reasons:
        note = '；'.join(sorted(set(deny_reasons)))
    return {'product_id': product_id, 'product_name': product.name if product else '', 'policy_mode': policy_mode, 'candidates': candidates, 'note': note, 'manual_review': False, 'used_fallback': used_fallback, 'fallback_supplier_ids': list(fallback_ids)}


def build_route_decision_for_products(db: Session, product_ids: list[int], *, exclude_supplier_id: int | None = None) -> dict[str, Any]:
    if not product_ids:
        return {'ok': False, 'message': '订单无商品，无法指派供应链', 'previews': [], 'common_candidates': []}
    previews = [get_ranked_candidates_for_product(db, pid, exclude_supplier_id=exclude_supplier_id) for pid in product_ids]
    reasons = []
    for preview in previews:
        if preview.get('manual_review'):
            return {'ok': False, 'message': f"商品 {preview.get('product_name') or preview.get('product_id')} 策略要求人工审核", 'previews': previews, 'common_candidates': [], 'manual_review': True}
        if not preview.get('candidates'):
            return {'ok': False, 'message': f"商品 {preview.get('product_name') or preview.get('product_id')} 没有可用供应链：{preview.get('note') or '无可用候选'}", 'previews': previews, 'common_candidates': [], 'manual_review': True}
        if preview.get('used_fallback'):
            reasons.append(f"{preview.get('product_name') or preview.get('product_id')} 使用兜底路由")
    common_ids = set(x['supplier_id'] for x in previews[0]['candidates'])
    for preview in previews[1:]:
        common_ids &= {x['supplier_id'] for x in preview['candidates']}
    if not common_ids:
        per_product = [f"{preview.get('product_name') or preview.get('product_id')}：" + ', '.join(x.get('supplier_code') or '' for x in preview.get('candidates', [])[:3]) for preview in previews]
        return {'ok': False, 'message': '订单商品策略冲突，没有共同可用供应链，需人工审核', 'previews': previews, 'common_candidates': [], 'manual_review': True, 'reason_lines': per_product}
    scored = []
    for sid in common_ids:
        total = 0
        supplier_obj = None
        notes = []
        for preview in previews:
            row = next(x for x in preview['candidates'] if x['supplier_id'] == sid)
            total += int(row['score'])
            supplier_obj = row['supplier']
            if row.get('source') == 'fallback':
                notes.append(f"{preview.get('product_name') or preview.get('product_id')} 使用兜底")
        if supplier_obj is not None:
            scored.append((total, supplier_obj.supplier_code or '', supplier_obj, notes))
    scored.sort(key=lambda x: (x[0], x[1]))
    common_candidates = [{'supplier_id': supplier_obj.id, 'supplier_code': supplier_code, 'supplier_name': supplier_obj.supplier_name or '', 'route_status': supplier_obj.route_status or 'normal', 'route_score': int(getattr(supplier_obj, 'route_score', 100) or 0), 'score': total, 'note': '；'.join(notes)} for total, supplier_code, supplier_obj, notes in scored[:5]]
    chosen = scored[0][2]
    chosen_notes = list(dict.fromkeys(reasons + scored[0][3]))
    message = f'按商品级路由策略自动指派：{chosen.supplier_code}'
    if chosen_notes:
        message += '；' + '；'.join(chosen_notes[:4])
    return {'ok': True, 'supplier': chosen, 'message': message, 'previews': previews, 'common_candidates': common_candidates, 'manual_review': False}


def resolve_supplier_for_products(db: Session, product_ids: list[int], *, exclude_supplier_id: int | None = None) -> tuple[Supplier | None, str]:
    decision = build_route_decision_for_products(db, product_ids, exclude_supplier_id=exclude_supplier_id)
    return decision.get('supplier'), decision.get('message') or '未找到供应链'


def build_route_decision_for_order(db: Session, order_id: int, *, exclude_supplier_id: int | None = None) -> dict[str, Any]:
    items = db.query(OrderItem).filter(OrderItem.order_id == order_id).all()
    return build_route_decision_for_products(db, [x.product_id for x in items if x.product_id], exclude_supplier_id=exclude_supplier_id)


def routing_policy_summary(db: Session) -> dict[str, Any]:
    suppliers = db.query(Supplier).all()
    policies = db.query(ProductRoutePolicy).order_by(ProductRoutePolicy.product_id.asc()).all()
    mode_counts, fallback_enabled = {}, 0
    for item in policies:
        key = (item.policy_mode or 'auto').strip() or 'auto'
        mode_counts[key] = mode_counts.get(key, 0) + 1
        if _loads_ids(getattr(item, 'fallback_supplier_ids_json', '[]')):
            fallback_enabled += 1
    return {'supplier_summary': {'whitelist': sum(1 for x in suppliers if (getattr(x, 'route_list_tag', 'neutral') or 'neutral') == 'whitelist'), 'blacklist': sum(1 for x in suppliers if (getattr(x, 'route_list_tag', 'neutral') or 'neutral') == 'blacklist'), 'neutral': sum(1 for x in suppliers if (getattr(x, 'route_list_tag', 'neutral') or 'neutral') not in {'whitelist', 'blacklist'})}, 'policy_summary': mode_counts, 'policy_count': len(policies), 'fallback_policy_count': fallback_enabled}
