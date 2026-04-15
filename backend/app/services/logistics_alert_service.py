from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from ..models import LogisticsAlert, Order, OrderFulfillment, Shipment

ALERT_RULES = [
    {
        "alert_type": "not_shipped_timeout",
        "name": "未发货超时",
        "summary": "已支付后仍未发货的履约单",
        "thresholds": {"yellow_hours": 6, "orange_hours": 12, "red_hours": 24},
    },
    {
        "alert_type": "no_tracking_number",
        "name": "缺少快递单号",
        "summary": "已标记发货或已导入发货，但没有有效快递单号",
        "thresholds": {"red": 1},
    },
    {
        "alert_type": "no_first_trace",
        "name": "首轨迹超时",
        "summary": "发货后 24 小时仍无首条轨迹",
        "thresholds": {"yellow_hours": 24},
    },
    {
        "alert_type": "trace_stagnant",
        "name": "物流停更",
        "summary": "轨迹 48/72 小时未更新",
        "thresholds": {"orange_hours": 48, "red_hours": 72},
    },
    {
        "alert_type": "sync_fail",
        "name": "同步连续失败",
        "summary": "物流同步失败达到 3/6 次",
        "thresholds": {"orange_count": 3, "red_count": 6},
    },
    {
        "alert_type": "logistics_exception",
        "name": "物流异常",
        "summary": "退回、拒收、异常件、问题件等高危状态",
        "thresholds": {"red": 1},
    },
]

SEVERITY_RANK = {"": 0, "yellow": 1, "orange": 2, "red": 3}
EXCEPTION_KEYWORDS = ["退回", "拒收", "异常", "问题件", "problem", "exception", "reject", "returned"]


def _hours_between(start: datetime | None, end: datetime | None = None) -> int:
    if not start:
        return 0
    end = end or datetime.utcnow()
    return max(0, int((end - start).total_seconds() // 3600))


def _alert_age_hours(alert: LogisticsAlert, end: datetime | None = None) -> int:
    return _hours_between(alert.created_at, end)


def _get_open_alert(db: Session, alert_type: str, fulfillment_id: int | None = None, shipment_id: int | None = None) -> LogisticsAlert | None:
    q = db.query(LogisticsAlert).filter(LogisticsAlert.alert_type == alert_type, LogisticsAlert.is_resolved == False)
    if fulfillment_id is not None:
        q = q.filter(LogisticsAlert.fulfillment_id == fulfillment_id)
    else:
        q = q.filter(LogisticsAlert.fulfillment_id.is_(None))
    if shipment_id is not None:
        q = q.filter(LogisticsAlert.shipment_id == shipment_id)
    else:
        q = q.filter(LogisticsAlert.shipment_id.is_(None))
    return q.order_by(LogisticsAlert.id.desc()).first()


def _resolve_open_alert(db: Session, alert_type: str, fulfillment_id: int | None = None, shipment_id: int | None = None) -> None:
    alert = _get_open_alert(db, alert_type, fulfillment_id=fulfillment_id, shipment_id=shipment_id)
    if alert:
        alert.is_resolved = True
        alert.resolved_at = datetime.utcnow()


def _upsert_alert(
    db: Session,
    *,
    alert_type: str,
    level: str,
    text: str,
    supplier_code: str,
    fulfillment_id: int | None = None,
    shipment_id: int | None = None,
) -> LogisticsAlert:
    alert = _get_open_alert(db, alert_type, fulfillment_id=fulfillment_id, shipment_id=shipment_id)
    if alert:
        alert.alert_level = level
        alert.alert_text = text
        alert.supplier_code = supplier_code or alert.supplier_code or ""
        return alert
    alert = LogisticsAlert(
        supplier_code=supplier_code or "",
        fulfillment_id=fulfillment_id,
        shipment_id=shipment_id,
        alert_type=alert_type,
        alert_level=level,
        alert_text=text,
    )
    db.add(alert)
    db.flush()
    return alert


def refresh_fulfillment_warning_summary(db: Session, fulfillment: OrderFulfillment) -> None:
    alerts = (
        db.query(LogisticsAlert)
        .filter(LogisticsAlert.fulfillment_id == fulfillment.id, LogisticsAlert.is_resolved == False)
        .order_by(LogisticsAlert.created_at.desc(), LogisticsAlert.id.desc())
        .all()
    )
    top = None
    for alert in alerts:
        if top is None or SEVERITY_RANK.get(alert.alert_level or "", 0) > SEVERITY_RANK.get(top.alert_level or "", 0):
            top = alert
    if top:
        fulfillment.warning_level = top.alert_level or ""
        fulfillment.warning_type = top.alert_type or ""
        fulfillment.warning_text = top.alert_text or ""
    else:
        fulfillment.warning_level = ""
        fulfillment.warning_type = ""
        fulfillment.warning_text = ""


def refresh_shipment_warning_summary(db: Session, shipment: Shipment) -> None:
    alerts = (
        db.query(LogisticsAlert)
        .filter(LogisticsAlert.shipment_id == shipment.id, LogisticsAlert.is_resolved == False)
        .order_by(LogisticsAlert.created_at.desc(), LogisticsAlert.id.desc())
        .all()
    )
    top = None
    for alert in alerts:
        if top is None or SEVERITY_RANK.get(alert.alert_level or "", 0) > SEVERITY_RANK.get(top.alert_level or "", 0):
            top = alert
    if top:
        shipment.warning_level = top.alert_level or ""
        shipment.warning_text = top.alert_text or ""
    else:
        shipment.warning_level = ""
        shipment.warning_text = ""


def evaluate_fulfillment_alerts(db: Session, fulfillment: OrderFulfillment, order: Order | None = None) -> list[dict[str, Any]]:
    order = order or db.query(Order).filter(Order.id == fulfillment.order_id).first()
    if not order:
        return []
    now = datetime.utcnow()
    results: list[dict[str, Any]] = []
    ship_statuses = {"fulfilled", "shipped", "signed"}
    has_shipment = db.query(Shipment).filter(Shipment.fulfillment_id == fulfillment.id).count() > 0
    active_supplier_code = fulfillment.supplier_code_snapshot or order.supplier_code or ""

    if order.pay_status == "paid" and (fulfillment.fulfillment_status or "") not in ship_statuses and not fulfillment.shipped_at:
        base_time = order.paid_at or order.created_at or fulfillment.assigned_at or fulfillment.created_at
        hours = _hours_between(base_time, now)
        level = ""
        if hours >= 24:
            level = "red"
        elif hours >= 12:
            level = "orange"
        elif hours >= 6:
            level = "yellow"
        if level:
            text = f"已支付 {hours} 小时仍未发货"
            _upsert_alert(
                db,
                alert_type="not_shipped_timeout",
                level=level,
                text=text,
                supplier_code=active_supplier_code,
                fulfillment_id=fulfillment.id,
            )
            results.append({"alert_type": "not_shipped_timeout", "level": level, "hours": hours})
        else:
            _resolve_open_alert(db, "not_shipped_timeout", fulfillment_id=fulfillment.id)
    else:
        _resolve_open_alert(db, "not_shipped_timeout", fulfillment_id=fulfillment.id)

    if ((fulfillment.fulfillment_status or "") in ship_statuses or fulfillment.shipped_at) and not has_shipment:
        _upsert_alert(
            db,
            alert_type="no_tracking_number",
            level="red",
            text="履约单已进入发货状态，但尚未录入快递单号",
            supplier_code=active_supplier_code,
            fulfillment_id=fulfillment.id,
        )
        results.append({"alert_type": "no_tracking_number", "level": "red"})
    else:
        _resolve_open_alert(db, "no_tracking_number", fulfillment_id=fulfillment.id)

    refresh_fulfillment_warning_summary(db, fulfillment)
    return results


def evaluate_shipment_alerts(db: Session, shipment: Shipment, fulfillment: OrderFulfillment | None = None) -> list[dict[str, Any]]:
    now = datetime.utcnow()
    fulfillment = fulfillment or (db.query(OrderFulfillment).filter(OrderFulfillment.id == shipment.fulfillment_id).first() if shipment.fulfillment_id else None)
    supplier_code = shipment.supplier_code_snapshot or (fulfillment.supplier_code_snapshot if fulfillment else "") or ""
    results: list[dict[str, Any]] = []

    base_ship_time = shipment.created_at or (fulfillment.shipped_at if fulfillment else None)
    if shipment.ship_status != "signed" and not shipment.first_trace_at and base_ship_time:
        hours = _hours_between(base_ship_time, now)
        if hours >= 24:
            _upsert_alert(
                db,
                alert_type="no_first_trace",
                level="yellow",
                text=f"发货后 {hours} 小时仍无首条轨迹",
                supplier_code=supplier_code,
                fulfillment_id=fulfillment.id if fulfillment else None,
                shipment_id=shipment.id,
            )
            results.append({"alert_type": "no_first_trace", "level": "yellow", "hours": hours})
        else:
            _resolve_open_alert(db, "no_first_trace", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)
    else:
        _resolve_open_alert(db, "no_first_trace", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)

    if shipment.ship_status != "signed" and shipment.last_trace_time:
        stagnant_hours = _hours_between(shipment.last_trace_time, now)
        level = ""
        if stagnant_hours >= 72:
            level = "red"
        elif stagnant_hours >= 48:
            level = "orange"
        if level:
            _upsert_alert(
                db,
                alert_type="trace_stagnant",
                level=level,
                text=f"物流轨迹 {stagnant_hours} 小时未更新",
                supplier_code=supplier_code,
                fulfillment_id=fulfillment.id if fulfillment else None,
                shipment_id=shipment.id,
            )
            results.append({"alert_type": "trace_stagnant", "level": level, "hours": stagnant_hours})
        else:
            _resolve_open_alert(db, "trace_stagnant", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)
    else:
        _resolve_open_alert(db, "trace_stagnant", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)

    fail_count = int(getattr(shipment, "sync_fail_count", 0) or 0)
    if fail_count >= 3:
        level = "red" if fail_count >= 6 else "orange"
        _upsert_alert(
            db,
            alert_type="sync_fail",
            level=level,
            text=f"物流同步连续失败 {fail_count} 次",
            supplier_code=supplier_code,
            fulfillment_id=fulfillment.id if fulfillment else None,
            shipment_id=shipment.id,
        )
        results.append({"alert_type": "sync_fail", "level": level, "count": fail_count})
    else:
        _resolve_open_alert(db, "sync_fail", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)

    exception_text = f"{shipment.ship_status or ''} {shipment.last_trace_text or ''}".lower()
    if shipment.ship_status == "returned" or any(k in exception_text for k in EXCEPTION_KEYWORDS):
        _upsert_alert(
            db,
            alert_type="logistics_exception",
            level="red",
            text=(shipment.last_trace_text or "物流状态异常，请人工跟进")[:240],
            supplier_code=supplier_code,
            fulfillment_id=fulfillment.id if fulfillment else None,
            shipment_id=shipment.id,
        )
        results.append({"alert_type": "logistics_exception", "level": "red"})
    else:
        _resolve_open_alert(db, "logistics_exception", fulfillment_id=fulfillment.id if fulfillment else None, shipment_id=shipment.id)

    if fulfillment:
        fulfillment.last_track_at = shipment.last_trace_time
        fulfillment.track_stagnant_hours = _hours_between(shipment.last_trace_time, now) if shipment.last_trace_time else 0
        fulfillment.last_sync_at = shipment.last_sync_at
        fulfillment.sync_fail_count = int(getattr(shipment, "sync_fail_count", 0) or 0)
        refresh_fulfillment_warning_summary(db, fulfillment)
    refresh_shipment_warning_summary(db, shipment)
    return results


def scan_logistics_alerts(db: Session, supplier_code: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "supplier_code": supplier_code or "",
        "checked_fulfillments": 0,
        "checked_shipments": 0,
        "open_alerts": 0,
        "new_or_updated": 0,
        "by_type": {},
        "by_level": {"yellow": 0, "orange": 0, "red": 0},
        "generated_at": datetime.utcnow().isoformat(),
    }

    fq = db.query(OrderFulfillment, Order).join(Order, Order.id == OrderFulfillment.order_id)
    if supplier_code:
        fq = fq.filter(OrderFulfillment.supplier_code_snapshot == supplier_code)
    fulfillments = fq.order_by(OrderFulfillment.id.asc()).all()
    for fulfillment, order in fulfillments:
        alerts = evaluate_fulfillment_alerts(db, fulfillment, order)
        result["checked_fulfillments"] += 1
        for item in alerts:
            result["new_or_updated"] += 1

    sq = db.query(Shipment)
    if supplier_code:
        sq = sq.filter(Shipment.supplier_code_snapshot == supplier_code)
    shipments = sq.order_by(Shipment.id.asc()).all()
    for shipment in shipments:
        fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.id == shipment.fulfillment_id).first() if shipment.fulfillment_id else None
        alerts = evaluate_shipment_alerts(db, shipment, fulfillment)
        result["checked_shipments"] += 1
        for item in alerts:
            result["new_or_updated"] += 1

    open_alerts = db.query(LogisticsAlert).filter(LogisticsAlert.is_resolved == False)
    if supplier_code:
        open_alerts = open_alerts.filter(LogisticsAlert.supplier_code == supplier_code)
    rows = open_alerts.all()
    result["open_alerts"] = len(rows)
    by_type: dict[str, int] = {}
    for row in rows:
        by_type[row.alert_type or "unknown"] = by_type.get(row.alert_type or "unknown", 0) + 1
        if (row.alert_level or "") in result["by_level"]:
            result["by_level"][row.alert_level] += 1
    result["by_type"] = by_type
    db.flush()
    return result


def build_alert_overview(db: Session, supplier_code: str | None = None) -> dict[str, Any]:
    q = db.query(LogisticsAlert).filter(LogisticsAlert.is_resolved == False)
    if supplier_code:
        q = q.filter(LogisticsAlert.supplier_code == supplier_code)
    rows = q.order_by(LogisticsAlert.created_at.desc(), LogisticsAlert.id.desc()).all()
    overview = {
        "supplier_code": supplier_code or "",
        "total": len(rows),
        "by_level": {"yellow": 0, "orange": 0, "red": 0},
        "by_type": {},
        "oldest_hours": 0,
        "newest_hours": 0,
        "generated_at": datetime.utcnow().isoformat(),
    }
    now = datetime.utcnow()
    ages: list[int] = []
    for row in rows:
        level = (row.alert_level or "").lower()
        if level in overview["by_level"]:
            overview["by_level"][level] += 1
        overview["by_type"][row.alert_type or "unknown"] = overview["by_type"].get(row.alert_type or "unknown", 0) + 1
        ages.append(_alert_age_hours(row, now))
    if ages:
        overview["oldest_hours"] = max(ages)
        overview["newest_hours"] = min(ages)
    return overview


def alert_rows_with_age(rows: list[LogisticsAlert]) -> list[dict[str, Any]]:
    now = datetime.utcnow()
    result = []
    for row in rows:
        result.append(
            {
                "id": row.id,
                "supplier_code": row.supplier_code or "",
                "fulfillment_id": row.fulfillment_id,
                "shipment_id": row.shipment_id,
                "alert_type": row.alert_type or "",
                "alert_level": row.alert_level or "",
                "alert_text": row.alert_text or "",
                "created_at": row.created_at.isoformat() if row.created_at else "",
                "age_hours": _alert_age_hours(row, now),
            }
        )
    return result
