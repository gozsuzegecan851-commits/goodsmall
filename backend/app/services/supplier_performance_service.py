from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from ..models import LogisticsAlert, Order, OrderFulfillment, Shipment, Supplier, SupplierExportJob

PENDING_STATUSES = {"assigned", "accepted", "pending", "unassigned"}
SHIPPED_STATUSES = {"fulfilled", "shipped", "signed"}
GRADE_THRESHOLDS = [(90, "A"), (80, "B"), (70, "C"), (60, "D")]

SCORE_MODEL = {
    "weights": {
        "ship_timeliness": 35,
        "trace_timeliness": 25,
        "alert_health": 20,
        "sync_health": 10,
        "export_discipline": 10,
    },
    "rules": [
        {"metric": "ship_timeliness", "summary": "近窗口内履约单发货及时率，24 小时内发货越多得分越高。"},
        {"metric": "trace_timeliness", "summary": "发货后 24 小时内出现首轨迹的比例，越及时得分越高。"},
        {"metric": "alert_health", "summary": "开放预警越少越好，红/橙/黄预警会拉低分数。"},
        {"metric": "sync_health", "summary": "物流同步连续失败越少越好，失败 3 次以上开始扣分。"},
        {"metric": "export_discipline", "summary": "今日待发货导出和昨日已发货导出都已生成则满分。"},
    ],
    "grade_thresholds": [
        {"grade": "A", "min_score": 90, "label": "优秀"},
        {"grade": "B", "min_score": 80, "label": "稳定"},
        {"grade": "C", "min_score": 70, "label": "可观察"},
        {"grade": "D", "min_score": 60, "label": "偏弱"},
        {"grade": "E", "min_score": 0, "label": "高风险"},
    ],
}


def clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def safe_ratio(numerator: int, denominator: int, fallback: float = 1.0) -> float:
    if denominator <= 0:
        return fallback
    return numerator / denominator


def score_grade(score: float) -> str:
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "E"


def score_label(score: float) -> str:
    return {
        "A": "优秀",
        "B": "稳定",
        "C": "可观察",
        "D": "偏弱",
        "E": "高风险",
    }.get(score_grade(score), "高风险")


def score_risk(score: float, red_alerts: int = 0, overdue_pending_count: int = 0) -> str:
    if red_alerts > 0 or score < 60 or overdue_pending_count >= 3:
        return "high"
    if score < 80:
        return "medium"
    return "low"


def format_risk_label(level: str) -> str:
    return {"high": "高风险", "medium": "中风险", "low": "低风险"}.get(level, "低风险")


def _hours_between(start: datetime | None, end: datetime | None = None) -> float | None:
    if not start:
        return None
    end = end or datetime.utcnow()
    return max(0.0, (end - start).total_seconds() / 3600.0)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _latest_job(db: Session, supplier_code: str, export_type: str, biz_date: str) -> SupplierExportJob | None:
    return (
        db.query(SupplierExportJob)
        .filter(
            SupplierExportJob.supplier_code == supplier_code,
            SupplierExportJob.export_type == export_type,
            SupplierExportJob.biz_date == biz_date,
        )
        .order_by(SupplierExportJob.id.desc())
        .first()
    )


def _collect_recent_fulfillments(db: Session, supplier_code: str, start_at: datetime) -> list[tuple[OrderFulfillment, Order]]:
    q = db.query(OrderFulfillment, Order).join(Order, Order.id == OrderFulfillment.order_id).filter(OrderFulfillment.supplier_code_snapshot == supplier_code)
    rows = q.order_by(OrderFulfillment.id.asc()).all()
    result: list[tuple[OrderFulfillment, Order]] = []
    for fulfillment, order in rows:
        if (
            (fulfillment.created_at and fulfillment.created_at >= start_at)
            or (fulfillment.shipped_at and fulfillment.shipped_at >= start_at)
            or (fulfillment.fulfillment_status or "") in PENDING_STATUSES
        ):
            result.append((fulfillment, order))
    return result


def _collect_recent_shipments(db: Session, supplier_code: str, start_at: datetime) -> list[Shipment]:
    q = db.query(Shipment).filter(Shipment.supplier_code_snapshot == supplier_code).order_by(Shipment.id.asc())
    rows = q.all()
    result: list[Shipment] = []
    for shipment in rows:
        if (
            (shipment.created_at and shipment.created_at >= start_at)
            or (shipment.last_sync_at and shipment.last_sync_at >= start_at)
            or (shipment.last_trace_time and shipment.last_trace_time >= start_at)
            or (shipment.ship_status or "") != "signed"
        ):
            result.append(shipment)
    return result


def compute_supplier_performance(db: Session, supplier: Supplier, days: int = 7, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.utcnow()
    days = max(1, int(days or 7))
    start_at = now - timedelta(days=days)
    supplier_code = (supplier.supplier_code or "").strip()
    rows = _collect_recent_fulfillments(db, supplier_code, start_at)
    shipments = _collect_recent_shipments(db, supplier_code, start_at)

    pending_total = 0
    overdue_pending_count = 0
    recent_created_count = 0
    shipped_total = 0
    on_time_ship_count = 0
    ship_hours_values: list[float] = []

    for fulfillment, order in rows:
        status = (fulfillment.fulfillment_status or "").strip().lower()
        base_time = order.paid_at or order.created_at or fulfillment.assigned_at or fulfillment.created_at
        if fulfillment.created_at and fulfillment.created_at >= start_at:
            recent_created_count += 1
        if status in PENDING_STATUSES and not fulfillment.shipped_at:
            pending_total += 1
            hours = _hours_between(base_time, now)
            if hours is not None and hours >= 24:
                overdue_pending_count += 1
        if fulfillment.shipped_at and fulfillment.shipped_at >= start_at:
            shipped_total += 1
            ship_hours = _hours_between(base_time, fulfillment.shipped_at)
            if ship_hours is not None:
                ship_hours_values.append(ship_hours)
                if ship_hours <= 24:
                    on_time_ship_count += 1

    ship_on_time_rate = safe_ratio(on_time_ship_count, shipped_total, fallback=1.0 if pending_total == 0 else 0.0)
    avg_ship_hours = round(_mean(ship_hours_values), 1)

    first_trace_total = 0
    first_trace_on_time = 0
    first_trace_overdue_count = 0
    first_trace_hours_values: list[float] = []
    stagnant_48_count = 0
    stagnant_72_count = 0
    sync_fail_shipments = 0
    returned_count = 0

    for shipment in shipments:
        base_ship_time = shipment.created_at
        if shipment.fulfillment_id:
            fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.id == shipment.fulfillment_id).first()
            if fulfillment and fulfillment.shipped_at:
                base_ship_time = fulfillment.shipped_at
        if base_ship_time:
            first_trace_total += 1
            if shipment.first_trace_at:
                hours = _hours_between(base_ship_time, shipment.first_trace_at)
                if hours is not None:
                    first_trace_hours_values.append(hours)
                    if hours <= 24:
                        first_trace_on_time += 1
            else:
                overdue_hours = _hours_between(base_ship_time, now)
                if overdue_hours is not None and overdue_hours >= 24:
                    first_trace_overdue_count += 1
        last_age = _hours_between(shipment.last_trace_time, now) if shipment.last_trace_time else None
        if last_age is not None and last_age >= 48:
            stagnant_48_count += 1
        if last_age is not None and last_age >= 72:
            stagnant_72_count += 1
        if int(getattr(shipment, "sync_fail_count", 0) or 0) >= 3:
            sync_fail_shipments += 1
        if (shipment.ship_status or "").lower() == "returned":
            returned_count += 1

    first_trace_on_time_rate = safe_ratio(first_trace_on_time, first_trace_total, fallback=1.0 if not shipments else 0.0)
    avg_first_trace_hours = round(_mean(first_trace_hours_values), 1)

    open_alerts = db.query(LogisticsAlert).filter(LogisticsAlert.supplier_code == supplier_code, LogisticsAlert.is_resolved == False).all()
    red_alerts = sum(1 for row in open_alerts if (row.alert_level or "") == "red")
    orange_alerts = sum(1 for row in open_alerts if (row.alert_level or "") == "orange")
    yellow_alerts = sum(1 for row in open_alerts if (row.alert_level or "") == "yellow")
    exception_alerts = sum(1 for row in open_alerts if (row.alert_type or "") == "logistics_exception")
    open_alert_total = len(open_alerts)

    pending_today = now.date().isoformat()
    shipped_yesterday = (now.date() - timedelta(days=1)).isoformat()
    pending_job = _latest_job(db, supplier_code, "pending", pending_today)
    shipped_job = _latest_job(db, supplier_code, "shipped", shipped_yesterday)
    export_ready_count = int(bool(pending_job)) + int(bool(shipped_job))

    weights = SCORE_MODEL["weights"]
    ship_component = weights["ship_timeliness"] * ship_on_time_rate - overdue_pending_count * 2.5
    trace_component = weights["trace_timeliness"] * first_trace_on_time_rate - first_trace_overdue_count * 2.0 - stagnant_72_count * 1.5
    alert_component = weights["alert_health"] - red_alerts * 5 - orange_alerts * 3 - yellow_alerts * 1 - exception_alerts * 2
    sync_component = weights["sync_health"] - sync_fail_shipments * 2
    export_component = weights["export_discipline"] if export_ready_count == 2 else (weights["export_discipline"] * 0.5 if export_ready_count == 1 else 0.0)

    ship_component = clamp(ship_component, 0.0, float(weights["ship_timeliness"]))
    trace_component = clamp(trace_component, 0.0, float(weights["trace_timeliness"]))
    alert_component = clamp(alert_component, 0.0, float(weights["alert_health"]))
    sync_component = clamp(sync_component, 0.0, float(weights["sync_health"]))
    export_component = clamp(export_component, 0.0, float(weights["export_discipline"]))

    total_score = round(clamp(ship_component + trace_component + alert_component + sync_component + export_component), 1)
    grade = score_grade(total_score)
    risk_level = score_risk(total_score, red_alerts=red_alerts, overdue_pending_count=overdue_pending_count)

    highlights: list[str] = []
    if ship_on_time_rate >= 0.9 and shipped_total > 0:
        highlights.append(f"近{days}天 24 小时内发货率 {round(ship_on_time_rate * 100)}%")
    if first_trace_on_time_rate >= 0.85 and first_trace_total > 0:
        highlights.append(f"首轨迹及时率 {round(first_trace_on_time_rate * 100)}%")
    if export_ready_count == 2:
        highlights.append("日报导出闭环完整")

    issues: list[str] = []
    if overdue_pending_count > 0:
        issues.append(f"有 {overdue_pending_count} 单已超 24 小时未发货")
    if red_alerts > 0:
        issues.append(f"仍有 {red_alerts} 条红色预警")
    if first_trace_overdue_count > 0:
        issues.append(f"有 {first_trace_overdue_count} 单首轨迹超时")
    if stagnant_72_count > 0:
        issues.append(f"有 {stagnant_72_count} 单物流停更超过 72 小时")
    if sync_fail_shipments > 0:
        issues.append(f"有 {sync_fail_shipments} 单同步连续失败")
    if returned_count > 0:
        issues.append(f"有 {returned_count} 单退回或异常状态")
    if export_ready_count < 2:
        issues.append("日报导出仍未形成完整闭环")

    return {
        "supplier_code": supplier_code,
        "supplier_name": supplier.supplier_name or "",
        "days": days,
        "window_start": start_at.isoformat(),
        "window_end": now.isoformat(),
        "score": total_score,
        "grade": grade,
        "grade_label": score_label(total_score),
        "risk_level": risk_level,
        "risk_label": format_risk_label(risk_level),
        "components": {
            "ship_timeliness": round(ship_component, 1),
            "trace_timeliness": round(trace_component, 1),
            "alert_health": round(alert_component, 1),
            "sync_health": round(sync_component, 1),
            "export_discipline": round(export_component, 1),
        },
        "metrics": {
            "recent_created_count": recent_created_count,
            "pending_total": pending_total,
            "overdue_pending_count": overdue_pending_count,
            "shipped_total": shipped_total,
            "ship_on_time_rate": round(ship_on_time_rate, 4),
            "avg_ship_hours": avg_ship_hours,
            "first_trace_total": first_trace_total,
            "first_trace_overdue_count": first_trace_overdue_count,
            "first_trace_on_time_rate": round(first_trace_on_time_rate, 4),
            "avg_first_trace_hours": avg_first_trace_hours,
            "open_alert_total": open_alert_total,
            "red_alerts": red_alerts,
            "orange_alerts": orange_alerts,
            "yellow_alerts": yellow_alerts,
            "exception_alerts": exception_alerts,
            "sync_fail_shipments": sync_fail_shipments,
            "stagnant_48_count": stagnant_48_count,
            "stagnant_72_count": stagnant_72_count,
            "returned_count": returned_count,
            "pending_export_ready": bool(pending_job),
            "shipped_export_ready": bool(shipped_job),
        },
        "highlights": highlights,
        "issues": issues,
        "latest_jobs": {
            "pending": {
                "job_id": pending_job.id if pending_job else None,
                "status": pending_job.status if pending_job else "",
                "file_name": pending_job.file_name if pending_job else "",
            },
            "shipped": {
                "job_id": shipped_job.id if shipped_job else None,
                "status": shipped_job.status if shipped_job else "",
                "file_name": shipped_job.file_name if shipped_job else "",
            },
        },
    }


def build_supplier_performance_overview(db: Session, supplier_code: str | None = None, days: int = 7) -> dict[str, Any]:
    now = datetime.utcnow()
    days = max(1, int(days or 7))
    q = db.query(Supplier).filter(Supplier.is_active == True)
    if supplier_code:
        q = q.filter(Supplier.supplier_code == supplier_code)
    suppliers = q.order_by(Supplier.supplier_code.asc()).all()
    rows = [compute_supplier_performance(db, supplier, days=days, now=now) for supplier in suppliers]
    rows.sort(key=lambda item: (-float(item.get("score") or 0), item.get("supplier_code") or ""))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    avg_score = round(sum(float(r.get("score") or 0) for r in rows) / len(rows), 1) if rows else 0.0
    return {
        "generated_at": now.isoformat(),
        "days": days,
        "supplier_code": supplier_code or "",
        "score_model": SCORE_MODEL,
        "rows": rows,
        "totals": {
            "suppliers": len(rows),
            "avg_score": avg_score,
            "high_risk": sum(1 for r in rows if r.get("risk_level") == "high"),
            "medium_risk": sum(1 for r in rows if r.get("risk_level") == "medium"),
            "low_risk": sum(1 for r in rows if r.get("risk_level") == "low"),
            "red_alerts": sum(int((r.get("metrics") or {}).get("red_alerts") or 0) for r in rows),
            "overdue_pending": sum(int((r.get("metrics") or {}).get("overdue_pending_count") or 0) for r in rows),
        },
    }


def build_supplier_performance_detail(db: Session, supplier_code: str, days: int = 7) -> dict[str, Any]:
    supplier = db.query(Supplier).filter(Supplier.supplier_code == supplier_code).first()
    if not supplier:
        raise ValueError("供应链不存在")
    now = datetime.utcnow()
    row = compute_supplier_performance(db, supplier, days=days, now=now)
    row["generated_at"] = now.isoformat()
    row["score_model"] = SCORE_MODEL
    return row


def _trend_direction(delta: float) -> str:
    if delta >= 5:
        return "up"
    if delta <= -5:
        return "down"
    return "flat"


def build_supplier_performance_trends(db: Session, supplier_code: str | None = None, days: int = 15, window_days: int = 7) -> dict[str, Any]:
    now = datetime.utcnow()
    days = max(3, int(days or 15))
    window_days = max(1, int(window_days or 7))
    q = db.query(Supplier).filter(Supplier.is_active == True)
    if supplier_code:
        q = q.filter(Supplier.supplier_code == supplier_code)
    suppliers = q.order_by(Supplier.supplier_code.asc()).all()
    point_times = []
    base_day = now.date() - timedelta(days=days - 1)
    for idx in range(days):
        day = base_day + timedelta(days=idx)
        point_times.append(datetime(day.year, day.month, day.day, 23, 59, 59))

    rows: list[dict[str, Any]] = []
    for supplier in suppliers:
        points: list[dict[str, Any]] = []
        for point_now in point_times:
            perf = compute_supplier_performance(db, supplier, days=window_days, now=point_now)
            metrics = perf.get('metrics') or {}
            points.append({
                'date': point_now.date().isoformat(),
                'score': perf.get('score', 0),
                'grade': perf.get('grade', 'E'),
                'risk_level': perf.get('risk_level', 'high'),
                'red_alerts': metrics.get('red_alerts', 0),
                'open_alert_total': metrics.get('open_alert_total', 0),
                'overdue_pending_count': metrics.get('overdue_pending_count', 0),
                'ship_on_time_rate': metrics.get('ship_on_time_rate', 0),
                'avg_ship_hours': metrics.get('avg_ship_hours', 0),
                'first_trace_on_time_rate': metrics.get('first_trace_on_time_rate', 0),
                'avg_first_trace_hours': metrics.get('avg_first_trace_hours', 0),
            })
        first = points[0] if points else {}
        last = points[-1] if points else {}
        delta_score = round(float(last.get('score') or 0) - float(first.get('score') or 0), 1) if points else 0.0
        delta_red_alerts = int(last.get('red_alerts') or 0) - int(first.get('red_alerts') or 0) if points else 0
        delta_ship_rate = round((float(last.get('ship_on_time_rate') or 0) - float(first.get('ship_on_time_rate') or 0)) * 100, 1) if points else 0.0
        row = {
            'supplier_code': supplier.supplier_code or '',
            'supplier_name': supplier.supplier_name or '',
            'days': days,
            'window_days': window_days,
            'trend_direction': _trend_direction(delta_score),
            'current_score': last.get('score', 0),
            'current_grade': last.get('grade', 'E'),
            'current_risk': last.get('risk_level', 'high'),
            'delta_score': delta_score,
            'delta_red_alerts': delta_red_alerts,
            'delta_ship_rate': delta_ship_rate,
            'min_score': round(min(float(pt.get('score') or 0) for pt in points), 1) if points else 0.0,
            'max_score': round(max(float(pt.get('score') or 0) for pt in points), 1) if points else 0.0,
            'points': points,
        }
        rows.append(row)

    rows.sort(key=lambda item: (-float(item.get('current_score') or 0), -(float(item.get('delta_score') or 0)), item.get('supplier_code') or ''))
    for idx, row in enumerate(rows, start=1):
        row['rank'] = idx

    avg_delta = round(sum(float(r.get('delta_score') or 0) for r in rows) / len(rows), 1) if rows else 0.0
    improved = sum(1 for r in rows if float(r.get('delta_score') or 0) >= 5)
    declined = sum(1 for r in rows if float(r.get('delta_score') or 0) <= -5)
    red_alert_improved = sum(1 for r in rows if int(r.get('delta_red_alerts') or 0) < 0)
    return {
        'generated_at': now.isoformat(),
        'days': days,
        'window_days': window_days,
        'supplier_code': supplier_code or '',
        'rows': rows,
        'totals': {
            'suppliers': len(rows),
            'avg_delta_score': avg_delta,
            'improved_suppliers': improved,
            'declined_suppliers': declined,
            'red_alert_improved_suppliers': red_alert_improved,
        },
    }
