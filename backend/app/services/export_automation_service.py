from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from ..models import Supplier, SupplierExportJob, LogisticsAlert, OrderFulfillment, Shipment
from .shipment_export_service import build_shipments_workbook, get_export_file_path
from .logistics_alert_service import build_alert_overview


def _today_text(offset_days: int = 0) -> str:
    return (datetime.utcnow().date() + timedelta(days=offset_days)).isoformat()


def _active_suppliers(db: Session, supplier_code: str | None = None) -> list[Supplier]:
    q = db.query(Supplier).filter(Supplier.is_active == True)
    if supplier_code:
        q = q.filter(Supplier.supplier_code == supplier_code)
    return q.order_by(Supplier.supplier_code.asc()).all()


def _existing_job(db: Session, supplier_code: str, export_type: str, biz_date: str) -> SupplierExportJob | None:
    return (
        db.query(SupplierExportJob)
        .filter(
            SupplierExportJob.supplier_code == supplier_code,
            SupplierExportJob.export_type == export_type,
            SupplierExportJob.biz_date == biz_date,
            SupplierExportJob.status.in_(["auto_created", "bot_sent", "supplier_sent", "both_sent"]),
        )
        .order_by(SupplierExportJob.id.desc())
        .first()
    )


def _latest_job(db: Session, supplier_code: str, export_type: str) -> SupplierExportJob | None:
    return (
        db.query(SupplierExportJob)
        .filter(SupplierExportJob.supplier_code == supplier_code, SupplierExportJob.export_type == export_type)
        .order_by(SupplierExportJob.id.desc())
        .first()
    )


def mark_export_job_sent(db: Session, job_id: int, target: str) -> dict[str, Any]:
    job = db.query(SupplierExportJob).filter(SupplierExportJob.id == job_id).first()
    if not job:
        raise ValueError("导出批次不存在")
    now = datetime.utcnow()
    if target == 'bot':
        job.sent_to_bot_at = now
        job.status = 'both_sent' if job.sent_to_supplier_at else 'bot_sent'
    elif target == 'supplier':
        job.sent_to_supplier_at = now
        job.status = 'both_sent' if job.sent_to_bot_at else 'supplier_sent'
    else:
        raise ValueError("target 仅支持 bot / supplier")
    db.commit()
    return {
        'ok': True,
        'job_id': job.id,
        'status': job.status,
        'sent_to_bot_at': job.sent_to_bot_at.isoformat() if job.sent_to_bot_at else '',
        'sent_to_supplier_at': job.sent_to_supplier_at.isoformat() if job.sent_to_supplier_at else '',
    }


def build_daily_report(db: Session, supplier_code: str | None = None, biz_date: str | None = None) -> dict[str, Any]:
    shipped_date = biz_date or _today_text(-1)
    pending_date = _today_text(0)
    suppliers = _active_suppliers(db, supplier_code)
    rows: list[dict[str, Any]] = []
    for supplier in suppliers:
        pending_count = db.query(OrderFulfillment).filter(
            OrderFulfillment.supplier_code_snapshot == supplier.supplier_code,
            OrderFulfillment.fulfillment_status.in_(['assigned', 'accepted', 'pending', 'unassigned'])
        ).count()
        start = datetime.fromisoformat(shipped_date + 'T00:00:00')
        end = datetime.fromisoformat(shipped_date + 'T23:59:59.999999')
        shipped_count = db.query(OrderFulfillment).filter(
            OrderFulfillment.supplier_code_snapshot == supplier.supplier_code,
            OrderFulfillment.shipped_at.isnot(None),
            OrderFulfillment.shipped_at >= start,
            OrderFulfillment.shipped_at <= end,
        ).count()
        alert_overview = build_alert_overview(db, supplier_code=supplier.supplier_code)
        pending_job = (
            db.query(SupplierExportJob)
            .filter(SupplierExportJob.supplier_code == supplier.supplier_code, SupplierExportJob.export_type == 'pending', SupplierExportJob.biz_date == pending_date)
            .order_by(SupplierExportJob.id.desc())
            .first()
        )
        shipped_job = (
            db.query(SupplierExportJob)
            .filter(SupplierExportJob.supplier_code == supplier.supplier_code, SupplierExportJob.export_type == 'shipped', SupplierExportJob.biz_date == shipped_date)
            .order_by(SupplierExportJob.id.desc())
            .first()
        )
        warn_shipments = db.query(Shipment).filter(Shipment.supplier_code_snapshot == supplier.supplier_code, Shipment.warning_level.in_(['yellow', 'orange', 'red'])).count()
        rows.append({
            'supplier_code': supplier.supplier_code,
            'supplier_name': supplier.supplier_name or '',
            'pending_count': pending_count,
            'shipped_count': shipped_count,
            'alert_total': alert_overview.get('total', 0),
            'alert_red': (alert_overview.get('by_level') or {}).get('red', 0),
            'alert_orange': (alert_overview.get('by_level') or {}).get('orange', 0),
            'alert_yellow': (alert_overview.get('by_level') or {}).get('yellow', 0),
            'warning_shipments': warn_shipments,
            'pending_export_job_id': pending_job.id if pending_job else None,
            'pending_export_status': pending_job.status if pending_job else '',
            'pending_export_file': pending_job.file_name if pending_job else '',
            'shipped_export_job_id': shipped_job.id if shipped_job else None,
            'shipped_export_status': shipped_job.status if shipped_job else '',
            'shipped_export_file': shipped_job.file_name if shipped_job else '',
        })
    return {
        'generated_at': datetime.utcnow().isoformat(),
        'pending_biz_date': pending_date,
        'shipped_biz_date': shipped_date,
        'supplier_code': supplier_code or '',
        'rows': rows,
        'totals': {
            'suppliers': len(rows),
            'pending_count': sum(int(r['pending_count'] or 0) for r in rows),
            'shipped_count': sum(int(r['shipped_count'] or 0) for r in rows),
            'alert_total': sum(int(r['alert_total'] or 0) for r in rows),
            'warning_shipments': sum(int(r['warning_shipments'] or 0) for r in rows),
        },
    }


def run_daily_export_automation(
    db: Session,
    supplier_code: str | None = None,
    shipped_biz_date: str | None = None,
    pending_biz_date: str | None = None,
    force: bool = False,
    auto: bool = False,
) -> dict[str, Any]:
    shipped_biz_date = shipped_biz_date or _today_text(-1)
    pending_biz_date = pending_biz_date or _today_text(0)
    suppliers = _active_suppliers(db, supplier_code)
    result: dict[str, Any] = {
        'ok': True,
        'supplier_code': supplier_code or '',
        'pending_biz_date': pending_biz_date,
        'shipped_biz_date': shipped_biz_date,
        'created': 0,
        'skipped': 0,
        'rows': [],
        'generated_at': datetime.utcnow().isoformat(),
    }
    for supplier in suppliers:
        for export_type, biz_date in [('pending', pending_biz_date), ('shipped', shipped_biz_date)]:
            existed = _existing_job(db, supplier.supplier_code, export_type, biz_date)
            if existed and not force:
                result['skipped'] += 1
                result['rows'].append({
                    'supplier_code': supplier.supplier_code,
                    'export_type': export_type,
                    'biz_date': biz_date,
                    'job_id': existed.id,
                    'status': existed.status,
                    'file_name': existed.file_name,
                    'action': 'skipped_existing',
                })
                continue
            build_shipments_workbook(
                db,
                mode=export_type,
                supplier_code=supplier.supplier_code,
                biz_date=biz_date,
                sample_only=False,
                persist_job=True,
                job_status='auto_created' if auto else 'created',
                save_to_disk=True,
            )
            job = _latest_job(db, supplier.supplier_code, export_type)
            result['created'] += 1
            result['rows'].append({
                'supplier_code': supplier.supplier_code,
                'export_type': export_type,
                'biz_date': biz_date,
                'job_id': job.id if job else None,
                'status': job.status if job else '',
                'file_name': job.file_name if job else '',
                'action': 'created',
            })
    return result


def get_job_download_path(db: Session, job_id: int) -> tuple[SupplierExportJob, Path]:
    job = db.query(SupplierExportJob).filter(SupplierExportJob.id == job_id).first()
    if not job:
        raise ValueError('导出批次不存在')
    path = get_export_file_path(job.file_name or '')
    if not path.exists():
        build_shipments_workbook(
            db,
            mode='shipped' if (job.export_type or '') == 'shipped' else 'pending',
            supplier_code=job.supplier_code or None,
            biz_date=job.biz_date or None,
            sample_only=False,
            persist_job=False,
            save_to_disk=True,
        )
        path = get_export_file_path(job.file_name or '')
    return job, path
