from __future__ import annotations

from datetime import datetime
import io
import json
import uuid

import openpyxl
from sqlalchemy.orm import Session

from ..config import settings
from ..models import Order, Shipment, ShipmentImportBatch, ShipmentImportError, OrderFulfillment, Supplier
from .order_service import mark_order_shipped_state


def import_shipments(db: Session, file_bytes: bytes, filename: str, operator_name: str = "admin", supplier_code: str = "") -> ShipmentImportBatch:
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("导入文件为空")
    headers = [str(v).strip() if v is not None else "" for v in rows[0]]
    idx = {h: i for i, h in enumerate(headers)}
    required = ["订单号", "快递公司", "快递编码", "快递单号"]
    missing = [h for h in required if h not in idx]
    if missing:
        raise ValueError("模板缺少字段：" + "、".join(missing))

    batch = ShipmentImportBatch(
        batch_no=f"IMP{datetime.utcnow():%Y%m%d%H%M%S}{uuid.uuid4().hex[:6].upper()}",
        file_name=filename,
        biz_date=datetime.utcnow().date().isoformat(),
        supplier_code=supplier_code or "",
        total_rows=0,
        success_rows=0,
        failed_rows=0,
        operator_name=operator_name,
    )
    db.add(batch)
    db.flush()

    total = success = failed = 0
    latest_biz_date = None
    for row_no, row in enumerate(rows[1:], start=2):
        if row is None or all(v in (None, "") for v in row):
            continue
        total += 1
        order_no = str(row[idx["订单号"]] or "").strip()
        courier_company = str(row[idx["快递公司"]] or "").strip()
        courier_code = str(row[idx["快递编码"]] or "").strip().lower()
        tracking_no = str(row[idx["快递单号"]] or "").strip()
        shipped_at_text = str(row[idx.get("发货时间", -1)] if idx.get("发货时间") is not None else "" or "").strip()
        shipped_at = None
        if shipped_at_text:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
                try:
                    shipped_at = datetime.strptime(shipped_at_text, fmt)
                    break
                except Exception:
                    pass
        if shipped_at:
            latest_biz_date = (shipped_at.date().isoformat())
        try:
            order = db.query(Order).filter(Order.order_no == order_no).first()
            if not order:
                raise ValueError("订单不存在")
            if order.pay_status != "paid":
                raise ValueError("订单未支付，不能录入发货")
            if not tracking_no:
                raise ValueError("快递单号不能为空")
            if supplier_code and (getattr(order, "supplier_code", "") or "") not in {"", supplier_code}:
                raise ValueError("订单供应链不匹配")
            exists_same_tracking = db.query(Shipment).filter(Shipment.tracking_no == tracking_no, Shipment.order_id != order.id).first()
            if exists_same_tracking:
                raise ValueError("快递单号已被其他订单使用")

            shipment = db.query(Shipment).filter(Shipment.order_id == order.id).first()
            if shipment is None:
                shipment = Shipment(order_id=order.id)
                db.add(shipment)
            shipment.courier_company = courier_company
            shipment.courier_code = courier_code
            shipment.tracking_no = tracking_no
            shipment.ship_status = "shipped"
            shipment.provider_name = settings.logistics_provider or "kuaidi100"
            shipment.sync_status = "pending"
            shipment.sync_error = ""
            shipment.last_sync_at = None

            order.courier_company = courier_company
            order.courier_code = courier_code
            order.tracking_no = tracking_no
            mark_order_shipped_state(db, order, shipped_at=shipped_at or datetime.utcnow())

            fulfillment = db.query(OrderFulfillment).filter(OrderFulfillment.order_id == order.id).order_by(OrderFulfillment.id.desc()).first()
            if fulfillment:
                fulfillment.sync_status = "pending"
                fulfillment.sync_error = ""
            elif order.supplier_code:
                supplier = db.query(Supplier).filter(Supplier.supplier_code == order.supplier_code).first()
                if supplier:
                    db.add(OrderFulfillment(
                        order_id=order.id,
                        supplier_id=supplier.id,
                        fulfillment_status="shipped",
                        assigned_at=order.created_at,
                        shipped_at=order.shipped_at,
                        sync_status="pending",
                        sync_error="",
                    ))
            success += 1
        except Exception as e:
            failed += 1
            db.add(ShipmentImportError(
                batch_id=batch.id,
                row_no=row_no,
                order_no=order_no,
                tracking_no=tracking_no,
                error_message=str(e),
                raw_row_json=json.dumps([str(v) for v in row], ensure_ascii=False),
            ))
    batch.total_rows = total
    batch.success_rows = success
    batch.failed_rows = failed
    if latest_biz_date:
        batch.biz_date = latest_biz_date
    db.commit()
    db.refresh(batch)
    return batch
