"""快递助手（KDZS）开放平台：鉴权、签名、请求组装、响应归一化均在本模块完成。

环境变量（未配置则后台同步仍回退快递100，见 logistics_sync）：
- KDZS_APP_KEY、KDZS_APP_SECRET、KDZS_GATEWAY_URL

签名 v1（与控制台不一致时仅改本文件 _sign_v1 / _kdzs_invoke）：
sign = MD5(appKey + method + timestamp + bizContent + appSecret).upper()
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any

import httpx

_KDZS_APP_KEY = (os.getenv("KDZS_APP_KEY") or "").strip()
_KDZS_APP_SECRET = (os.getenv("KDZS_APP_SECRET") or "").strip()
_KDZS_GATEWAY_URL = (os.getenv("KDZS_GATEWAY_URL") or "https://openapi.kdzs.com/router/rest").strip()


def kdzs_configured() -> bool:
    return bool(_KDZS_APP_KEY and _KDZS_APP_SECRET)


def _sign_v1(method: str, timestamp: str, biz_content: str) -> str:
    raw = f"{_KDZS_APP_KEY}{method}{timestamp}{biz_content}{_KDZS_APP_SECRET}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()


def _kdzs_invoke(method: str, biz: dict[str, Any], timeout: float = 25.0) -> dict[str, Any]:
    if not kdzs_configured():
        raise RuntimeError("快递助手未配置（KDZS_APP_KEY / KDZS_APP_SECRET）")
    ts = str(int(time.time()))
    biz_json = json.dumps(biz, ensure_ascii=False, separators=(",", ":"))
    payload = {
        "appKey": _KDZS_APP_KEY,
        "method": method,
        "timestamp": ts,
        "bizContent": biz_json,
        "sign": _sign_v1(method, ts, biz_json),
    }
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            _KDZS_GATEWAY_URL,
            headers={"Content-Type": "application/json; charset=utf-8"},
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
    if not isinstance(data, dict):
        return {"success": False, "message": "快递助手返回格式异常"}
    return data


def _biz_mail(cp: str, num: str) -> dict[str, str]:
    return {
        "cpCode": str(cp or "").strip().lower(),
        "mailNo": str(num or "").strip(),
    }


def kdzs_trace_subscribe(tracking_no: str, courier_code: str) -> dict[str, Any]:
    """单票订阅（method：kdzs.logistics.trace.subscribe）。"""
    biz = _biz_mail(courier_code, tracking_no)
    return _kdzs_invoke("kdzs.logistics.trace.subscribe", biz)


def kdzs_trace_search(tracking_no: str, courier_code: str) -> dict[str, Any]:
    """实时查询（method：kdzs.logistics.trace.search）。"""
    biz = _biz_mail(courier_code, tracking_no)
    return _kdzs_invoke("kdzs.logistics.trace.search", biz)


def kdzs_trace_batch_subscribe(items: list[dict[str, Any]]) -> dict[str, Any]:
    """
    批量订阅（method：kdzs.logistics.trace.batch.subscribe）。
    items 每项建议：{"tracking_no": "...", "courier_code": "..."}，兼容 cpCode/mailNo。
    """
    waybills: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        tn = str(it.get("tracking_no") or it.get("mailNo") or it.get("num") or "").strip()
        cc = str(it.get("courier_code") or it.get("cpCode") or it.get("com") or "").strip()
        if tn and cc:
            waybills.append(_biz_mail(cc, tn))
    biz = {"waybillList": waybills}
    return _kdzs_invoke("kdzs.logistics.trace.batch.subscribe", biz)


def _subscribe_http_ok(raw: dict[str, Any]) -> bool:
    if raw.get("success") is True:
        return True
    code = raw.get("code")
    if code in (0, "0", 10000, "10000", 200, "200"):
        return True
    data = raw.get("data")
    if isinstance(data, dict) and data.get("success") is True:
        return True
    return False


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _extract_trace_list(data: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        data.get("traces"),
        data.get("traceList"),
        data.get("detailList"),
        data.get("details"),
        data.get("list"),
    ]
    for sub in candidates:
        if isinstance(sub, dict):
            inner = sub.get("traces") or sub.get("traceList") or sub.get("detailList") or sub.get("details")
            if isinstance(inner, list):
                sub = inner
        if isinstance(sub, list) and sub:
            out: list[dict[str, Any]] = []
            for item in sub:
                if not isinstance(item, dict):
                    continue
                text = (
                    item.get("context")
                    or item.get("desc")
                    or item.get("remark")
                    or item.get("traceDesc")
                    or item.get("statusDesc")
                    or ""
                )
                tm = item.get("ftime") or item.get("time") or item.get("traceTime") or item.get("acceptTime")
                out.append({"time": str(tm or "").strip(), "context": str(text or "").strip()})
            return out
    return []


def _infer_ship_status(traces: list[dict[str, Any]], raw_status: str | None) -> tuple[str, str]:
    st = (raw_status or "").strip().upper()
    status_map = {
        "SIGN": ("signed", "已签收"),
        "SIGNED": ("signed", "已签收"),
        "3": ("signed", "已签收"),
        "DELIVERED": ("signed", "已签收"),
        "RETURN": ("returned", "退回"),
        "RETURNED": ("returned", "退回"),
        "WAIT_ACCEPT": ("pending", "待揽收"),
        "ACCEPT": ("shipped", "运输中"),
        "TRANSPORT": ("shipped", "运输中"),
        "TRANSIT": ("shipped", "运输中"),
    }
    if st in status_map:
        return status_map[st]
    joined = " ".join(t.get("context", "") for t in traces[:5]).lower()
    if any(x in joined for x in ("签收", "已签收", "派件成功", "投递成功")):
        return "signed", "已签收"
    if any(x in joined for x in ("退回", "拒签")):
        return "returned", "退回"
    if traces:
        return "shipped", "运输中"
    return "pending", "待揽收"


def normalize_kdzs_trace_summary(
    raw: dict[str, Any],
    *,
    tracking_no: str = "",
    courier_code: str = "",
    for_subscribe: bool = False,
) -> dict[str, Any]:
    """
    统一摘要结构（供 jobs 使用，不向外层暴露第三方原始字段名）。
    - for_subscribe=True：解析订阅接口返回
    - for_subscribe=False：解析查询接口返回
    """
    tn = str(tracking_no or "").strip()
    cc = str(courier_code or "").strip().lower()
    raw_payload = dict(raw) if isinstance(raw, dict) else {}

    if for_subscribe:
        sub_ok = _subscribe_http_ok(raw)
        err = str(raw.get("errorMsg") or raw.get("message") or raw.get("subMsg") or "").strip()
        return {
            "ok": sub_ok,
            "message": "" if sub_ok else (err or "快递助手订阅未成功"),
            "subscribe_ok": sub_ok,
            "tracking_no": tn,
            "courier_code": cc,
            "status_code": "subscribed" if sub_ok else "subscribe_failed",
            "status_text": "已提交订阅" if sub_ok else "订阅失败",
            "latest_trace_text": "",
            "latest_trace_time": None,
            "recent_traces": [],
            "signed_at": None,
            "raw_payload": raw_payload,
        }

    code = raw.get("code")
    success = raw.get("success") is True or raw.get("success") == "true"
    err_msg = str(raw.get("errorMsg") or raw.get("message") or raw.get("subMsg") or "").strip()

    data_block = raw.get("data")
    if data_block is None and isinstance(raw.get("result"), dict):
        data_block = raw["result"]
    if not isinstance(data_block, dict):
        data_block = {}

    if code not in (None, 0, "0", 10000, "10000", 200, "200") and not success:
        if str(code) not in ("10000", "0"):
            return {
                "ok": False,
                "message": err_msg or "快递助手查询失败",
                "subscribe_ok": False,
                "tracking_no": tn,
                "courier_code": cc,
                "status_code": "",
                "status_text": "",
                "latest_trace_text": "",
                "latest_trace_time": None,
                "recent_traces": [],
                "signed_at": None,
                "raw_payload": raw_payload,
            }

    traces = _extract_trace_list(data_block)
    raw_status = None
    if isinstance(data_block, dict):
        rs = (
            data_block.get("logisticsStatus")
            or data_block.get("status")
            or data_block.get("mailStatus")
            or data_block.get("state")
        )
        raw_status = str(rs) if rs is not None else None

    status_code, status_text = _infer_ship_status(traces, raw_status)
    latest_text = ""
    latest_time: datetime | None = None
    recent: list[dict[str, str]] = []
    if traces:
        t0 = traces[0]
        latest_text = str(t0.get("context") or "").strip()
        latest_time = _parse_ts(t0.get("time"))
        for tr in traces[:3]:
            recent.append({"time": tr.get("time", ""), "context": tr.get("context", "")})

    signed_at: datetime | None = None
    if status_code == "signed":
        signed_at = latest_time or datetime.utcnow()

    return {
        "ok": True,
        "message": "",
        "subscribe_ok": False,
        "tracking_no": tn,
        "courier_code": cc,
        "status_code": status_code,
        "status_text": status_text,
        "latest_trace_text": latest_text,
        "latest_trace_time": latest_time,
        "recent_traces": recent,
        "signed_at": signed_at,
        "raw_payload": raw_payload,
    }
