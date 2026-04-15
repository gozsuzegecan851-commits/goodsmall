import asyncio
import os
import socket
import contextlib
from dataclasses import dataclass
from typing import Any

import httpx
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, BufferedInputFile

API_BASE = os.getenv("API_BASE", "http://backend:8000")
RUNNER_TYPE = os.getenv("RUNNER_TYPE", "shipping")
SYNC_SECONDS = max(5, int(os.getenv("BOT_ENABLED_SYNC_SECONDS", "5") or 5))
HEARTBEAT_SECONDS = max(10, int(os.getenv("BOT_RUNTIME_HEARTBEAT_SECONDS", "15") or 15))
INSTANCE_ID = os.getenv("HOSTNAME") or socket.gethostname()
TIMEOUT = 20
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "").strip()


def internal_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = dict(extra or {})
    if INTERNAL_API_TOKEN:
        headers["X-Internal-API-Token"] = INTERNAL_API_TOKEN
    return headers


HELP = "供应链机器人命令：\n/pending_ship\n/ship_excel_today\n/ship_excel_yesterday\n/shipped_summary_yesterday\n/shipped_summary_today\n/missing_tracking\n/template_sample\n\n提示：供应链机器人必须绑定 supplier_code，仅能查看自己的供应链订单，导出的模板会按供应链类型自动分流。"

@dataclass
class RunningBot:
    bot_code: str
    token: str
    supplier_code: str
    bot: Bot
    task: asyncio.Task
    heartbeat_task: asyncio.Task

RUNNING_BOTS: dict[str, RunningBot] = {}

async def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{API_BASE}{path}", params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(r.text[:200])
        return r.json()

async def api_post(path: str, payload: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(f"{API_BASE}{path}", json=payload or {}, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(r.text[:200])
        return r.json()


async def api_get_bytes(path: str, params: dict[str, Any] | None = None) -> tuple[bytes, str]:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{API_BASE}{path}", params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(r.text[:200])
        dispo = r.headers.get("content-disposition", "")
        filename = "export.xlsx"
        if "filename=" in dispo:
            filename = dispo.split("filename=",1)[1].strip().strip('"')
        return r.content, filename

async def wait_backend(max_attempts: int = 40, delay_seconds: int = 2) -> bool:
    for i in range(max_attempts):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{API_BASE}/health")
                print(f"[shipping-runner] backend health: {r.status_code} {r.text}")
                return True
        except Exception as e:
            print(f"[shipping-runner] backend not ready ({i+1}/{max_attempts}): {e}")
            await asyncio.sleep(delay_seconds)
    return False

async def report_runtime(bot_code: str, run_status: str, status_text: str = "", last_error: str | None = None, heartbeat: bool = True):
    payload = {
        "bot_code": bot_code,
        "bot_type": RUNNER_TYPE,
        "run_status": run_status,
        "status_text": status_text,
        "instance_id": INSTANCE_ID,
        "heartbeat": heartbeat,
    }
    if last_error is not None:
        payload["last_error"] = last_error
    try:
        await api_post("/admin/bots/runtime/report", payload)
    except Exception as e:
        print(f"[shipping-runner] runtime report failed for {bot_code}: {e}")

async def heartbeat_loop(bot_code: str):
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        await report_runtime(bot_code, "running", "已启动")




async def prepare_bot(bot: Bot, bot_code: str) -> None:
    try:
        me = await bot.get_me()
        print(f"[shipping-runner] bot ready: bot_code={bot_code} username=@{me.username}")
    except Exception as e:
        print(f"[shipping-runner] get_me failed for {bot_code}: {e}")
        raise
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print(f"[shipping-runner] deleteWebhook success: {bot_code}")
    except Exception as e:
        print(f"[shipping-runner] deleteWebhook failed for {bot_code}: {e}")





FOLDER_LINK_RUNTIME_CACHE: dict[str, Any] = {"expires_at": 0.0, "data": None}

async def get_folder_link_runtime(bot_code: str, bot_type: str) -> dict:
    now = time.time()
    cached = FOLDER_LINK_RUNTIME_CACHE.get("data")
    if cached and float(FOLDER_LINK_RUNTIME_CACHE.get("expires_at") or 0) > now:
        return cached
    try:
        data = await api_get('/admin/folder-link/runtime', params={'bot_code': bot_code, 'bot_type': bot_type})
        if not isinstance(data, dict):
            data = {'enabled_for_bot': False}
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] load folder link runtime failed for {bot_code}: {e}")
        data = {'enabled_for_bot': False}
    FOLDER_LINK_RUNTIME_CACHE['data'] = data
    FOLDER_LINK_RUNTIME_CACHE['expires_at'] = now + 60
    return data


def build_folder_link_keyboard(data: dict | None):
    if not isinstance(data, dict):
        return None
    rows = []
    if bool(data.get('enabled_for_bot')) and str(data.get('folder_link_url') or '').strip():
        rows.append([InlineKeyboardButton(text=str(data.get('primary_button_text') or '添加到商城文件夹'), url=str(data.get('folder_link_url') or '').strip())])
    if bool(data.get('show_settings_button')) and str(data.get('settings_button_url') or '').strip():
        rows.append([InlineKeyboardButton(text=str(data.get('settings_button_text') or '打开文件夹设置'), url=str(data.get('settings_button_url') or '').strip())])
    if bool(data.get('show_manual_hint_button')):
        rows.append([InlineKeyboardButton(text=str(data.get('manual_hint_button_text') or '如何手动加入机器人'), callback_data='folder_hint')])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


async def send_folder_link_prompt(message: Message, bot_code: str, bot_type: str):
    data = await get_folder_link_runtime(bot_code, bot_type)
    markup = build_folder_link_keyboard(data)
    if not markup:
        return
    await message.answer('可使用下方按钮导入商城文件夹。机器人私聊仍需你在 Telegram 内手动加入文件夹或手动置顶。', reply_markup=markup)

async def maybe_send_startup_announcement(bot, bot_code: str, bot_type: str, chat_id: int | str, telegram_user_id: int | str):
    try:
        data = await api_get('/admin/announcements/startup', params={'bot_code': bot_code, 'telegram_chat_id': str(chat_id), 'telegram_user_id': str(telegram_user_id)})
        if not isinstance(data, dict) or not data.get('should_send'):
            return
        text = str(data.get('content_text') or '').strip()
        media_type = str(data.get('media_type') or 'none').strip().lower()
        media_url = str(data.get('media_url') or '').strip()
        if media_type == 'video' and media_url:
            await bot.send_video(chat_id=chat_id, video=media_url, caption=text[:1024] if text else None)
        elif text:
            await bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] startup announcement failed for {bot_code}: {e}")

def ensure_supplier_scope(supplier_code: str):
    if supplier_code:
        return
    raise RuntimeError("当前供应链机器人未绑定供应链 supplier_code，禁止查看全部订单。")

def build_dispatcher(bot_code: str, supplier_code: str = "") -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())

    @dp.message(Command("start"))
    async def start_cmd(message: Message):
        extra = f"\n绑定供应链：{supplier_code}" if supplier_code else "\n未绑定供应链（禁止查看订单，请在后台绑定供应链）"
        await message.answer(f"供应链机器人已连接（{bot_code}）。{extra}\n\n{HELP}")
        await maybe_send_startup_announcement(message.bot, bot_code, RUNNER_TYPE, message.chat.id, message.from_user.id)
        await send_folder_link_prompt(message, bot_code, RUNNER_TYPE)

    @dp.callback_query(F.data == 'folder_hint')
    async def folder_hint_cb(callback: CallbackQuery):
        runtime = await get_folder_link_runtime(bot_code, RUNNER_TYPE)
        text = str(runtime.get('manual_hint_text') or '已导入商城文件夹。机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。')
        await callback.answer(text[:180], show_alert=True)

    @dp.message(Command("pending_ship"))
    async def pending_ship(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data = await api_get('/admin/shipments/pending-summary', params={'supplier_code': supplier_code})
            lines = [f"供应链：{supplier_code}", f"待发货总数：{data.get('pending_shipment_count', 0)}"]
            by_supplier = data.get('by_supplier') or {}
            if by_supplier:
                lines.append('分供应链：')
                lines.extend([f"- {k}: {v}" for k, v in by_supplier.items()])
            await message.answer('\n'.join(lines))
        except Exception as e:
            await message.answer(f"查询失败：{e}")

    @dp.message(Command("shipped_summary_yesterday"))
    async def shipped_summary(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data = await api_get('/admin/shipments/shipped-summary', params={'supplier_code': supplier_code})
            lines = [f"供应链：{supplier_code}", f"已发货总数：{data.get('shipped_count', 0)}"]
            for k, v in (data.get('by_courier') or {}).items():
                lines.append(f"- {k}: {v}")
            await message.answer('\n'.join(lines))
        except Exception as e:
            await message.answer(f"查询失败：{e}")

    @dp.message(Command("shipped_summary_today"))
    async def shipped_summary_today(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data = await api_get('/admin/shipments/shipped-summary', params={'supplier_code': supplier_code})
            lines = [f"供应链：{supplier_code}", f"今日已发货总数：{data.get('shipped_count', 0)}"]
            for k, v in (data.get('by_courier') or {}).items():
                lines.append(f"- {k}: {v}")
            await message.answer('\n'.join(lines))
        except Exception as e:
            await message.answer(f"查询失败：{e}")

    @dp.message(Command("missing_tracking"))
    async def missing_tracking(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data = await api_get("/admin/shipments/pending-summary", params={"supplier_code": supplier_code})
            rows = data.get('rows') or []
            lines = [f"供应链：{supplier_code}", f"待发货/漏单号参考：{data.get('pending_shipment_count', 0)}单"]
            for row in rows[:8]:
                lines.append(f"- {row.get('order_no')} / {row.get('customer_name')} / {row.get('tracking_no') or '未录单号'}")
            await message.answer('\n'.join(lines))
        except Exception as e:
            await message.answer(f"查询失败：{e}")

    @dp.message(Command("ship_excel_today"))
    async def ship_excel_today(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data, filename = await api_get_bytes('/admin/shipments/export-pending', params={'supplier_code': supplier_code})
            await message.answer_document(BufferedInputFile(data, filename=filename), caption=f'供应链 {supplier_code} 待发货导出')
        except Exception as e:
            await message.answer(f"导出失败：{e}")

    @dp.message(Command("ship_excel_yesterday"))
    async def ship_excel_yesterday(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            data, filename = await api_get_bytes('/admin/shipments/export-shipped', params={'supplier_code': supplier_code})
            await message.answer_document(BufferedInputFile(data, filename=filename), caption=f'供应链 {supplier_code} 已发货导出')
        except Exception as e:
            await message.answer(f"导出失败：{e}")

    @dp.message(Command("template_sample"))
    async def template_sample(message: Message):
        try:
            ensure_supplier_scope(supplier_code)
            suppliers = await api_get('/admin/suppliers')
            supplier = next((x for x in suppliers if (x.get('supplier_code') or '').strip().upper() == supplier_code.strip().upper()), None)
            if not supplier:
                raise RuntimeError('未找到当前供应链配置')
            data, filename = await api_get_bytes(f"/admin/suppliers/{supplier['id']}/template-sample", params={'mode': 'pending'})
            await message.answer_document(BufferedInputFile(data, filename=filename), caption=f'供应链 {supplier_code} 模板样例')
        except Exception as e:
            await message.answer(f"获取样例失败：{e}")
    return dp

async def start_bot(bot_code: str, token: str, supplier_code: str = ""):
    if bot_code in RUNNING_BOTS:
        return
    bot = Bot(token=token, default=DefaultBotProperties())
    dp = build_dispatcher(bot_code, supplier_code)
    async def runner_task():
        try:
            await report_runtime(bot_code, "starting", "启动中")
            await prepare_bot(bot, bot_code)
            await report_runtime(bot_code, "running", "已启动")
            await dp.start_polling(bot, polling_timeout=10, allowed_updates=dp.resolve_used_update_types())
        except asyncio.CancelledError:
            await report_runtime(bot_code, "stopping", "停止中")
            raise
        except Exception as e:
            await report_runtime(bot_code, "stopped", "异常退出", last_error=str(e))
            print(f"[shipping-runner] bot {bot_code} error: {e}")
        finally:
            with contextlib.suppress(Exception):
                await bot.session.close()
            await report_runtime(bot_code, "stopped", "已停止")
    task = asyncio.create_task(runner_task(), name=f"shipping:{bot_code}")
    hb = asyncio.create_task(heartbeat_loop(bot_code), name=f"shipping-hb:{bot_code}")
    RUNNING_BOTS[bot_code] = RunningBot(bot_code=bot_code, token=token, supplier_code=supplier_code or "", bot=bot, task=task, heartbeat_task=hb)

async def stop_bot(bot_code: str):
    running = RUNNING_BOTS.pop(bot_code, None)
    if not running:
        return
    running.heartbeat_task.cancel()
    with contextlib.suppress(Exception):
        await report_runtime(bot_code, "stopping", "停止中")
    running.task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await running.task
    with contextlib.suppress(asyncio.CancelledError):
        await running.heartbeat_task

async def sync_loop():
    while True:
        try:
            rows = await api_get("/admin/bots/enabled_tokens", params={"bot_type": RUNNER_TYPE})
            used = {}
            desired = {}
            for row in rows:
                token = (row.get("bot_token") or "").strip()
                bot_code = row.get("bot_code")
                if not token or not bot_code:
                    continue
                if token in used:
                    await report_runtime(bot_code, "stopped", f"重复 token，已跳过；与 {used[token]} 冲突", last_error="duplicate token", heartbeat=False)
                    continue
                used[token] = bot_code
                desired[bot_code] = {"token": token, "supplier_code": (row.get("supplier_code") or "").strip()}
            for bot_code in list(RUNNING_BOTS.keys()):
                if bot_code not in desired or RUNNING_BOTS[bot_code].token != desired[bot_code]["token"] or RUNNING_BOTS[bot_code].supplier_code != desired[bot_code]["supplier_code"]:
                    await stop_bot(bot_code)
            for bot_code, conf in desired.items():
                if bot_code not in RUNNING_BOTS:
                    await start_bot(bot_code, conf["token"], conf["supplier_code"])
        except Exception as e:
            print(f"[shipping-runner] sync loop error: {e}")
        await asyncio.sleep(SYNC_SECONDS)

async def main():
    print(f"[shipping-runner] startup. API_BASE={API_BASE} runner_type={RUNNER_TYPE} sync={SYNC_SECONDS}s heartbeat={HEARTBEAT_SECONDS}s")
    ok = await wait_backend()
    if not ok:
        print("[shipping-runner] backend not ready, exit")
        return
    await sync_loop()

if __name__ == "__main__":
    asyncio.run(main())
