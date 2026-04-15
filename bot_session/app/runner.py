import asyncio
import os
import socket
import contextlib
from dataclasses import dataclass, field
from typing import Any

import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

API_BASE = os.getenv("API_BASE", "http://backend:8000")
RUNNER_TYPE = os.getenv("RUNNER_TYPE", "session")
SYNC_SECONDS = max(5, int(os.getenv("BOT_ENABLED_SYNC_SECONDS", "5") or 5))
HEARTBEAT_SECONDS = max(10, int(os.getenv("BOT_RUNTIME_HEARTBEAT_SECONDS", "15") or 15))
SESSION_PUSH_SECONDS = max(3, int(os.getenv("SESSION_PUSH_SECONDS", "5") or 5))
INSTANCE_ID = os.getenv("HOSTNAME") or socket.gethostname()
TIMEOUT = 20
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "").strip()


def internal_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = dict(extra or {})
    if INTERNAL_API_TOKEN:
        headers["X-Internal-API-Token"] = INTERNAL_API_TOKEN
    return headers






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

HELP = (
    "聚合聊天机器人命令：\n"
    "/start 开启自动推送\n"
    "/mute 关闭自动推送\n"
    "/unread 查看未读会话\n"
    "/sessions 查看最近会话\n"
    "/cancel 取消当前回复\n\n"
    "说明：该机器人会聚合所有商城机器人的客户消息，并可直接回复客户。"
)


@dataclass
class RunningBot:
    bot_code: str
    token: str
    bot: Bot
    task: asyncio.Task
    heartbeat_task: asyncio.Task
    notify_task: asyncio.Task | None = None
    subscribers: set[int] = field(default_factory=set)
    last_seen: dict[int, tuple[int, str]] = field(default_factory=dict)


class ReplyForm(StatesGroup):
    waiting_text = State()


RUNNING_BOTS: dict[str, RunningBot] = {}


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _extract_session_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list):
            return items
        rows = data.get("rows")
        if isinstance(rows, list):
            return rows
    return []


async def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{API_BASE}{path}", params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(r.text[:300])
        return r.json()


async def api_post(path: str, payload: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(f"{API_BASE}{path}", json=payload or {}, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(r.text[:300])
        return r.json()


async def wait_backend(max_attempts: int = 40, delay_seconds: int = 2) -> bool:
    for i in range(max_attempts):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{API_BASE}/health")
                print(f"[session-runner] backend health: {r.status_code} {r.text}")
                return True
        except Exception as e:
            print(f"[session-runner] backend not ready ({i+1}/{max_attempts}): {e}")
            await asyncio.sleep(delay_seconds)
    return False




async def prepare_bot(bot: Bot, bot_code: str) -> None:
    try:
        me = await bot.get_me()
        print(f"[session-runner] bot ready: bot_code={bot_code} username=@{me.username}")
    except Exception as e:
        print(f"[session-runner] get_me failed for {bot_code}: {e}")
        raise
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print(f"[session-runner] deleteWebhook success: {bot_code}")
    except Exception as e:
        print(f"[session-runner] deleteWebhook failed for {bot_code}: {e}")

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
        print(f"[session-runner] runtime report failed for {bot_code}: {e}")


async def heartbeat_loop(bot_code: str):
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        await report_runtime(bot_code, "running", "已启动")


def session_list_kb(rows: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons = []
    for row in rows[:10]:
        title = f"#{row['id']} {row.get('bot_code') or '-'} | {row.get('display_name') or row.get('telegram_user_id')}"
        unread = int(row.get('unread_count') or 0)
        if unread:
            title += f" ({unread}未读)"
        buttons.append([InlineKeyboardButton(text=title[:64], callback_data=f"sess:{row['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="暂无会话", callback_data="noop")]])


def session_actions_kb(session_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="打开会话", callback_data=f"sess:{session_id}"), InlineKeyboardButton(text="回复客户", callback_data=f"sreply:{session_id}")],
        [InlineKeyboardButton(text="标记已读", callback_data=f"sread:{session_id}")],
    ])


def _notification_text(row: dict[str, Any]) -> str:
    message = _safe_str(row.get("last_message_text")) or f"[{_safe_str(row.get('last_message_type')) or 'text'}]"
    display_name = _safe_str(row.get("display_name")) or _safe_str(row.get("telegram_user_id"))
    return (
        "🔔 收到新客户消息\n"
        f"会话：#{row.get('id')}\n"
        f"买家Bot：{_safe_str(row.get('bot_code')) or '-'}\n"
        f"客户：{display_name}\n"
        f"用户ID：{_safe_str(row.get('telegram_user_id')) or '-'}\n\n"
        "客户发言：\n"
        f"{message[:1200]}"
    )


async def notification_loop(bot_code: str):
    while True:
        await asyncio.sleep(SESSION_PUSH_SECONDS)
        running = RUNNING_BOTS.get(bot_code)
        if not running or not running.subscribers:
            continue
        try:
            data = await api_get('/admin/chat-sessions', params={'status': 'all', 'only_unread': 'true', 'limit': 100, 'page': 1, 'page_size': 100})
            rows = _extract_session_rows(data)
            current_keys: dict[int, tuple[int, str]] = {}
            for row in rows:
                sid = int(row.get('id') or 0)
                unread = int(row.get('unread_count') or 0)
                last_customer_at = _safe_str(row.get('last_customer_message_at'))
                current_keys[sid] = (unread, last_customer_at)
                prev = running.last_seen.get(sid)
                if unread <= 0:
                    continue
                if prev == (unread, last_customer_at):
                    continue
                text = _notification_text(row)
                markup = session_actions_kb(sid)
                stale: set[int] = set()
                for chat_id in list(running.subscribers):
                    try:
                        await running.bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
                    except Exception as e:
                        print(f"[session-runner] notify failed chat_id={chat_id}: {e}")
                        if 'chat not found' in str(e).lower() or 'forbidden' in str(e).lower():
                            stale.add(chat_id)
                for chat_id in stale:
                    running.subscribers.discard(chat_id)
            running.last_seen = current_keys
        except Exception as e:
            print(f"[session-runner] notification loop error for {bot_code}: {e}")


def build_dispatcher(bot_code: str) -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())

    async def send_session_summary(message: Message, only_unread: bool = False):
        rows = await api_get('/admin/chat-sessions', params={'status': 'all', 'only_unread': str(only_unread).lower(), 'limit': 10})
        text = '未读会话：' if only_unread else '最近会话：'
        if not rows:
            await message.answer(text + '\n暂无数据')
            return
        await message.answer(text, reply_markup=session_list_kb(rows))

    async def send_session_detail(target: Message | CallbackQuery, session_id: int, mark_read: bool = False):
        data = await api_get(f'/admin/chat-sessions/{session_id}', params={'mark_read': str(mark_read).lower()})
        session = data.get('session') or {}
        messages = data.get('messages') or []
        lines = [
            f"会话 #{session.get('id')}",
            f"机器人：{session.get('bot_code') or '-'}",
            f"客户：{session.get('display_name') or session.get('telegram_user_id')}",
            f"状态：{session.get('session_status') or 'open'}",
            f"未读：{session.get('unread_count') or 0}",
            '',
            '最近消息：',
        ]
        for row in messages[-12:]:
            prefix = '客户' if row.get('direction') == 'customer' else '客服'
            content = row.get('content_text') or f"[{row.get('message_type')}]"
            lines.append(f"{prefix}｜{content}")
        msg = '\n'.join(lines)[:3900]
        if isinstance(target, CallbackQuery):
            await target.message.answer(msg, reply_markup=session_actions_kb(session_id))
            await target.answer()
        else:
            await target.answer(msg, reply_markup=session_actions_kb(session_id))

    @dp.message(Command('start'))
    async def start_cmd(message: Message):
        running = RUNNING_BOTS.get(bot_code)
        if running:
            running.subscribers.add(message.chat.id)
        await message.answer(f"聚合聊天机器人已连接（{bot_code}）。\n已开启当前窗口的自动推送。\n\n{HELP}")
        await maybe_send_startup_announcement(message.bot, bot_code, RUNNER_TYPE, message.chat.id, message.from_user.id)
        await send_folder_link_prompt(message, bot_code, RUNNER_TYPE)

    @dp.callback_query(F.data == 'folder_hint')
    async def folder_hint_cb(callback: CallbackQuery):
        runtime = await get_folder_link_runtime(bot_code, RUNNER_TYPE)
        text = str(runtime.get('manual_hint_text') or '已导入商城文件夹。机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。')
        await callback.answer(text[:180], show_alert=True)

    @dp.message(Command('mute'))
    async def mute_cmd(message: Message):
        running = RUNNING_BOTS.get(bot_code)
        if running:
            running.subscribers.discard(message.chat.id)
        await message.answer('已关闭当前窗口的自动推送。再次发送 /start 可重新开启。')

    @dp.message(Command('unread'))
    async def unread_cmd(message: Message):
        await send_session_summary(message, only_unread=True)

    @dp.message(Command('sessions'))
    async def sessions_cmd(message: Message):
        await send_session_summary(message, only_unread=False)

    @dp.message(Command('cancel'))
    async def cancel_cmd(message: Message, state: FSMContext):
        await state.clear()
        await message.answer('已取消当前回复。')

    @dp.callback_query(F.data == 'noop')
    async def noop(callback: CallbackQuery):
        await callback.answer()

    @dp.callback_query(F.data.startswith('sess:'))
    async def open_session(callback: CallbackQuery):
        session_id = int(callback.data.split(':', 1)[1])
        await send_session_detail(callback, session_id, mark_read=True)

    @dp.callback_query(F.data.startswith('sreply:'))
    async def begin_reply(callback: CallbackQuery, state: FSMContext):
        session_id = int(callback.data.split(':', 1)[1])
        await state.update_data(reply_session_id=session_id)
        await state.set_state(ReplyForm.waiting_text)
        await callback.message.answer(f'请发送要回复给会话 #{session_id} 的内容。发送 /cancel 可取消。')
        await callback.answer()

    @dp.callback_query(F.data.startswith('sread:'))
    async def mark_read(callback: CallbackQuery):
        session_id = int(callback.data.split(':', 1)[1])
        await api_post(f'/admin/chat-sessions/{session_id}/read', {})
        await callback.answer('已标记已读')
        await send_session_detail(callback, session_id, mark_read=False)

    @dp.message(ReplyForm.waiting_text)
    async def recv_reply(message: Message, state: FSMContext):
        data = await state.get_data()
        session_id = int(data.get('reply_session_id') or 0)
        if not session_id:
            await state.clear()
            await message.answer('会话已丢失，请重新打开。')
            return
        try:
            await api_post(f'/admin/chat-sessions/{session_id}/reply', {'text': message.text or '', 'operator_name': bot_code})
            await message.answer('已发送给客户。')
            await state.clear()
            await send_session_detail(message, session_id, mark_read=False)
        except Exception as e:
            await message.answer(f'发送失败：{e}')

    return dp


async def start_bot(bot_code: str, token: str):
    if bot_code in RUNNING_BOTS:
        return
    bot = Bot(token=token, default=DefaultBotProperties())
    dp = build_dispatcher(bot_code)

    async def runner_task():
        try:
            await report_runtime(bot_code, 'starting', '启动中')
            await prepare_bot(bot, bot_code)
            await report_runtime(bot_code, 'running', '已启动')
            await dp.start_polling(bot, polling_timeout=10, allowed_updates=dp.resolve_used_update_types())
        except asyncio.CancelledError:
            await report_runtime(bot_code, 'stopping', '停止中')
            raise
        except Exception as e:
            await report_runtime(bot_code, 'stopped', '异常退出', last_error=str(e))
            print(f'[session-runner] bot {bot_code} error: {e}')
        finally:
            with contextlib.suppress(Exception):
                await bot.session.close()
            await report_runtime(bot_code, 'stopped', '已停止')

    task = asyncio.create_task(runner_task(), name=f'session:{bot_code}')
    hb = asyncio.create_task(heartbeat_loop(bot_code), name=f'session-hb:{bot_code}')
    notify = asyncio.create_task(notification_loop(bot_code), name=f'session-notify:{bot_code}')
    RUNNING_BOTS[bot_code] = RunningBot(bot_code=bot_code, token=token, bot=bot, task=task, heartbeat_task=hb, notify_task=notify)


async def stop_bot(bot_code: str):
    running = RUNNING_BOTS.pop(bot_code, None)
    if not running:
        return
    if running.notify_task:
        running.notify_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running.notify_task
    running.heartbeat_task.cancel()
    with contextlib.suppress(Exception):
        await report_runtime(bot_code, 'stopping', '停止中')
    running.task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await running.task
    with contextlib.suppress(asyncio.CancelledError):
        await running.heartbeat_task


async def sync_loop():
    while True:
        try:
            rows = await api_get('/admin/bots/enabled_tokens', params={'bot_type': RUNNER_TYPE})
            used = {}
            desired = {}
            for row in rows:
                token = (row.get('bot_token') or '').strip()
                bot_code = row.get('bot_code')
                if not token or not bot_code:
                    continue
                if token in used:
                    await report_runtime(bot_code, 'stopped', f"重复 token，已跳过；与 {used[token]} 冲突", last_error='duplicate token', heartbeat=False)
                    continue
                used[token] = bot_code
                desired[bot_code] = token
            for bot_code in list(RUNNING_BOTS.keys()):
                if bot_code not in desired or RUNNING_BOTS[bot_code].token != desired[bot_code]:
                    await stop_bot(bot_code)
            for bot_code, token in desired.items():
                if bot_code not in RUNNING_BOTS:
                    await start_bot(bot_code, token)
        except Exception as e:
            print(f'[session-runner] sync loop error: {e}')
        await asyncio.sleep(SYNC_SECONDS)


async def main():
    print(f"[session-runner] startup. API_BASE={API_BASE} runner_type={RUNNER_TYPE} sync={SYNC_SECONDS}s heartbeat={HEARTBEAT_SECONDS}s push={SESSION_PUSH_SECONDS}s")
    ok = await wait_backend()
    if not ok:
        print('[session-runner] backend not ready, exit')
        return
    await sync_loop()


if __name__ == '__main__':
    asyncio.run(main())
