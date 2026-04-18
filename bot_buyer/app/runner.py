
import asyncio
import contextlib
import json
import os
import socket
import time
from dataclasses import dataclass
from html import escape
from typing import Any

import httpx
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, KeyboardButton, Message, ReplyKeyboardMarkup

API_BASE = os.getenv("API_BASE", "http://backend:8000")
RUNNER_TYPE = os.getenv("RUNNER_TYPE", "buyer")
SYNC_SECONDS = max(5, int(os.getenv("BOT_ENABLED_SYNC_SECONDS", "5") or 5))
HEARTBEAT_SECONDS = max(10, int(os.getenv("BOT_RUNTIME_HEARTBEAT_SECONDS", "15") or 15))
INSTANCE_ID = os.getenv("HOSTNAME") or socket.gethostname()
TIMEOUT = 20
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "").strip()

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
    enabled_for_bot = bool(data.get('enabled_for_bot')) and bool(str(data.get('folder_link_url') or '').strip())
    if enabled_for_bot:
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
    hint = '可使用下方按钮导入商城文件夹。机器人私聊仍需你在 Telegram 内手动加入文件夹或手动置顶。'
    await message.answer(hint, reply_markup=markup)


def internal_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = dict(extra or {})
    if INTERNAL_API_TOKEN:
        headers["X-Internal-API-Token"] = INTERNAL_API_TOKEN
    return headers


MENU_CATALOG = "🛍 商品分类"
MENU_ADDRESS = "📍 我的地址"
MENU_ORDERS = "📦 我的订单"
MENU_HELP = "💳 支付帮助"

class AddressForm(StatesGroup):
    waiting_template = State()

@dataclass
class RunningBot:
    bot_code: str
    bot_type: str
    token: str
    bot: Bot
    task: asyncio.Task
    heartbeat_task: asyncio.Task

RUNNING_BOTS: dict[str, RunningBot] = {}
START_WELCOME_TEXTS: dict[str, str] = {}


async def _post_chat_event_safely(payload: dict[str, Any]) -> None:
    try:
        await api_post('/admin/chat-events', payload)
    except Exception as e:
        print(f"[buyer-runner] chat capture failed: {e}")


async def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(f"{API_BASE}{path}", params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(parse_error(r))
        return r.json()

async def api_post(path: str, payload: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(f"{API_BASE}{path}", json=payload or {}, params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(parse_error(r))
        return r.json()

async def api_delete(path: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.delete(f"{API_BASE}{path}", params=params, headers=internal_headers())
        if r.status_code >= 400:
            raise RuntimeError(parse_error(r))
        return r.json()


def parse_error(response: httpx.Response) -> str:
    msg = f"HTTP {response.status_code}"
    try:
        data = response.json()
        detail = data.get("detail")
        if isinstance(detail, str) and detail:
            return detail
        if isinstance(detail, list) and detail:
            return "；".join(str(x.get("msg", x)) for x in detail)
    except Exception:
        pass
    return response.text[:200] if response.text else msg

async def wait_backend(max_attempts: int = 40, delay_seconds: int = 2) -> bool:
    for i in range(max_attempts):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{API_BASE}/health")
                print(f"[buyer-runner] backend health: {r.status_code} {r.text}")
                return True
        except Exception as e:
            print(f"[buyer-runner] backend not ready ({i+1}/{max_attempts}): {e}")
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
        print(f"[buyer-runner] runtime report failed for {bot_code}: {e}")

async def heartbeat_loop(bot_code: str):
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        await report_runtime(bot_code, "running", "已启动")




async def prepare_bot(bot: Bot, bot_code: str) -> None:
    try:
        me = await bot.get_me()
        print(f"[buyer-runner] bot ready: bot_code={bot_code} username=@{me.username}")
    except Exception as e:
        print(f"[buyer-runner] get_me failed for {bot_code}: {e}")
        raise
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        print(f"[buyer-runner] deleteWebhook success: {bot_code}")
    except Exception as e:
        print(f"[buyer-runner] deleteWebhook failed for {bot_code}: {e}")



async def confirm_startup_announcement_receipt(bot_code: str, chat_id: int | str, telegram_user_id: int | str, bot_type: str):
    try:
        await api_post('/admin/announcements/receipt', {
            'scene': 'startup',
            'bot_code': bot_code,
            'telegram_chat_id': str(chat_id),
            'telegram_user_id': str(telegram_user_id),
            'target_bot_type': bot_type,
        })
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] startup announcement receipt failed for {bot_code}: {e}")


async def update_startup_announcement_media_cache(file_rows: list[dict[str, str]]):
    if not file_rows:
        return
    try:
        await api_post('/admin/announcements/media-cache', {
            'scene': 'startup',
            'media_file_ids': file_rows,
        })
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] startup announcement media-cache update failed: {e}")


def _pick_media_entry(media_items: list[dict[str, Any]], media_cache: list[dict[str, Any]], idx: int) -> dict[str, Any]:
    cache_row = media_cache[idx] if idx < len(media_cache) and isinstance(media_cache[idx], dict) else {}
    item_row = media_items[idx] if idx < len(media_items) and isinstance(media_items[idx], dict) else {}
    return {
        'sort': int(cache_row.get('sort') or item_row.get('sort') or (idx + 1)),
        'telegram_file_id': str(cache_row.get('telegram_file_id') or '').strip(),
        'normalized_url': str(cache_row.get('normalized_url') or item_row.get('normalized_url') or item_row.get('url') or '').strip(),
        'source_url': str(cache_row.get('source_url') or item_row.get('source_url') or item_row.get('url') or '').strip(),
        'url': str(item_row.get('url') or cache_row.get('normalized_url') or cache_row.get('source_url') or '').strip(),
    }


async def maybe_send_startup_announcement(bot, bot_code: str, bot_type: str, chat_id: int | str, telegram_user_id: int | str):
    try:
        data = await api_get('/admin/announcements/startup', params={'bot_code': bot_code, 'telegram_chat_id': str(chat_id), 'telegram_user_id': str(telegram_user_id)})
        if not isinstance(data, dict):
            print(f"[{RUNNER_TYPE}-runner] startup announcement invalid payload for {bot_code}: {data!r}")
            return {'should_send': False, 'sent': False, 'replace_start_welcome': False}
        if not data.get('should_send'):
            print(f"[{RUNNER_TYPE}-runner] startup announcement skipped for {bot_code}: should_send=false")
            return {'should_send': False, 'sent': False, 'replace_start_welcome': False}
        text = str(data.get('content_text') or '').strip()
        media_mode = str(data.get('media_mode') or 'none').strip().lower()
        media_items = data.get('media_items') if isinstance(data.get('media_items'), list) else []
        media_cache = data.get('media_cache') if isinstance(data.get('media_cache'), list) else []
        replace_start_welcome = bool(data.get('replace_start_welcome', True))
        fallback_mode = str(data.get('fallback_mode') or 'text_only').strip().lower()
        print(f"[{RUNNER_TYPE}-runner] startup announcement begin bot={bot_code} chat_id={chat_id} media_mode={media_mode} items={len(media_items)}")
        sent = False
        if media_mode == 'video_album' and len(media_items) >= 2:
            medias = []
            used_sorts: list[int] = []
            for idx in range(min(len(media_items), 4)):
                entry = _pick_media_entry(media_items, media_cache, idx)
                used_sorts.append(int(entry.get('sort') or (idx + 1)))
                file_id = str(entry.get('telegram_file_id') or '').strip()
                if file_id:
                    medias.append(InputMediaVideo(media=file_id, caption=text[:1024] if idx == 0 and text else None))
                    continue
                src_url = str(entry.get('normalized_url') or entry.get('source_url') or entry.get('url') or '').strip()
                file_data = await fetch_file_bytes(src_url)
                if not file_data:
                    raise RuntimeError(f'相册组视频 {idx+1} 下载失败')
                data_bytes, name = file_data
                medias.append(InputMediaVideo(media=BufferedInputFile(data_bytes, filename=name), caption=text[:1024] if idx == 0 and text else None))
            results = await bot.send_media_group(chat_id=chat_id, media=medias)
            cache_rows = []
            for idx, msg in enumerate(results or []):
                video = getattr(msg, 'video', None)
                if video and getattr(video, 'file_id', None):
                    cache_rows.append({
                        'sort': used_sorts[idx] if idx < len(used_sorts) else (idx + 1),
                        'telegram_file_id': str(video.file_id),
                        'telegram_unique_id': str(getattr(video, 'file_unique_id', '') or ''),
                    })
            if cache_rows:
                await update_startup_announcement_media_cache(cache_rows)
            print(f"[{RUNNER_TYPE}-runner] startup announcement album sent bot={bot_code} count={len(medias)}")
            sent = True
        elif media_mode in {'single_video', 'video_album'} and media_items:
            entry = _pick_media_entry(media_items, media_cache, 0)
            file_id = str(entry.get('telegram_file_id') or '').strip()
            if file_id:
                await bot.send_video(chat_id=chat_id, video=file_id, caption=text[:1024] if text else None)
                sent = True
            else:
                first_url = str(entry.get('normalized_url') or entry.get('source_url') or entry.get('url') or '').strip()
                file_data = await fetch_file_bytes(first_url)
                if file_data:
                    data_bytes, name = file_data
                    result = await bot.send_video(chat_id=chat_id, video=BufferedInputFile(data_bytes, filename=name), caption=text[:1024] if text else None)
                    video = getattr(result, 'video', None)
                    if video and getattr(video, 'file_id', None):
                        await update_startup_announcement_media_cache([{'sort': 1, 'telegram_file_id': str(video.file_id), 'telegram_unique_id': str(getattr(video, 'file_unique_id', '') or '')}])
                    sent = True
            if sent:
                print(f"[{RUNNER_TYPE}-runner] startup announcement single video sent bot={bot_code}")
        elif text:
            await bot.send_message(chat_id=chat_id, text=text)
            print(f"[{RUNNER_TYPE}-runner] startup announcement text sent bot={bot_code}")
            sent = True

        if not sent and fallback_mode == 'single_video_first_item' and media_items:
            entry = _pick_media_entry(media_items, media_cache, 0)
            first_url = str(entry.get('normalized_url') or entry.get('source_url') or entry.get('url') or '').strip()
            file_data = await fetch_file_bytes(first_url)
            if file_data:
                data_bytes, name = file_data
                await bot.send_video(chat_id=chat_id, video=BufferedInputFile(data_bytes, filename=name), caption=text[:1024] if text else None)
                print(f"[{RUNNER_TYPE}-runner] startup announcement fallback single video sent bot={bot_code}")
                sent = True
        if not sent and fallback_mode == 'text_only' and text:
            await bot.send_message(chat_id=chat_id, text=text)
            print(f"[{RUNNER_TYPE}-runner] startup announcement fallback text sent bot={bot_code}")
            sent = True

        if sent:
            await confirm_startup_announcement_receipt(bot_code, chat_id, telegram_user_id, bot_type)
        return {'should_send': True, 'sent': sent, 'replace_start_welcome': replace_start_welcome}
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] startup announcement failed for {bot_code}: {e}")
        return {'should_send': True, 'sent': False, 'replace_start_welcome': False}


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_CATALOG), KeyboardButton(text=MENU_ADDRESS)],
            [KeyboardButton(text=MENU_ORDERS), KeyboardButton(text=MENU_HELP)],
        ],
        resize_keyboard=True,
    )

async def fetch_file_bytes(url: str) -> tuple[bytes, str] | None:
    if not url:
        print(f"[{RUNNER_TYPE}-runner] fetch_file_bytes skipped: empty url")
        return None
    internal_url = url
    if url.startswith("http://localhost:8002"):
        internal_url = url.replace("http://localhost:8002", API_BASE, 1)
    elif url.startswith("http://127.0.0.1:8002"):
        internal_url = url.replace("http://127.0.0.1:8002", API_BASE, 1)
    elif url.startswith("http://localhost:8001"):
        internal_url = url.replace("http://localhost:8001", API_BASE, 1)
    elif url.startswith("http://127.0.0.1:8001"):
        internal_url = url.replace("http://127.0.0.1:8001", API_BASE, 1)
    elif url.startswith("/"):
        internal_url = f"{API_BASE}{url}"
    print(f"[{RUNNER_TYPE}-runner] fetch_file_bytes url={url} internal={internal_url}")
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(internal_url)
        print(f"[{RUNNER_TYPE}-runner] fetch_file_bytes status={r.status_code} internal={internal_url}")
        if r.status_code >= 400:
            return None
        name = url.split("/")[-1] or "announcement_video.mp4"
        return r.content, name


def category_kb(categories: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=c["name"], callback_data=f"cat:{c['id']}")] for c in categories]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def cn_pay_status(value: str | None) -> str:
    return {"pending": "待支付", "paid": "已支付", "failed": "失败"}.get(value or "pending", value or "待支付")

def cn_delivery_status(value: str | None) -> str:
    return {"not_shipped": "待发货", "shipped": "运输中", "signed": "已签收"}.get(value or "not_shipped", value or "待发货")

def short_txid(value: str | None) -> str:
    if not value:
        return ""
    value = str(value)
    return value if len(value) <= 18 else f"{value[:8]}...{value[-8:]}"

def human_time(value: str | None) -> str:
    if not value:
        return "-"
    return str(value).replace("T", " ")[:19]


def parse_gallery_images(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw or '').strip()
    if not text:
        return []
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception as e:
        print(f"[{RUNNER_TYPE}-runner] parse_gallery_images failed: {e}; raw={text[:200]}")
    return []

def normalize_sku_list(product: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = product.get("sku_list") if isinstance(product, dict) else []
    if not isinstance(rows, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("is_active") is False:
            continue
        cleaned.append(row)
    cleaned.sort(key=lambda x: (int(x.get("sort_order") or 100), int(x.get("id") or 0)))
    return cleaned


def sku_display_name(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return "默认规格"
    return str(row.get("sku_name") or row.get("sku_code") or "默认规格").strip() or "默认规格"


def pick_sku(product: dict[str, Any], selected_sku_id: int | None = None) -> dict[str, Any] | None:
    rows = normalize_sku_list(product)
    if not rows:
        return None
    if selected_sku_id is not None:
        for row in rows:
            try:
                if int(row.get("id") or 0) == int(selected_sku_id):
                    return row
            except Exception:
                continue
    return rows[0]


def build_sku_lines(product: dict[str, Any]) -> str:
    rows = normalize_sku_list(product)
    if not rows:
        return ""
    lines: list[str] = []
    for idx, row in enumerate(rows[:8], 1):
        name = sku_display_name(row)
        price = str(row.get("price_cny") or "0").strip()
        stock = int(row.get("stock_qty") or 0)
        unit_text = str(row.get("unit_text") or "件").strip() or "件"
        spec_text = str(row.get("spec_text") or "").strip()
        line = f"{idx}. {name}｜¥{price}｜库存 {stock}{unit_text}"
        if spec_text:
            line += f"｜{spec_text}"
        lines.append(line)
    return "\n".join(lines)


def sku_button_label(row: dict[str, Any]) -> str:
    return f"{sku_display_name(row)}｜¥{str(row.get('price_cny') or '0').strip()}"


def products_kb(products: list[dict[str, Any]], category_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"{p['name']} · ¥{p['price_cny']}", callback_data=f"prod:{p['id']}")] for p in products]
    rows.append([InlineKeyboardButton(text="🔙 返回分类", callback_data="show_categories")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def product_detail_kb(product: dict[str, Any], selected_sku_id: int | None = None) -> InlineKeyboardMarkup:
    product_id = int(product.get("id") or 0)
    category_id = product.get("category_id")
    back = f"cat:{category_id}" if category_id else "show_categories"
    skus = normalize_sku_list(product)
    selected = pick_sku(product, selected_sku_id)
    rows: list[list[InlineKeyboardButton]] = []

    if len(skus) > 1:
        for row in skus[:8]:
            sid = int(row.get("id") or 0)
            checked = "✅ " if selected and int(selected.get("id") or 0) == sid else "▫️ "
            rows.append([
                InlineKeyboardButton(
                    text=checked + sku_button_label(row),
                    callback_data=f"sku:{product_id}:{sid}",
                )
            ])
        if selected is not None:
            sid = int(selected.get("id") or 0)
            rows.append([InlineKeyboardButton(text="🛒 立即下单（1件）", callback_data=f"buy:{product_id}:{sid}")])
            rows.append([InlineKeyboardButton(text="📍 选择地址下单", callback_data=f"buyaddrlist:{product_id}:{sid}")])
        rows.append([InlineKeyboardButton(text="🔙 返回商品列表", callback_data=back)])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if selected is not None:
        sid = int(selected.get("id") or 0)
        if sid > 0:
            rows.append([InlineKeyboardButton(text="🛒 立即下单（1件）", callback_data=f"buy:{product_id}:{sid}")])
            rows.append([InlineKeyboardButton(text="📍 选择地址下单", callback_data=f"buyaddrlist:{product_id}:{sid}")])
        else:
            rows.append([InlineKeyboardButton(text="🛒 立即下单（1件）", callback_data=f"buy:{product_id}")])
            rows.append([InlineKeyboardButton(text="📍 选择地址下单", callback_data=f"buyaddrlist:{product_id}")])
    else:
        rows.append([InlineKeyboardButton(text="🛒 立即下单（1件）", callback_data=f"buy:{product_id}")])
        rows.append([InlineKeyboardButton(text="📍 选择地址下单", callback_data=f"buyaddrlist:{product_id}")])
    rows.append([InlineKeyboardButton(text="🔙 返回商品列表", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def order_actions_kb(order_id: int, pending_pay: bool, show_logistics: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if pending_pay:
        rows.append([
            InlineKeyboardButton(text="💳 生成/查看USDT支付页", callback_data=f"pay:{order_id}"),
            InlineKeyboardButton(text="🔄 刷新支付状态", callback_data=f"payrefresh:{order_id}"),
        ])
    else:
        rows.append([InlineKeyboardButton(text="✅ 已确认支付", callback_data=f"paidhint:{order_id}")])
    if show_logistics:
        rows.append([InlineKeyboardButton(text="📍 物流查询", callback_data=f"otrace:{order_id}")])
    rows.append([InlineKeyboardButton(text="📦 返回我的订单", callback_data="orders:list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)




class ConversationCaptureMiddleware(BaseMiddleware):
    def __init__(self, bot_code: str):
        self.bot_code = bot_code

    async def __call__(self, handler, event, data):
        try:
            user = getattr(event, "from_user", None)
            chat = getattr(event, "chat", None)
            if user and chat:
                message_type = getattr(event, "content_type", None)
                message_type = str(message_type) if message_type else "text"
                content_text = (getattr(event, "text", None) or getattr(event, "caption", None) or "").strip()
                extra = {}
                if getattr(event, "photo", None):
                    extra["photo_count"] = len(event.photo or [])
                if getattr(event, "document", None):
                    extra["document_name"] = getattr(event.document, "file_name", "") or ""
                if getattr(event, "voice", None):
                    extra["voice"] = True
                if getattr(event, "video", None):
                    extra["video"] = True
                payload = {
                    'bot_code': self.bot_code,
                    'telegram_user_id': str(user.id),
                    'telegram_chat_id': str(chat.id),
                    'telegram_username': getattr(user, 'username', '') or '',
                    'telegram_first_name': getattr(user, 'first_name', '') or '',
                    'telegram_last_name': getattr(user, 'last_name', '') or '',
                    'sender_name': ' '.join(x for x in [getattr(user, 'first_name', '') or '', getattr(user, 'last_name', '') or ''] if x).strip() or getattr(user, 'username', '') or str(user.id),
                    'direction': 'customer',
                    'message_type': message_type,
                    'content_text': content_text,
                    'content_json': extra,
                    'telegram_message_id': str(getattr(event, 'message_id', '') or ''),
                }
                asyncio.create_task(_post_chat_event_safely(payload))
        except Exception as e:
            print(f"[buyer-runner] chat capture enqueue failed: {e}")
        return await handler(event, data)

def get_start_welcome_text(bot_code: str) -> str:
    value = str(START_WELCOME_TEXTS.get(bot_code) or '').strip()
    if value:
        return value
    return f"欢迎使用实货商城机器人（{bot_code}）。\n请选择功能："


def build_dispatcher(bot_code: str) -> Dispatcher:
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.message.outer_middleware(ConversationCaptureMiddleware(bot_code))

    async def get_addresses(user_id: int) -> list[dict[str, Any]]:
        return await api_get("/addresses", params={"telegram_user_id": str(user_id)})

    def address_line(addr: dict[str, Any]) -> str:
        full = " ".join(x for x in [addr.get('province'), addr.get('city'), addr.get('district'), addr.get('address_detail')] if x)
        return f"{addr.get('receiver_name')} / {addr.get('receiver_phone')}\n{full}"

    async def show_categories(target: Message | CallbackQuery):
        categories = await api_get("/catalog/categories")
        text = "请选择商品分类：" if categories else "当前还没有启用中的分类。"
        if isinstance(target, CallbackQuery):
            await target.message.answer(text, reply_markup=category_kb(categories) if categories else None)
            await target.answer()
        else:
            await target.answer(text, reply_markup=main_menu())
            if categories:
                await target.answer("分类列表如下：", reply_markup=category_kb(categories))

    async def show_products(callback: CallbackQuery, category_id: int):
        products = await api_get("/catalog/products", params={"category_id": category_id})
        if not products:
            await callback.answer("该分类暂无可售商品", show_alert=True)
            return
        await callback.message.answer("请选择商品：", reply_markup=products_kb(products, category_id))
        await callback.answer()

    async def send_product_detail(chat: Message, product: dict[str, Any], selected_sku_id: int | None = None):
        print(f"[{RUNNER_TYPE}-runner] send_product_detail product_id={product.get('id')} selected_sku_id={selected_sku_id}")
        skus = normalize_sku_list(product)
        selected_sku = pick_sku(product, selected_sku_id)
        has_multi_sku = len(skus) > 1

        if selected_sku is not None:
            current_price = str(selected_sku.get('price_cny') or product.get('price_cny') or '0')
            current_stock = int(selected_sku.get('stock_qty') or 0)
            current_unit = str(selected_sku.get('unit_text') or product.get('unit_text') or '件').strip() or '件'
            current_sku_code = str(selected_sku.get('sku_code') or product.get('sku_code') or '-')
            selected_line = (
                f"已选规格：<b>{escape(sku_display_name(selected_sku))}</b>\n"
                f"价格：<b>¥ {escape(current_price)}</b>\n"
                f"库存：{escape(str(current_stock))} {escape(current_unit)}\n"
                f"SKU：{escape(current_sku_code)}"
            )
        else:
            selected_line = (
                f"价格：<b>¥ {escape(str(product.get('price_cny') or '0'))}</b>\n"
                f"库存：{escape(str(product.get('stock_qty', 0)))} {escape(product.get('unit_text') or '件')}\n"
                f"SKU：{escape(product.get('sku_code') or '-')}"
            )

        sku_lines = build_sku_lines(product)
        if has_multi_sku:
            if selected_sku_id is None:
                sku_hint = f"\n\n<b>请选择规格</b>\n{escape(sku_lines)}"
            else:
                sku_hint = f"\n\n<b>可选规格</b>\n{escape(sku_lines)}"
        else:
            sku_hint = f"\n\n<b>可选规格</b>\n{escape(sku_lines)}" if sku_lines else ""

        text = (
            f"<b>{escape(product['name'])}</b>\n"
            f"{escape(product.get('subtitle') or '暂无副标题')}\n\n"
            f"{selected_line}"
            f"{sku_hint}\n\n"
            f"{escape(product.get('description') or '暂无商品描述')}"
        )
        markup = product_detail_kb(product, selected_sku_id)
        cover_url = str(product.get('cover_image') or '').strip()
        gallery_urls = parse_gallery_images(product.get('gallery_images_json'))
        print(f"[{RUNNER_TYPE}-runner] send_product_detail product_id={product.get('id')} gallery_count={len(gallery_urls)} sku_count={len(skus)}")

        image_urls: list[str] = []
        for url in [cover_url, *gallery_urls]:
            value = str(url or '').strip()
            if value and value not in image_urls:
                image_urls.append(value)

        if not image_urls:
            await chat.answer(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            return

        if len(image_urls) == 1:
            first = await fetch_file_bytes(image_urls[0])
            if first:
                data, name = first
                await chat.answer_photo(
                    BufferedInputFile(data, filename=name),
                    caption=text[:1024],
                    parse_mode=ParseMode.HTML,
                )
            else:
                await chat.answer(text, parse_mode=ParseMode.HTML)
            await chat.answer("请选择操作：", reply_markup=markup)
            return

        group_urls = image_urls[:4]
        media = []
        for idx, url in enumerate(group_urls):
            file_data = await fetch_file_bytes(url)
            if not file_data:
                continue
            data_bytes, name = file_data
            if idx == 0:
                media.append(
                    InputMediaPhoto(
                        media=BufferedInputFile(data_bytes, filename=name),
                        caption=text[:1024],
                        parse_mode=ParseMode.HTML,
                    )
                )
            else:
                media.append(InputMediaPhoto(media=BufferedInputFile(data_bytes, filename=name)))

        if media:
            print(f"[{RUNNER_TYPE}-runner] send_product_detail media_group_count={len(media)} product_id={product.get('id')}")
            await chat.answer_media_group(media)
            await chat.answer("请选择操作：", reply_markup=markup)
            return

        await chat.answer(text, parse_mode=ParseMode.HTML, reply_markup=markup)

    def resolve_selected_sku(product: dict[str, Any], sku_id: int | None) -> dict[str, Any] | None:
        sku = pick_sku(product, sku_id)
        if not sku:
            return None
        if sku.get("is_active") is False:
            return None
        if int(sku.get("stock_qty") or 0) <= 0:
            return None
        return sku

    async def show_addresses(message: Message, user_id: int):
        rows = await get_addresses(user_id)
        if not rows:
            await message.answer(
                "你还没有收货地址。\n\n发送地址模板：\n收件人\n手机号\n省\n市\n区\n详细地址\n邮编（可空）",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ 立即添加地址", callback_data="addr:add")]]),
            )
            return
        text_parts = ["你的收货地址："]
        buttons = []
        for addr in rows:
            flag = "【默认】" if addr.get("is_default") else ""
            text_parts.append(f"\n{flag} ID {addr['id']}\n{address_line(addr)}")
            btn_row = []
            if not addr.get("is_default"):
                btn_row.append(InlineKeyboardButton(text=f"设为默认 #{addr['id']}", callback_data=f"addr:def:{addr['id']}"))
            btn_row.append(InlineKeyboardButton(text=f"删除 #{addr['id']}", callback_data=f"addr:del:{addr['id']}"))
            buttons.append(btn_row)
        buttons.append([InlineKeyboardButton(text="➕ 新增地址", callback_data="addr:add")])
        await message.answer("\n".join(text_parts), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

    async def begin_address_input(target: Message | CallbackQuery, state: FSMContext):
        await state.set_state(AddressForm.waiting_template)
        text = (
            "请按下面模板发送地址，一行一个字段：\n\n"
            "收件人\n手机号\n省\n市\n区\n详细地址\n邮编（可空）\n\n"
            "例如：\n张三\n13800138000\n广东省\n深圳市\n南山区\n科技园一路 88 号\n518000\n\n"
            "发送 /cancel 可取消。"
        )
        if isinstance(target, CallbackQuery):
            await target.message.answer(text)
            await target.answer()
        else:
            await target.answer(text)

    async def create_order_and_payment(message: Message, telegram_user_id: int | str, product_id: int, address_id: int, sku: dict[str, Any] | None = None):
        user_id = str(telegram_user_id)
        item_payload: dict[str, Any] = {"product_id": product_id, "qty": 1}
        buyer_remark = ""
        if isinstance(sku, dict):
            sku_id = int(sku.get("id") or 0)
            if sku_id > 0:
                item_payload["sku_id"] = sku_id
            buyer_remark = f"规格：{sku_display_name(sku)}｜SKU:{str(sku.get('sku_code') or '').strip()}｜价格:¥{str(sku.get('price_cny') or '0').strip()}"
        payload = {
            "bot_code": bot_code,
            "telegram_user_id": user_id,
            "address_id": address_id,
            "items": [item_payload],
            "buyer_remark": buyer_remark,
        }
        print(f"[buyer-runner] create_order payload={payload}")
        try:
            order = await api_post("/orders/create", payload)
        except Exception as e:
            print(f"[buyer-runner] create_order failed user_id={user_id} product_id={product_id} address_id={address_id} item_payload={item_payload} err={e}")
            raise
        payment = await api_post("/payments/usdt/create", {"order_id": order["order_id"], "telegram_user_id": str(message.chat.id if getattr(message, "chat", None) else telegram_user_id)})
        await send_payment_page(message, order["order_id"], order["order_no"], payment)

    async def send_payment_page(message: Message, order_id: int, order_no: str, payment: dict[str, Any]):
        text = (
            f"<b>订单已创建</b>\n订单号：<code>{escape(order_no)}</code>\n\n"
            f"支付方式：USDT-TRC20\n"
            f"收款标签：{escape(payment.get('address_label') or '-')}\n"
            f"金额：<b>{escape(str(payment['expected_amount']))} USDT</b>\n"
            f"收款地址：<code>{escape(payment['receive_address'])}</code>\n"
            f"到期时间：{escape(payment.get('expired_at') or '-')}\n\n"
            f"支付后点击下方按钮刷新状态。"
        )
        markup = order_actions_kb(order_id, pending_pay=True)
        photo = await fetch_file_bytes(payment.get("qr_image") or "")
        if photo:
            data, name = photo
            await message.answer_photo(BufferedInputFile(data, filename=name), caption=text, parse_mode=ParseMode.HTML, reply_markup=markup)
        else:
            await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=markup)

    async def show_orders(message: Message, user_id: int):
        rows = await api_get("/orders", params={"telegram_user_id": str(user_id)})
        if not rows:
            await message.answer("你还没有订单。")
            return
        buttons = []
        text = ["最近订单："]
        for row in rows[:10]:
            text.append(f"\n{row['order_no']} | ¥{row['payable_amount']} | 支付:{row['pay_status']} | 发货:{row['delivery_status']}")
            buttons.append([InlineKeyboardButton(text=f"查看 {row['order_no']}", callback_data=f"order:{row['id']}")])
        await message.answer("\n".join(text), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

    async def show_order_detail(target: Message | CallbackQuery, order_id: int, user_id: int):
        order = await api_get(f"/orders/{order_id}", params={"telegram_user_id": str(user_id)})
        lines = [
            f"<b>订单号：</b><code>{escape(order['order_no'])}</code>",
            f"<b>金额：</b>¥ {escape(str(order['payable_amount']))}",
            f"<b>支付状态：</b>{escape(order.get('pay_status_text') or cn_pay_status(order.get('pay_status')))}",
            f"<b>订单状态：</b>{escape(order.get('order_status') or '-')}",
            f"<b>发货状态：</b>{escape(order.get('delivery_status_text') or cn_delivery_status(order.get('delivery_status')))}",
        ]
        if order.get("paid_at"):
            lines.append(f"<b>支付时间：</b>{escape(human_time(order.get('paid_at')))}")
        payment = order.get("payment") or {}
        if payment:
            lines.append(f"<b>支付单状态：</b>{escape(payment.get('confirm_status_text') or payment.get('confirm_status') or '待支付')}")
            if payment.get("txid"):
                lines.append(f"<b>链上交易：</b><code>{escape(short_txid(payment.get('txid')))}</code>")
            lines.append(f"<b>收款地址：</b><code>{escape(payment.get('receive_address') or '-')}</code>")
        if order.get("pay_status") == "paid":
            lines.append("<b>支付结果：</b>已确认，订单已进入待发货或发货流程")
        if order.get("customer_name") or order.get("customer_phone"):
            lines.append(f"<b>收件人：</b>{escape(order.get('customer_name') or '-')}")
            lines.append(f"<b>手机号：</b>{escape(order.get('customer_phone') or '-')}")
            address = " ".join([str(order.get('province') or ''), str(order.get('city') or ''), str(order.get('district') or ''), str(order.get('address_detail') or '')]).strip()
            lines.append(f"<b>地址：</b>{escape(address or '-')}")
        shipment = order.get("shipment") or {}
        if shipment or order.get("courier_company") or order.get("tracking_no"):
            lines.append("\n<b>发货信息</b>")
            lines.append(f"<b>快递公司：</b>{escape((shipment.get('courier_company') or order.get('courier_company') or '-'))}")
            lines.append(f"<b>快递单号：</b><code>{escape((shipment.get('tracking_no') or order.get('tracking_no') or '-'))}</code>")
            lines.append(f"<b>发货状态：</b>{escape(shipment.get('ship_status_text') or order.get('delivery_status_text') or cn_delivery_status(order.get('delivery_status')))}")
            if order.get('shipped_at'):
                lines.append(f"<b>发货时间：</b>{escape(human_time(order.get('shipped_at')))}")
            if shipment.get('last_trace_text'):
                lines.append(f"<b>最新物流：</b>{escape(shipment.get('last_trace_text') or '-')}")
        items = order.get("items") or []
        if items:
            lines.append("\n<b>商品明细</b>")
            for item in items:
                lines.append(f"- {escape(item['product_name'])} × {escape(str(item['qty']))}  ¥{escape(item['subtotal'])}")
        traces = order.get("traces") or []
        if traces:
            lines.append("\n<b>最近物流轨迹</b>")
            for tr in traces[:3]:
                lines.append(f"- {escape(human_time(tr.get('trace_time')))} {escape(tr.get('trace_text') or '')}")
        msg = "\n".join(lines)
        shipment = order.get("shipment") or {}
        track_no = str(shipment.get("tracking_no") or order.get("tracking_no") or "").strip()
        markup = order_actions_kb(
            order_id,
            pending_pay=order.get("pay_status") != "paid",
            show_logistics=bool(track_no),
        )
        if isinstance(target, CallbackQuery):
            await target.message.answer(msg, parse_mode=ParseMode.HTML, reply_markup=markup)
            await target.answer()
        else:
            await target.answer(msg, parse_mode=ParseMode.HTML, reply_markup=markup)

    @dp.message(Command("start"))
    async def start_cmd(message: Message, state: FSMContext):
        await state.clear()
        ann = await maybe_send_startup_announcement(message.bot, bot_code, RUNNER_TYPE, message.chat.id, message.from_user.id)
        if not ann.get('should_send') or not ann.get('replace_start_welcome') or not ann.get('sent'):
            await message.answer(get_start_welcome_text(bot_code), reply_markup=main_menu())
            if ann.get('should_send') and not ann.get('sent'):
                await maybe_send_startup_announcement(message.bot, bot_code, RUNNER_TYPE, message.chat.id, message.from_user.id)
        else:
            await message.answer('请选择下方菜单开始浏览。', reply_markup=main_menu())
        await send_folder_link_prompt(message, bot_code, RUNNER_TYPE)

    @dp.message(Command("cancel"))
    async def cancel_cmd(message: Message, state: FSMContext):
        await state.clear()
        await message.answer("已取消当前输入。", reply_markup=main_menu())

    @dp.message(F.text == MENU_CATALOG)
    async def menu_catalog(message: Message):
        await show_categories(message)

    @dp.message(F.text == MENU_ADDRESS)
    async def menu_address(message: Message):
        await show_addresses(message, message.from_user.id)

    @dp.message(F.text == MENU_ORDERS)
    async def menu_orders(message: Message):
        await show_orders(message, message.from_user.id)

    @dp.message(F.text == MENU_HELP)
    async def menu_help(message: Message):
        await message.answer("支付说明：\n1. 下单后会生成 USDT-TRC20 支付页。\n2. 支付后点击“刷新支付状态”。\n3. 自动确认完成后，订单会进入待发货。", reply_markup=main_menu())
        await send_folder_link_prompt(message, bot_code, RUNNER_TYPE)

    @dp.callback_query(F.data == 'folder_hint')
    async def cb_folder_hint(callback: CallbackQuery):
        runtime = await get_folder_link_runtime(bot_code, RUNNER_TYPE)
        text = str(runtime.get('manual_hint_text') or '已导入商城文件夹。机器人私聊请在 Telegram 内手动加入文件夹或手动置顶。')
        await callback.answer(text[:180], show_alert=True)

    @dp.callback_query(F.data == "show_categories")
    async def cb_show_categories(callback: CallbackQuery):
        await show_categories(callback)

    @dp.callback_query(F.data.startswith("cat:"))
    async def cb_category(callback: CallbackQuery):
        _, cat_id = callback.data.split(":", 1)
        await show_products(callback, int(cat_id))

    @dp.callback_query(F.data.startswith("prod:"))
    async def cb_product(callback: CallbackQuery):
        _, product_id = callback.data.split(":", 1)
        product = await api_get(f"/catalog/products/{int(product_id)}")
        await send_product_detail(callback.message, product, None)
        await callback.answer()

    @dp.callback_query(F.data.startswith("sku:"))
    async def cb_select_sku(callback: CallbackQuery):
        _, product_id, sku_id = callback.data.split(":")
        product = await api_get(f"/catalog/products/{int(product_id)}")
        sku = pick_sku(product, int(sku_id))
        if sku is None:
            await callback.answer("该规格不存在", show_alert=True)
            return
        if sku.get("is_active") is False:
            await callback.answer("该规格已下架", show_alert=True)
            return
        if int(sku.get("stock_qty") or 0) <= 0:
            await callback.answer("该规格库存不足", show_alert=True)
            return
        await send_product_detail(callback.message, product, int(sku_id))
        await callback.answer("规格已选中")

    @dp.callback_query(F.data == "addr:add")
    async def cb_add_address(callback: CallbackQuery, state: FSMContext):
        await begin_address_input(callback, state)

    @dp.callback_query(F.data.startswith("addr:def:"))
    async def cb_default_address(callback: CallbackQuery):
        address_id = int(callback.data.split(":")[2])
        await api_post(f"/addresses/{address_id}/default", params={"telegram_user_id": str(callback.from_user.id)})
        await callback.answer("已设为默认地址")
        await show_addresses(callback.message, callback.from_user.id)

    @dp.callback_query(F.data.startswith("addr:del:"))
    async def cb_delete_address(callback: CallbackQuery):
        address_id = int(callback.data.split(":")[2])
        await api_delete(f"/addresses/{address_id}", params={"telegram_user_id": str(callback.from_user.id)})
        await callback.answer("地址已删除")
        await show_addresses(callback.message, callback.from_user.id)

    @dp.message(AddressForm.waiting_template)
    async def recv_address_template(message: Message, state: FSMContext):
        lines = [x.strip() for x in (message.text or "").splitlines() if x.strip()]
        if len(lines) not in {6, 7}:
            await message.answer("地址格式不对，请按 6 或 7 行发送。发送 /cancel 可取消。")
            return
        postal_code = lines[6] if len(lines) == 7 else ""
        addresses = await get_addresses(message.from_user.id)
        payload = {
            "bot_code": bot_code,
            "telegram_user_id": str(message.from_user.id),
            "receiver_name": lines[0],
            "receiver_phone": lines[1],
            "province": lines[2],
            "city": lines[3],
            "district": lines[4],
            "address_detail": lines[5],
            "postal_code": postal_code,
            "is_default": len(addresses) == 0,
            "remark": "",
        }
        await api_post("/addresses", payload)
        await state.clear()
        await message.answer("地址已保存。", reply_markup=main_menu())
        await show_addresses(message, message.from_user.id)

    @dp.callback_query(F.data.startswith("buyaddrlist:"))
    async def cb_buy_choose_address(callback: CallbackQuery, state: FSMContext):
        parts = callback.data.split(":")
        product_id = int(parts[1])
        sku_id = int(parts[2]) if len(parts) >= 3 and str(parts[2]).isdigit() else None
        if sku_id is not None:
            product = await api_get(f"/catalog/products/{product_id}")
            sku = resolve_selected_sku(product, sku_id)
            if sku is None:
                await callback.answer("该规格已失效，请重新选择", show_alert=True)
                return
        addresses = await get_addresses(callback.from_user.id)
        if not addresses:
            await callback.answer("请先添加收货地址", show_alert=True)
            await begin_address_input(callback, state)
            return
        buttons = []
        for addr in addresses[:8]:
            label = f"{addr['receiver_name']} / {addr['receiver_phone']}"
            if addr.get('is_default'):
                label = f"⭐ {label}"
            if sku_id is not None:
                cb = f"buyaddr:{product_id}:{sku_id}:{addr['id']}"
            else:
                cb = f"buyaddr:{product_id}:{addr['id']}"
            buttons.append([InlineKeyboardButton(text=label, callback_data=cb)])
        buttons.append([InlineKeyboardButton(text="➕ 新增地址", callback_data="addr:add")])
        await callback.message.answer("请选择下单地址：", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        await callback.answer()

    @dp.callback_query(F.data.startswith("buy:"))
    async def cb_buy_default(callback: CallbackQuery, state: FSMContext):
        parts = callback.data.split(":")
        product_id = int(parts[1])
        sku_id = int(parts[2]) if len(parts) >= 3 and str(parts[2]).isdigit() else None
        selected_sku = None
        if sku_id is not None:
            product = await api_get(f"/catalog/products/{product_id}")
            selected_sku = resolve_selected_sku(product, sku_id)
            if selected_sku is None:
                await callback.answer("该规格已失效，请重新选择", show_alert=True)
                return
        addresses = await get_addresses(callback.from_user.id)
        if not addresses:
            await callback.answer("请先添加收货地址", show_alert=True)
            await begin_address_input(callback, state)
            return
        default_addr = next((x for x in addresses if x.get("is_default")), addresses[0])
        try:
            await create_order_and_payment(callback.message, callback.from_user.id, product_id, default_addr["id"], selected_sku)
            await callback.answer("订单已创建")
        except Exception as e:
            await callback.answer(str(e), show_alert=True)

    @dp.callback_query(F.data.startswith("buyaddr:"))
    async def cb_buy_with_address(callback: CallbackQuery):
        parts = callback.data.split(":")
        product_id = int(parts[1])
        if len(parts) >= 4:
            sku_id = int(parts[2]) if str(parts[2]).isdigit() else None
            address_id = int(parts[3])
        else:
            sku_id = None
            address_id = int(parts[2])
        selected_sku = None
        if sku_id is not None:
            product = await api_get(f"/catalog/products/{product_id}")
            selected_sku = resolve_selected_sku(product, sku_id)
            if selected_sku is None:
                await callback.answer("该规格已失效，请重新选择", show_alert=True)
                return
        try:
            await create_order_and_payment(callback.message, callback.from_user.id, product_id, address_id, selected_sku)
            await callback.answer("订单已创建")
        except Exception as e:
            await callback.answer(str(e), show_alert=True)

    @dp.callback_query(F.data == "orders:list")
    async def cb_orders_list(callback: CallbackQuery):
        await show_orders(callback.message, callback.from_user.id)
        await callback.answer()

    @dp.callback_query(F.data.startswith("order:"))
    async def cb_order_detail(callback: CallbackQuery):
        order_id = int(callback.data.split(":", 1)[1])
        await show_order_detail(callback, order_id, callback.from_user.id)

    @dp.callback_query(F.data.startswith("otrace:"))
    async def cb_order_logistics_trace(callback: CallbackQuery):
        order_id = int(callback.data.split(":", 1)[1])
        try:
            order = await api_get(
                f"/orders/{order_id}",
                params={"telegram_user_id": str(callback.from_user.id)},
            )
        except Exception:
            await callback.answer("加载物流信息失败，请稍后再试", show_alert=True)
            return

        shipment = order.get("shipment") or {}
        courier = str(shipment.get("courier_company") or order.get("courier_company") or "").strip()
        track = str(shipment.get("tracking_no") or order.get("tracking_no") or "").strip()

        if not track:
            await callback.answer("暂未发货，暂无物流单号。", show_alert=True)
            return

        if courier:
            text = (
                f"快递公司：{courier}\n"
                f"快递单号：{track}\n\n"
                "请复制快递单号，到快递官网、支付宝/微信「查快递」或常用查件工具中自行查询。\n"
                "如物流信息刚录入，可能会有短暂延迟。"
            )
        else:
            text = (
                f"快递单号：{track}\n\n"
                "请复制快递单号，到常用查件工具中自行查询。\n"
                "如需人工协助，请联系客服。"
            )

        await callback.message.answer(text)
        await callback.answer()

    @dp.callback_query(F.data.startswith("pay:"))
    async def cb_pay(callback: CallbackQuery):
        order_id = int(callback.data.split(":", 1)[1])
        order = await api_get(f"/orders/{order_id}", params={"telegram_user_id": str(callback.from_user.id)})
        payment = order.get("payment") or {}

        if payment and payment.get("receive_address"):
            await send_payment_page(callback.message, order_id, order["order_no"], payment)
            await callback.answer("已打开当前支付页")
            return

        if order.get("pay_status") == "paid":
            await callback.answer("该订单已支付，无需再生成支付页", show_alert=True)
            return

        await callback.answer("该订单暂无可查看支付单", show_alert=True)

    @dp.callback_query(F.data.startswith("payrefresh:"))
    async def cb_pay_refresh(callback: CallbackQuery):
        order_id = int(callback.data.split(":", 1)[1])
        try:
            refresh_resp = await api_post(f"/admin/orders/{order_id}/payment-refresh", {})
        except Exception as e:
            await callback.answer(str(e), show_alert=True)
            return

        order = await api_get(f"/orders/{order_id}", params={"telegram_user_id": str(callback.from_user.id)})
        payment = order.get("payment") or {}
        result = refresh_resp.get("result") or {}
        reason = str(result.get("reason") or "").strip()

        if order.get("pay_status") == "paid":
            txid = str(payment.get("txid") or "").strip()
            msg = "支付已确认，订单进入待发货"
            if txid:
                msg += f"\nTXID: {short_txid(txid)}"
            await callback.answer(msg[:190], show_alert=True)
        else:
            status = payment.get("confirm_status_text") or payment.get("confirm_status") or order.get("pay_status") or "pending"
            msg = f"当前支付状态：{status}"
            if reason:
                msg += f"\n{reason}"
            await callback.answer(msg[:190], show_alert=True)

        await show_order_detail(callback, order_id, callback.from_user.id)

    @dp.callback_query(F.data.startswith("paidhint:"))
    async def cb_paid_hint(callback: CallbackQuery):
        await callback.answer("该订单已确认支付，请等待发货。", show_alert=True)

    return dp

async def run_one_bot(bot_code: str, token: str):
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = build_dispatcher(bot_code)
    try:
        await report_runtime(bot_code, "starting", "启动中")
        await report_runtime(bot_code, "running", "已启动")
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        await report_runtime(bot_code, "stopping", "停止中")
        raise
    except Exception as e:
        await report_runtime(bot_code, "stopped", "异常退出", last_error=str(e))
        print(f"[buyer-runner] bot {bot_code} error: {e}")
    finally:
        with contextlib.suppress(Exception):
            await bot.session.close()
        await report_runtime(bot_code, "stopped", "已停止")

async def start_bot(bot_code: str, token: str):
    if bot_code in RUNNING_BOTS:
        return
    print(f"[buyer-runner] starting bot {bot_code}")
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = build_dispatcher(bot_code)
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
            print(f"[buyer-runner] bot {bot_code} error: {e}")
        finally:
            with contextlib.suppress(Exception):
                await bot.session.close()
            await report_runtime(bot_code, "stopped", "已停止")
    task = asyncio.create_task(runner_task(), name=f"buyer:{bot_code}")
    hb = asyncio.create_task(heartbeat_loop(bot_code), name=f"hb:{bot_code}")
    RUNNING_BOTS[bot_code] = RunningBot(bot_code=bot_code, bot_type=RUNNER_TYPE, token=token, bot=bot, task=task, heartbeat_task=hb)

async def stop_bot(bot_code: str):
    running = RUNNING_BOTS.pop(bot_code, None)
    if not running:
        return
    print(f"[buyer-runner] stopping bot {bot_code}")
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
            deduped = []
            used_tokens = {}
            for row in rows:
                token = (row.get("bot_token") or "").strip()
                bot_code = row.get("bot_code")
                if not token or not bot_code:
                    continue
                if token in used_tokens:
                    print(f"[buyer-runner] duplicate token skipped: {bot_code}, same as {used_tokens[token]}")
                    await report_runtime(bot_code, "stopped", f"重复 token，已跳过；与 {used_tokens[token]} 冲突", last_error="duplicate token", heartbeat=False)
                    continue
                used_tokens[token] = bot_code
                deduped.append({
                    "bot_code": bot_code,
                    "bot_token": token,
                    "start_welcome_text": str(row.get("start_welcome_text") or "").strip(),
                })
            desired = {
                x['bot_code']: {
                    'token': x['bot_token'],
                    'start_welcome_text': str(x.get('start_welcome_text') or '').strip(),
                }
                for x in deduped
            }
            for bot_code in list(RUNNING_BOTS.keys()):
                if bot_code not in desired or RUNNING_BOTS[bot_code].token != desired[bot_code]['token']:
                    await stop_bot(bot_code)
            for bot_code, meta in desired.items():
                START_WELCOME_TEXTS[bot_code] = meta.get('start_welcome_text') or ''
                if START_WELCOME_TEXTS[bot_code]:
                    print(f"[buyer-runner] welcome text loaded for {bot_code}: {START_WELCOME_TEXTS[bot_code][:60]}")
                token = meta['token']
                if bot_code not in RUNNING_BOTS:
                    await start_bot(bot_code, token)
        except Exception as e:
            print(f"[buyer-runner] sync loop error: {e}")
        await asyncio.sleep(SYNC_SECONDS)

async def main():
    print(f"[buyer-runner] startup. API_BASE={API_BASE} runner_type={RUNNER_TYPE} sync={SYNC_SECONDS}s heartbeat={HEARTBEAT_SECONDS}s")
    ok = await wait_backend()
    if not ok:
        print("[buyer-runner] backend not ready, exit")
        return
    await sync_loop()

if __name__ == "__main__":
    asyncio.run(main())
