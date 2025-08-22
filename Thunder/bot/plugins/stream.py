# Thunder/bot/plugins/stream.py

import asyncio
import secrets
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit, quote, urlencode, parse_qsl

from pyrogram import Client, enums, filters
from pyrogram.errors import MessageNotModified, MessageDeleteForbidden
from pyrogram.errors import ButtonUrlInvalid
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, Message

from Thunder.bot import StreamBot
from Thunder.utils.bot_utils import gen_links, is_admin, log_newusr, notify_own, reply_user_err
from Thunder.utils.database import db
from Thunder.utils.decorators import check_banned, get_shortener_status, require_token
from Thunder.utils.force_channel import force_channel_check
from Thunder.utils.handler import handle_flood_wait
from Thunder.utils.logger import logger
from Thunder.utils.messages import *
from Thunder.vars import Var


async def fwd_media(m_msg: Message) -> Optional[Message]:
    try:
        return await handle_flood_wait(m_msg.copy, chat_id=Var.BIN_CHANNEL)
    except Exception as e:
        if "MEDIA_CAPTION_TOO_LONG" in str(e):
            logger.debug(f"MEDIA_CAPTION_TOO_LONG error, retrying without caption: {e}")
            return await handle_flood_wait(m_msg.copy, chat_id=Var.BIN_CHANNEL, caption=None)
        logger.error(f"Error fwd_media copy: {e}", exc_info=True)
        return None


def _sanitize_url(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    if s.startswith(("t.me/", "telegram.me/", "telegram.dog/")):
        s = "https://" + s
    parts = urlsplit(s)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None
    if any(c.isspace() for c in s):
        path = quote(parts.path, safe="/%._-~")
        query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
        s = urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))
        parts = urlsplit(s)
    path = quote(parts.path, safe="/%._-~")
    query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
    safe = urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))
    if len(safe) > 1024:
        return None
    return safe


def get_link_buttons(links: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
    stream = _sanitize_url(links.get("stream_link"))
    download = _sanitize_url(links.get("online_link"))
    row = []
    if stream:
        row.append(InlineKeyboardButton(MSG_BUTTON_STREAM_NOW, url=stream))
    elif links.get("stream_link"):
        logger.warning(f"Invalid stream_link for button: {links.get('stream_link')}")
    if download:
        row.append(InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=download))
    elif links.get("online_link"):
        logger.warning(f"Invalid online_link for button: {links.get('online_link')}")
    return InlineKeyboardMarkup([row]) if row else None


async def _safe_send_with_buttons(func, *, text: str, reply_markup: Optional[InlineKeyboardMarkup], **kwargs):
    try:
        return await handle_flood_wait(func, text, reply_markup=reply_markup, **kwargs)
    except ButtonUrlInvalid as e:
        logger.error(f"ButtonUrlInvalid: {e}. Retrying without buttons.")
        return await handle_flood_wait(func, text, reply_markup=None, **kwargs)
    except Exception as e:
        if "BUTTON_URL_INVALID" in str(e):
            logger.error(f"Button error (generic): {e}. Retrying without buttons.")
            return await handle_flood_wait(func, text, reply_markup=None, **kwargs)
        raise


async def send_link(msg: Message, links: Dict[str, Any]):
    unique_id = secrets.token_hex(6)
    body = MSG_LINKS.format(
        file_name=links.get('media_name', 'Unknown'),
        file_size=links.get('media_size', 'Unknown'),
        download_link=links.get('online_link', 'N/A'),
        stream_link=links.get('stream_link', 'N/A')
    ) + f"\n\n`ID: {unique_id}`"
    markup = get_link_buttons(links)
    logger.info(f"Generated links: online={links.get('online_link')} | stream={links.get('stream_link')} | has_markup={bool(markup)} | ID={unique_id}")
    await _safe_send_with_buttons(
        msg.reply_text,
        text=body,
        quote=True,
        parse_mode=enums.ParseMode.MARKDOWN,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=markup
    )


# ---------------- COMMAND & HANDLERS ----------------

@StreamBot.on_message(filters.command("link") & ~filters.private)
async def link_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg) or not await require_token(bot, msg) or not await force_channel_check(bot, msg):
        return
    shortener_val = await get_shortener_status(bot, msg)
    if msg.from_user and not await db.is_user_exist(msg.from_user.id):
        invite_link = f"https://t.me/{bot.me.username}?start=start"
        await handle_flood_wait(
            msg.reply_text,
            MSG_ERROR_START_BOT.format(invite_link=invite_link),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode=enums.ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(MSG_BUTTON_START_CHAT, url=invite_link)]]),
            quote=True
        )
        return
    if msg.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP] and not await is_admin(bot, msg.chat.id):
        await reply_user_err(msg, MSG_ERROR_NOT_ADMIN)
        return
    if not msg.reply_to_message or not msg.reply_to_message.media:
        await reply_user_err(msg, MSG_ERROR_REPLY_FILE if not msg.reply_to_message else MSG_ERROR_NO_FILE)
        return
    parts = msg.text.split()
    num_files = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
    if not 1 <= num_files <= Var.MAX_BATCH_FILES:
        await reply_user_err(msg, MSG_ERROR_NUMBER_RANGE.format(max_files=Var.MAX_BATCH_FILES))
        return
    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_REQUEST, quote=True)
    shortener_val = kwargs.get('shortener', Var.SHORTEN_MEDIA_LINKS)
    if num_files == 1:
        await process_single(bot, msg, msg.reply_to_message, status_msg, shortener_val)
    else:
        await process_batch(bot, msg, msg.reply_to_message.id, num_files, status_msg, shortener_val)


# ---------------- PRIVATE & CHANNEL HANDLERS ----------------

@StreamBot.on_message(filters.private & filters.incoming &
                      (filters.document | filters.video | filters.photo | filters.audio |
                       filters.voice | filters.animation | filters.video_note), group=4)
async def private_receive_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg) or not await require_token(bot, msg) or not await force_channel_check(bot, msg):
        return
    if not msg.from_user:
        return
    await log_newusr(bot, msg.from_user.id, msg.from_user.first_name or "")
    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_FILE, quote=True)
    await process_single(bot, msg, msg, status_msg, await get_shortener_status(bot, msg))


@StreamBot.on_message(filters.channel & filters.incoming &
                      (filters.document | filters.video | filters.audio) &
                      ~filters.chat(Var.BIN_CHANNEL), group=-1)
async def channel_receive_handler(bot: Client, msg: Message):
    if hasattr(Var, 'BANNED_CHANNELS') and msg.chat.id in Var.BANNED_CHANNELS:
        try: await handle_flood_wait(bot.leave_chat, msg.chat.id)
        except Exception as e: logger.error(f"Error leaving banned channel {msg.chat.id}: {e}")
        return
    if not await is_admin(bot, msg.chat.id):
        logger.debug(f"Bot is not admin in channel {msg.chat.id}. Ignoring message.")
        return
    try:
        stored_msg = await fwd_media(msg)
        if not stored_msg: return
        links = await gen_links(stored_msg, shortener=await get_shortener_status(bot, msg))
        await send_link(msg, links)
    except Exception as e:
        logger.error(f"Error in channel_receive_handler for message {msg.id}: {e}", exc_info=True)


# ---------------- PROCESS SINGLE ----------------

async def process_single(bot: Client, msg: Message, file_msg: Message, status_msg: Optional[Message], shortener_val: bool, original_request_msg: Optional[Message] = None):
    try:
        stored_msg = await fwd_media(file_msg)
        if not stored_msg: return None
        links = await gen_links(stored_msg, shortener=shortener_val)
        await send_link(msg, links if not original_request_msg else links)
        return links
    except Exception as e:
        if status_msg:
            try: await handle_flood_wait(status_msg.edit_text, MSG_ERROR_PROCESSING_MEDIA)
            except: pass
        await notify_own(bot, MSG_CRITICAL_ERROR.format(error=str(e), error_id=secrets.token_hex(6)))
        return None


# ---------------- PROCESS BATCH ----------------

async def process_batch(bot: Client, msg: Message, start_id: int, count: int, status_msg: Message, shortener_val: bool):
    processed = 0
    failed = 0
    links_list = []
    for batch_start in range(0, count, 10):
        batch_size = min(10, count - batch_start)
        batch_ids = list(range(start_id + batch_start, start_id + batch_start + batch_size))
        try:
            messages = await handle
