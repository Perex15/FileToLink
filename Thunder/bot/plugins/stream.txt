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

    if s.startswith("t.me/") or s.startswith("telegram.me/") or s.startswith("telegram.dog/"):
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
    if download:
        row.append(InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=download))

    if row:
        return InlineKeyboardMarkup([row])
    return None


async def _safe_send_with_buttons(func, *, text: str, reply_markup: Optional[InlineKeyboardMarkup], **kwargs):
    try:
        return await handle_flood_wait(func, text, reply_markup=reply_markup, **kwargs)
    except ButtonUrlInvalid:
        return await handle_flood_wait(func, text, reply_markup=None, **kwargs)
    except Exception as e:
        if "BUTTON_URL_INVALID" in str(e):
            return await handle_flood_wait(func, text, reply_markup=None, **kwargs)
        raise


async def send_link(msg: Message, links: Dict[str, Any]):
    body = MSG_LINKS.format(
        file_name=links.get('media_name', 'Unknown'),
        file_size=links.get('media_size', 'Unknown'),
        download_link=links.get('online_link', 'N/A'),
        stream_link=links.get('stream_link', 'N/A')
    )
    markup = get_link_buttons(links)
    await _safe_send_with_buttons(
        msg.reply_text,
        text=body,
        quote=True,
        parse_mode=enums.ParseMode.MARKDOWN,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=markup
    )


@StreamBot.on_message(filters.command("link") & ~filters.private)
async def link_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg) or not await require_token(bot, msg) or not await force_channel_check(bot, msg):
        return

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

    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_REQUEST, quote=True)
    shortener_val = kwargs.get('shortener', Var.SHORTEN_MEDIA_LINKS)
    await process_single(bot, msg, msg.reply_to_message, status_msg, shortener_val)


@StreamBot.on_message(
    filters.private &
    filters.incoming &
    (filters.document | filters.video | filters.photo | filters.audio |
     filters.voice | filters.animation | filters.video_note),
    group=4
)
async def private_receive_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg) or not await require_token(bot, msg) or not await force_channel_check(bot, msg):
        return
    if not msg.from_user:
        return

    await log_newusr(bot, msg.from_user.id, msg.from_user.first_name or "")
    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_FILE, quote=True)
    await process_single(bot, msg, msg, status_msg, await get_shortener_status(bot, msg))


@StreamBot.on_message(
    filters.channel &
    filters.incoming &
    (filters.document | filters.video | filters.audio) &
    ~filters.chat(Var.BIN_CHANNEL),
    group=-1
)
async def channel_receive_handler(bot: Client, msg: Message):
    if hasattr(Var, 'BANNED_CHANNELS') and msg.chat.id in Var.BANNED_CHANNELS:
        try:
            await handle_flood_wait(bot.leave_chat, msg.chat.id)
        except Exception:
            pass
        return
    if not await is_admin(bot, msg.chat.id):
        return

    stored_msg = await fwd_media(msg)
    if not stored_msg:
        return

    links = await gen_links(stored_msg, shortener=await get_shortener_status(bot, msg))

    await handle_flood_wait(
        stored_msg.reply_text,
        MSG_NEW_FILE_REQUEST.format(
            source_info=msg.chat.title or "Unknown Channel",
            id_=msg.chat.id,
            online_link=links.get('online_link', 'N/A'),
            stream_link=links.get('stream_link', 'N/A')
        ),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        quote=True
    )

    try:
        markup = get_link_buttons(links)
        if markup:
            await handle_flood_wait(msg.edit_reply_markup, reply_markup=markup)
        else:
            await send_link(msg, links)
    except (ButtonUrlInvalid, MessageNotModified, MessageDeleteForbidden):
        await send_link(msg, links)
    except Exception:
        await send_link(msg, links)


async def process_single(bot: Client, msg: Message, file_msg: Message, status_msg: Message, shortener_val: bool, original_request_msg: Optional[Message] = None):
    try:
        stored_msg = await fwd_media(file_msg)
        if not stored_msg:
            return None

        links = await gen_links(stored_msg, shortener=shortener_val)
        if not original_request_msg:
            await send_link(msg, links)

        if msg.chat.type != enums.ChatType.PRIVATE and msg.from_user:
            single_dm_text = MSG_DM_SINGLE_PREFIX.format(chat_title=msg.chat.title or "the chat") + "\n" + \
                             MSG_LINKS.format(
                                 file_name=links.get('media_name', 'Unknown'),
                                 file_size=links.get('media_size', 'Unknown'),
                                 download_link=links.get('online_link', 'N/A'),
                                 stream_link=links.get('stream_link', 'N/A')
                             )
            markup = get_link_buttons(links)
            try:
                await _safe_send_with_buttons(
                    bot.send_message,
                    text=single_dm_text,
                    chat_id=msg.from_user.id,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_markup=markup
                )
            except Exception:
                await reply_user_err(msg, MSG_ERROR_DM_FAILED)

        if status_msg:
            try:
                await handle_flood_wait(status_msg.delete)
            except Exception:
                pass

        return links

    except Exception as e:
        if status_msg:
            try:
                await handle_flood_wait(status_msg.edit_text, MSG_ERROR_PROCESSING_MEDIA)
            except Exception:
                pass
        await notify_own(bot, MSG_CRITICAL_ERROR.format(
            error=str(e),
            error_id=secrets.token_hex(6)
        ))
        return None
