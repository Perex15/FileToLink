# Thunder/bot/plugins/stream.py

import asyncio
import secrets
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit, quote, urlencode, parse_qsl

from pyrogram import Client, enums, filters
from pyrogram.errors import MessageNotModified, MessageDeleteForbidden
from pyrogram.errors import ButtonUrlInvalid  # specific exception
from pyrogram.types import (InlineKeyboardButton, InlineKeyboardMarkup,
                            LinkPreviewOptions, Message)

from Thunder.bot import StreamBot
from Thunder.utils.bot_utils import (gen_links, is_admin, log_newusr, notify_own,
                                     reply_user_err)
from Thunder.utils.database import db
from Thunder.utils.decorators import (check_banned, get_shortener_status,
                                      require_token)
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


# ------------------ URL SANITIZING & BUTTONS (robust) ------------------

def _sanitize_url(raw: Optional[str]) -> Optional[str]:
    """
    Make sure URL is http(s), has a host, is stripped, and percent-encodes spaces etc.
    Returns a safe, reconstructed URL or None.
    """
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None

    # common cases returned by some shorteners without protocol
    if s.startswith("t.me/") or s.startswith("telegram.me/") or s.startswith("telegram.dog/"):
        s = "https://" + s

    # reject non-http(s)
    parts = urlsplit(s)
    if parts.scheme not in ("http", "https"):
        return None
    if not parts.netloc:
        return None
    if any(c.isspace() for c in s):
        # recompose with encoded path/query to remove spaces
        path = quote(parts.path, safe="/%._-~")
        query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
        s = urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))
        parts = urlsplit(s)

    # Telegram can still complain on weird chars; re-encode path & query always.
    path = quote(parts.path, safe="/%._-~")
    query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
    safe = urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))

    # conservative length guard (Telegram buttons are picky)
    if len(safe) > 1024:  # very generous cap
        return None

    return safe


def get_link_buttons(links: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
    stream = _sanitize_url(links.get("stream_link"))
    download = _sanitize_url(links.get("online_link"))

    row = []
    if stream:
        row.append(InlineKeyboardButton(MSG_BUTTON_STREAM_NOW, url=stream))
    else:
        bad = links.get("stream_link")
        if bad:
            logger.warning(f"Invalid stream_link for button: {bad}")

    if download:
        row.append(InlineKeyboardButton(MSG_BUTTON_DOWNLOAD, url=download))
    else:
        bad = links.get("online_link")
        if bad:
            logger.warning(f"Invalid online_link for button: {bad}")

    if row:
        return InlineKeyboardMarkup([row])
    return None


async def _safe_send_with_buttons(func, *, text: str, reply_markup: Optional[InlineKeyboardMarkup], **kwargs):
    """
    Try sending with buttons. If Telegram rejects button URLs, resend without buttons.
    """
    try:
        return await handle_flood_wait(func, text, reply_markup=reply_markup, **kwargs)
    except ButtonUrlInvalid as e:
        logger.error(f"ButtonUrlInvalid: {e}. Retrying without buttons.")
        return await handle_flood_wait(func, text, reply_markup=None, **kwargs)
    except Exception as e:
        # Some other error with markup — fallback to no buttons
        if "BUTTON_URL_INVALID" in str(e):
            logger.error(f"Button error (generic): {e}. Retrying without buttons.")
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

    logger.info(f"Generated links: online={links.get('online_link')} | stream={links.get('stream_link')} | has_markup={bool(markup)}")

    await _safe_send_with_buttons(
        msg.reply_text,
        text=body,
        quote=True,
        parse_mode=enums.ParseMode.MARKDOWN,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=markup
    )


# ------------------ COMMAND & HANDLERS ------------------

@StreamBot.on_message(filters.command("link") & ~filters.private)
async def link_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg):
        return
    if not await require_token(bot, msg):
        return
    if not await force_channel_check(bot, msg):
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
    if msg.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_admin(bot, msg.chat.id):
            await reply_user_err(msg, MSG_ERROR_NOT_ADMIN)
            return
    if not msg.reply_to_message:
        await reply_user_err(msg, MSG_ERROR_REPLY_FILE)
        return
    if not msg.reply_to_message.media:
        await reply_user_err(msg, MSG_ERROR_NO_FILE)
        return

    parts = msg.text.split()
    num_files = 1
    if len(parts) > 1:
        try:
            num_files = int(parts[1])
            if not 1 <= num_files <= Var.MAX_BATCH_FILES:
                await reply_user_err(msg, MSG_ERROR_NUMBER_RANGE.format(max_files=Var.MAX_BATCH_FILES))
                return
        except ValueError:
            await reply_user_err(msg, MSG_ERROR_INVALID_NUMBER)
            return

    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_REQUEST, quote=True)
    shortener_val = kwargs.get('shortener', Var.SHORTEN_MEDIA_LINKS)

    if num_files == 1:
        await process_single(bot, msg, msg.reply_to_message, status_msg, shortener_val)
    else:
        await process_batch(bot, msg, msg.reply_to_message.id, num_files, status_msg, shortener_val)


@StreamBot.on_message(
    filters.private &
    filters.incoming &
    (filters.document | filters.video | filters.photo | filters.audio |
     filters.voice | filters.animation | filters.video_note),
    group=4
)
async def private_receive_handler(bot: Client, msg: Message, **kwargs):
    if not await check_banned(bot, msg):
        return
    if not await require_token(bot, msg):
        return
    if not await force_channel_check(bot, msg):
        return
    shortener_val = await get_shortener_status(bot, msg)
    if not msg.from_user:
        return
    await log_newusr(bot, msg.from_user.id, msg.from_user.first_name or "")
    status_msg = await handle_flood_wait(msg.reply_text, MSG_PROCESSING_FILE, quote=True)
    await process_single(bot, msg, msg, status_msg, shortener_val)


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
        except Exception as e:
            logger.error(f"Error leaving banned channel {msg.chat.id}: {e}")
        return
    if not await is_admin(bot, msg.chat.id):
        logger.debug(f"Bot is not admin in channel {msg.chat.id} ({msg.chat.title or 'Unknown'}). Ignoring message.")
        return
    try:
        stored_msg = await fwd_media(msg)
        if not stored_msg:
            logger.error(f"Failed to forward media from channel {msg.chat.id}. Ignoring.")
            return
        shortener_val = await get_shortener_status(bot, msg)
        links = await gen_links(stored_msg, shortener=shortener_val)

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
            # Try to add buttons to the original channel message
            markup = get_link_buttons(links)
            if markup:
                await handle_flood_wait(msg.edit_reply_markup, reply_markup=markup)
            else:
                # Fallback: just send a new message with text/links
                await send_link(msg, links)
        except ButtonUrlInvalid as e:
            logger.error(f"ButtonUrlInvalid while editing markup: {e}. Sending without buttons.")
            await send_link(msg, links)
        except MessageNotModified:
            pass
        except MessageDeleteForbidden:
            logger.debug(f"Failed to edit reply markup for message {msg.id} due to permissions. Sending new link instead.")
            await send_link(msg, links)
        except Exception as e:
            logger.error(f"Error editing reply markup for message {msg.id}: {e}", exc_info=True)
            await send_link(msg, links)

    except Exception as e:
        logger.error(f"Error in channel_receive_handler for message {msg.id}: {e}", exc_info=True)


async def process_single(bot: Client, msg: Message, file_msg: Message, status_msg: Message, shortener_val: bool, original_request_msg: Optional[Message] = None):
    try:
        stored_msg = await fwd_media(file_msg)
        if not stored_msg:
            logger.error(f"Failed to forward media for message {file_msg.id}. Skipping.")
            return None

        links = await gen_links(stored_msg, shortener=shortener_val)
        logger.info(f"Generated links: {links}")

        if not original_request_msg:
            await send_link(msg, links)

        if msg.chat.type != enums.ChatType.PRIVATE and msg.from_user:
            try:
                single_dm_text = MSG_DM_SINGLE_PREFIX.format(chat_title=msg.chat.title or "the chat") + "\n" + \
                                 MSG_LINKS.format(
                                     file_name=links.get('media_name', 'Unknown'),
                                     file_size=links.get('media_size', 'Unknown'),
                                     download_link=links.get('online_link', 'N/A'),
                                     stream_link=links.get('stream_link', 'N/A')
                                 )
                markup = get_link_buttons(links)
                await _safe_send_with_buttons(
                    bot.send_message,
                    text=single_dm_text,
                    chat_id=msg.from_user.id,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_markup=markup
                )
            except Exception as e:
                logger.error(f"Error sending DM for single file: {e}", exc_info=True)
                await reply_user_err(msg, MSG_ERROR_DM_FAILED)

        # log to BIN thread
        source_msg = original_request_msg if original_request_msg else msg
        source_info = ""
        source_id = 0
        if source_msg.from_user:
            source_info = source_msg.from_user.full_name or (f"@{source_msg.from_user.username}" if source_msg.from_user.username else "Unknown User")
            source_id = source_msg.from_user.id
        elif source_msg.chat.type == enums.ChatType.CHANNEL:
            source_info = source_msg.chat.title or "Unknown Channel"
            source_id = source_msg.chat.id

        if source_info and source_id:
            await handle_flood_wait(
                stored_msg.reply_text,
                MSG_NEW_FILE_REQUEST.format(
                    source_info=source_info,
                    id_=source_id,
                    online_link=links.get('online_link', 'N/A'),
                    stream_link=links.get('stream_link', 'N/A')
                ),
                link_preview_options=LinkPreviewOptions(is_disabled=True),
                quote=True
            )

        if status_msg:
            try:
                await handle_flood_wait(status_msg.delete)
            except MessageDeleteForbidden:
                logger.debug(f"Failed to delete status message {status_msg.id} due to permissions.")
            except Exception as e:
                logger.error(f"Error deleting status message {status_msg.id}: {e}", exc_info=True)

        return links

    except Exception as e:
        logger.error(f"Error processing single file for message {file_msg.id}: {e}", exc_info=True)
        if status_msg:
            try:
                await handle_flood_wait(status_msg.edit_text, MSG_ERROR_PROCESSING_MEDIA)
            except MessageNotModified:
                pass
            except MessageDeleteForbidden:
                logger.debug(f"Failed to edit status message {status_msg.id} due to permissions.")
            except Exception as edit_err:
                logger.error(f"Error editing status message {status_msg.id} after processing error: {edit_err}", exc_info=True)

        await notify_own(bot, MSG_CRITICAL_ERROR.format(
            error=str(e),
            error_id=secrets.token_hex(6)
        ))
        return None


async def process_batch(bot: Client, msg: Message, start_id: int, count: int, status_msg: Message, shortener_val: bool):
    processed = 0
    failed = 0
    links_list = []
    for batch_start in range(0, count, 10):
        batch_size = min(10, count - batch_start)
        batch_ids = list(range(start_id + batch_start, start_id + batch_start + batch_size))
        try:
            await handle_flood_wait(
                status_msg.edit_text,
                MSG_PROCESSING_BATCH.format(
                    batch_number=(batch_start // 10) + 1,
                    total_batches=(count + 9) // 10,
                    file_count=batch_size
                )
            )
        except MessageNotModified:
            pass
        try:
            messages = await handle_flood_wait(bot.get_messages, msg.chat.id, batch_ids)
            if messages is None:
                messages = []
        except Exception as e:
            logger.error(f"Error getting messages in batch: {e}", exc_info=True)
            messages = []
        for m in messages:
            if m and m.media:
                links = await process_single(bot, msg, m, None, shortener_val, original_request_msg=msg)
                if links:
                    links_list.append(links.get('online_link', ''))
                    processed += 1
                else:
                    failed += 1
            else:
                failed += 1
        if (processed + failed) % 5 == 0 or (processed + failed) == count:
            try:
                await handle_flood_wait(
                    status_msg.edit_text,
                    MSG_PROCESSING_STATUS.format(
                        processed=processed,
                        total=count,
                        failed=failed
                    )
                )
            except MessageNotModified:
                pass
    for i in range(0, len(links_list), 20):
        chunk = links_list[i:i+20]
        chunk_text = MSG_BATCH_LINKS_READY.format(count=len(chunk)) + f"\n\n`{chr(10).join(chunk)}`"
        await handle_flood_wait(
            msg.reply_text,
            chunk_text,
            quote=True,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        if msg.chat.type != enums.ChatType.PRIVATE and msg.from_user:
            try:
                await handle_flood_wait(
                    bot.send_message,
                    chat_id=msg.from_user.id,
                    text=MSG_DM_BATCH_PREFIX.format(chat_title=msg.chat.title or "the chat") + "\n" + chunk_text,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                    parse_mode=enums.ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Error sending DM in batch: {e}", exc_info=True)
                await reply_user_err(msg, MSG_ERROR_DM_FAILED)
        if i + 20 < len(links_list):
            await asyncio.sleep(0.5)
    await handle_flood_wait(
        status_msg.edit_text,
        MSG_PROCESSING_RESULT.format(
            processed=processed,
            total=count,
            failed=failed
        )
  )
