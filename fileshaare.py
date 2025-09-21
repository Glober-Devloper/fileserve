# filestore_advanced_final.py - COMPLETE, PERSISTENT, AND HIGH-PERFORMANCE VERSION
import asyncio
import os
import asyncpg
import uuid
import base64
import logging
from datetime import datetime
from typing import Optional, Tuple, Any
import io

# Imports for Health Check Server
import http.server
import socketserver
import threading

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
)
from telegram.ext import (
    Application, ApplicationBuilder, ContextTypes,
    CommandHandler, MessageHandler, filters, CallbackQueryHandler,
    JobQueue
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter as FloodWaitError

# Global DB Pool, initialized in main_async
DB_POOL = None

###############################################################################
# 1 — CONFIGURATION
###############################################################################
BOT_TOKEN = os.environ.get("BOT_TOKEN")
storage_id = int(os.environ.get("storage_id", 0))
BOT_USERNAME = os.environ.get("BOT_USERNAME")
ADMIN_IDS = list(map(int, os.environ.get("ADMIN_IDS", "").split(','))) if os.environ.get("ADMIN_IDS") else []
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT") or "@your_admin_contact"
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "t.me/your_channel")
HEALTH_CHECK_PORT = int(os.environ.get("PORT", 8000))
DATABASE_URL = os.environ.get("DATABASE_URL")

MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2GB
BULK_UPLOAD_DELAY = 0.5  # Seconds between bulk uploads
UPLOAD_CONCURRENCY = 5   # Max concurrent file forward operations
PAGINATION_LIMIT = 5     # Items per page for lists

###############################################################################
# 2 — ENHANCED LOGGING SYSTEM
###############################################################################
def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def setup_logging():
    clear_console()
    logger = logging.getLogger("FileStoreBot")
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    try:
        file_handler = logging.FileHandler('bot.log', encoding='utf-8')
        file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console_handler.setFormatter(console_formatter)
    original_emit = console_handler.emit
    def safe_emit(record):
        try:
            if hasattr(record, 'msg'):
                record.msg = str(record.msg).encode('ascii', 'ignore').decode('ascii')
            original_emit(record)
        except Exception:
            pass
    console_handler.emit = safe_emit
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

###############################################################################
# 3 — ASYNC DATABASE INITIALIZATION
###############################################################################
async def init_database():
    """Initialize PostgreSQL database using the global async pool"""
    async with DB_POOL.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS authorized_users (
                id SERIAL PRIMARY KEY, user_id BIGINT UNIQUE NOT NULL, username TEXT, first_name TEXT,
                added_by BIGINT NOT NULL, added_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE, caption_disabled BOOLEAN DEFAULT FALSE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id SERIAL PRIMARY KEY, name TEXT NOT NULL, owner_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, total_files INTEGER DEFAULT 0,
                total_size BIGINT DEFAULT 0, UNIQUE(name, owner_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY, group_id INTEGER NOT NULL, serial_number INTEGER NOT NULL,
                unique_id TEXT UNIQUE NOT NULL, file_name TEXT, file_type TEXT NOT NULL,
                file_size BIGINT DEFAULT 0, telegram_file_id TEXT NOT NULL, uploader_id BIGINT NOT NULL,
                uploader_username TEXT, uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                storage_message_id BIGINT, FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                UNIQUE(group_id, serial_number)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS file_links (
                id SERIAL PRIMARY KEY, link_code TEXT UNIQUE NOT NULL, file_id INTEGER, group_id INTEGER,
                link_type TEXT NOT NULL CHECK (link_type IN ('file', 'group')), owner_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, clicks INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE, FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("INSERT INTO bot_settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING", 'caption_enabled', '1')
        await conn.execute("INSERT INTO bot_settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING", 'custom_caption', CUSTOM_CAPTION)
        for admin_id in ADMIN_IDS:
            await conn.execute("""
                INSERT INTO authorized_users (user_id, username, first_name, added_by, is_active)
                VALUES ($1, $2, $3, $4, TRUE) ON CONFLICT (user_id) DO NOTHING
            """, admin_id, f'admin_{admin_id}', f'Admin {admin_id}', admin_id)
    logger.info("Database initialized successfully")

###############################################################################
# 4 — ASYNC UTILITY FUNCTIONS
###############################################################################
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def generate_id() -> str:
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode('utf-8')[:12]

def format_size(size_bytes: int) -> str:
    if not isinstance(size_bytes, (int, float, complex)) or size_bytes <= 0: return "0 B"
    size_bytes = int(size_bytes)
    if size_bytes < 1024: return f"{size_bytes} B"
    elif size_bytes < 1024**2: return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3: return f"{size_bytes/(1024**2):.1f} MB"
    else: return f"{size_bytes/(1024**3):.1f} GB"

def extract_file_data(message: Message) -> Tuple[Optional[Any], str, str, int, str]:
    file_obj, file_type, file_name, file_size, unique_id = None, "", "", 0, ""
    if message.document:
        file_obj, file_type = message.document, "document"
        file_name = file_obj.file_name or f"{file_type}_{file_obj.file_unique_id[:8]}"
    elif message.video:
        file_obj, file_type = message.video, "video"
        file_name = file_obj.file_name or f"{file_type}_{file_obj.file_unique_id[:8]}"
    elif message.audio:
        file_obj, file_type = message.audio, "audio"
        file_name = file_obj.file_name or f"{file_type}_{file_obj.file_unique_id[:8]}"
    elif message.photo:
        file_obj, file_type = message.photo[-1], "photo"
        file_name = f"{file_type}_{file_obj.file_unique_id[:8]}.jpg"
    elif message.voice:
        file_obj, file_type = message.voice, "voice"
        file_name = f"{file_type}_{file_obj.file_unique_id[:8]}.ogg"
    elif message.video_note:
        file_obj, file_type = message.video_note, "video_note"
        file_name = f"{file_type}_{file_obj.file_unique_id[:8]}.mp4"
    
    if file_obj:
        file_size = file_obj.file_size or 0
        unique_id = file_obj.file_unique_id
        
    return file_obj, file_type, file_name, file_size, unique_id


async def get_caption_setting() -> tuple:
    try:
        async with DB_POOL.acquire() as conn:
            settings = await conn.fetch("SELECT key, value FROM bot_settings WHERE key IN ('caption_enabled', 'custom_caption')")
        caption_enabled, custom_caption = True, CUSTOM_CAPTION
        settings_dict = {record['key']: record['value'] for record in settings}
        caption_enabled = settings_dict.get('caption_enabled', '1') == '1'
        custom_caption = settings_dict.get('custom_caption', CUSTOM_CAPTION)
        return caption_enabled, custom_caption
    except Exception as e:
        logger.error(f"Error getting caption settings: {e}")
        return True, CUSTOM_CAPTION

async def get_file_caption(file_name: str, serial_number: int = None, user_id: int = None) -> str:
    try:
        if user_id and not is_admin(user_id):
            async with DB_POOL.acquire() as conn:
                user_caption_disabled = await conn.fetchval("SELECT caption_disabled FROM authorized_users WHERE user_id = $1", user_id)
                if user_caption_disabled: return file_name
        caption_enabled, custom_caption = await get_caption_setting()
        if not caption_enabled: return file_name
        
        caption = f"`{file_name}`"
        if serial_number:
            caption = f"`#{serial_number:03d}` {caption}"
            
        return f"{caption}\n\n{custom_caption}"
    except Exception as e:
        logger.error(f"Error getting file caption: {e}")
        return file_name

async def is_user_authorized(user_id: int) -> bool:
    if is_admin(user_id): return True
    try:
        async with DB_POOL.acquire() as conn:
            result = await conn.fetchval("SELECT is_active FROM authorized_users WHERE user_id = $1 AND is_active = TRUE", user_id)
            return result is not None
    except Exception as e:
        logger.error(f"Error checking user authorization: {e}")
        return False

###############################################################################
# 5 — MAIN BOT CLASS
###############################################################################
class FileStoreBot:
    def __init__(self, application: Application):
        self.app = application
        self.upload_semaphore = asyncio.Semaphore(UPLOAD_CONCURRENCY)

    # --- Command Handlers ---
    async def start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id):
            await update.message.reply_text(f"🔒 You are not authorized to use this bot. Please contact the admin: {ADMIN_CONTACT}")
            return
            
        # Handle deep linking for shared files/groups
        if context.args and len(context.args) > 0:
            link_code = context.args[0]
            await self._handle_shared_link(update, context, link_code)
            return

        welcome_text = (
            f"👋 **Welcome, {update.effective_user.first_name}!**\n\n"
            "I am your personal File Storage Bot.\n\n"
            "**Here's how to get started:**\n"
            "1. Create a group using `/groups`.\n"
            "2. Select a group and start sending me files.\n"
            "3. Use `/getlink` to generate shareable links.\n\n"
            "Type /help for a full list of commands."
        )
        await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)

    async def help_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"🔒 You are not authorized. Contact: {ADMIN_CONTACT}")
            return
            
        user_help = (
            "**📖 Available Commands**\n\n"
            "`/start` - Welcome message\n"
            "`/help` - Show this help message\n"
            "`/groups` - Manage your file groups\n"
            "`/upload` - Select a group to start uploading\n"
            "`/bulkupload` - Select a group for bulk uploading\n"
            "`/clear` - Clear current upload selection\n"
            "`/getlink <group> <file_#> ` - Get a file link\n"
            "`/getgrouplink <group>` - Get a group link\n"
            "`/revokelink <code>` - Revoke a shared link\n"
            "`/search <query>` - Search for files"
        )
        admin_help = (
            "\n\n**👑 Admin Commands**\n"
            "`/admin` - Open Admin Panel\n"
            "`/adduser <id> <name>` - Add a user\n"
            "`/removeuser <id>` - Remove a user\n"
            "`/listusers` - List authorized users\n"
            "`/botstats` - Show bot statistics"
        )
        
        full_help = user_help
        if is_admin(update.effective_user.id):
            full_help += admin_help
            
        await update.message.reply_text(full_help, parse_mode=ParseMode.MARKDOWN)

    async def clear_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        context.user_data.pop('upload_group_id', None)
        context.user_data.pop('upload_group_name', None)
        context.user_data.pop('is_bulk_upload', None)
        
        await update.message.reply_text("✅ Your current upload selection has been cleared.")

    async def upload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE, bulk=False):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        context.user_data['is_bulk_upload'] = bulk
        
        text = "📁 **Select a Group for Upload**\n\nChoose an existing group or create a new one."
        if bulk:
            text = "📂 **Select a Group for Bulk Upload**\n\nAll files sent will be added to this group until you `/clear`."

        keyboard = await self._get_groups_keyboard(user_id, "select_group_upload", 0)
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

    async def bulkupload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.upload_handler(update, context, bulk=True)

    async def groups_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        text = "🗂️ **Your Groups**\n\nHere you can view, create, and manage your file groups."
        keyboard = await self._get_groups_keyboard(user_id, "view_group", 0)
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

    async def getlink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return

        if not context.args or len(context.args) != 2:
            await update.message.reply_text("<b>Usage:</b> <code>/getlink GROUP_NAME FILE_NUMBER</code>\n\nExample: <code>/getlink Movies 5</code>", parse_mode=ParseMode.HTML)
            return

        group_name, file_serial_str = context.args[0], context.args[1]
        if not file_serial_str.isdigit():
            await update.message.reply_text("❌ File number must be a valid integer.")
            return
        file_serial = int(file_serial_str)

        try:
            async with DB_POOL.acquire() as conn:
                file_record = await conn.fetchrow(
                    """
                    SELECT f.id FROM files f JOIN groups g ON f.group_id = g.id
                    WHERE g.owner_id = $1 AND g.name = $2 AND f.serial_number = $3
                    """, user_id, group_name, file_serial
                )
            if not file_record:
                await update.message.reply_text(f"❌ File #{file_serial} not found in group '{group_name}'.")
                return

            link_code = await self._create_link(file_record['id'], None, 'file', user_id)
            share_link = f"https://t.me/{BOT_USERNAME}?start={link_code}"
            await update.message.reply_text(f"🔗 **Share Link Generated:**\n\n`{share_link}`", parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Error generating link: {e}")
            await update.message.reply_text("An error occurred while creating the link.")

    async def getgrouplink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("<b>Usage:</b> <code>/getgrouplink GROUP_NAME</code>", parse_mode=ParseMode.HTML)
            return
            
        group_name = context.args[0]
        try:
            async with DB_POOL.acquire() as conn:
                group_id = await conn.fetchval("SELECT id FROM groups WHERE owner_id = $1 AND name = $2", user_id, group_name)
            
            if not group_id:
                await update.message.reply_text(f"❌ Group '{group_name}' not found.")
                return

            link_code = await self._create_link(None, group_id, 'group', user_id)
            share_link = f"https://t.me/{BOT_USERNAME}?start={link_code}"
            await update.message.reply_text(f"🔗 **Group Share Link Generated:**\n\n`{share_link}`", parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Error generating group link: {e}")
            await update.message.reply_text("An error occurred while creating the group link.")

    async def deletefile_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return

        if not context.args or len(context.args) != 2:
            await update.message.reply_text("<b>Usage:</b> <code>/deletefile GROUP_NAME FILE_NUMBER</code>", parse_mode=ParseMode.HTML)
            return

        group_name, file_serial_str = context.args[0], context.args[1]
        if not file_serial_str.isdigit():
            await update.message.reply_text("❌ File number must be a valid integer.")
            return
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes, Delete", callback_data=f"confirm_delete_file:{group_name}:{file_serial_str}"),
            InlineKeyboardButton("Cancel", callback_data="cancel_action")
        ]])
        await update.message.reply_text(f"⚠️ Are you sure you want to delete file #{file_serial_str} from group '{group_name}'? This cannot be undone.", reply_markup=keyboard)


    async def deletegroup_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return

        if not context.args or len(context.args) != 1:
            await update.message.reply_text("<b>Usage:</b> <code>/deletegroup GROUP_NAME</code>", parse_mode=ParseMode.HTML)
            return
            
        group_name = context.args[0]
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes, Delete Group", callback_data=f"confirm_delete_group:{group_name}"),
            InlineKeyboardButton("Cancel", callback_data="cancel_action")
        ]])
        await update.message.reply_text(f"⚠️ Are you sure you want to delete the entire group '{group_name}' and all its files? This is irreversible.", reply_markup=keyboard)
        
    async def revoke_link_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("<b>Usage:</b> <code>/revokelink LINK_CODE</code>\n\nYou can find the code in the share URL.", parse_mode=ParseMode.HTML)
            return
        
        link_code = context.args[0]
        try:
            async with DB_POOL.acquire() as conn:
                deleted_rows = await conn.execute("DELETE FROM file_links WHERE link_code = $1 AND owner_id = $2", link_code, user_id)
            if deleted_rows == 'DELETE 1':
                await update.message.reply_text("✅ Link has been successfully revoked.")
            else:
                await update.message.reply_text("❌ Link not found or you don't have permission to revoke it.")
        except Exception as e:
            logger.error(f"Error revoking link: {e}")
            await update.message.reply_text("An error occurred while revoking the link.")

    async def search_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return
        
        if not context.args:
            await update.message.reply_text("<b>Usage:</b> <code>/search QUERY</code>\n\nSearches for files across all your groups.", parse_mode=ParseMode.HTML)
            return
            
        query = " ".join(context.args)
        search_term = f"%{query}%"
        
        try:
            async with DB_POOL.acquire() as conn:
                results = await conn.fetch(
                    """
                    SELECT f.file_name, f.serial_number, g.name as group_name, f.file_size
                    FROM files f JOIN groups g ON f.group_id = g.id
                    WHERE g.owner_id = $1 AND f.file_name ILIKE $2
                    ORDER BY g.name, f.serial_number
                    LIMIT 25
                    """, user_id, search_term
                )
                
            if not results:
                await update.message.reply_text(f"No results found for '{query}'.")
                return
            
            message_text = f"🔎 **Search Results for '{query}'**\n\n"
            for r in results:
                message_text += f"- `[{r['group_name']}] #{r['serial_number']:03d}`: {r['file_name']} ({format_size(r['file_size'])})\n"
                
            if len(results) == 25:
                message_text += "\n*Note: Showing top 25 results.*"
                
            await update.message.reply_text(message_text, parse_mode=ParseMode.MARKDOWN)

        except Exception as e:
            logger.error(f"Error during search: {e}")
            await update.message.reply_text("An error occurred during the search.")

    # --- Admin Handlers ---
    async def admin_panel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id): return

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Caption Settings", callback_data="admin_caption_settings")],
            [InlineKeyboardButton("👥 List Users", callback_data="admin_list_users:0")],
        ])
        await update.message.reply_text("👑 **Admin Panel**", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

    async def add_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        admin_id = update.effective_user.id
        if not is_admin(admin_id): return

        if not context.args or len(context.args) < 2:
            await update.message.reply_text("Usage: `/adduser USER_ID FIRST_NAME`")
            return
        
        user_id_str, first_name = context.args[0], " ".join(context.args[1:])
        if not user_id_str.isdigit():
            await update.message.reply_text("Error: User ID must be a number.")
            return
        user_id = int(user_id_str)

        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    INSERT INTO authorized_users (user_id, first_name, added_by, is_active)
                    VALUES ($1, $2, $3, TRUE)
                    ON CONFLICT (user_id) DO UPDATE SET is_active = TRUE, first_name = $2
                """, user_id, first_name, admin_id)
            await update.message.reply_text(f"✅ User {first_name} (`{user_id}`) has been authorized.")
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
            await update.message.reply_text("An error occurred while adding the user.")
            
    async def remove_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        admin_id = update.effective_user.id
        if not is_admin(admin_id): return
        
        if not context.args or len(context.args) != 1 or not context.args[0].isdigit():
            await update.message.reply_text("Usage: `/removeuser USER_ID`")
            return
        user_id = int(context.args[0])

        if user_id in ADMIN_IDS:
            await update.message.reply_text("❌ Admins cannot be removed.")
            return

        try:
            async with DB_POOL.acquire() as conn:
                # We set is_active to FALSE instead of deleting to preserve history
                result = await conn.execute("UPDATE authorized_users SET is_active = FALSE WHERE user_id = $1", user_id)
            if result == 'UPDATE 1':
                await update.message.reply_text(f"✅ User `{user_id}` has been de-authorized.")
            else:
                await update.message.reply_text(f"User `{user_id}` not found.")
        except Exception as e:
            logger.error(f"Error removing user {user_id}: {e}")
            await update.message.reply_text("An error occurred.")
            
    async def list_users_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id): return
        await self._send_user_list(update.message)

    async def bot_stats_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id): return
        await self._send_bot_stats(update.message)
        
    # --- Core Logic Handlers ---
    async def file_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if not await is_user_authorized(user_id): return

        # Handle new group creation
        if 'awaiting_group_name' in context.user_data and update.message.text:
            await self._create_new_group(update, context)
            return

        # Handle file uploads
        upload_group_id = context.user_data.get('upload_group_id')
        if upload_group_id:
            file_obj, file_type, file_name, file_size, unique_id = extract_file_data(update.message)
            if not file_obj:
                await update.message.reply_text("Unsupported file type. Please try again.")
                return

            if file_size > MAX_FILE_SIZE:
                await update.message.reply_text(f"File is too large ({format_size(file_size)}). Maximum size is {format_size(MAX_FILE_SIZE)}.")
                return
            
            await self._process_file_upload(update, context, upload_group_id, file_obj, file_type, file_name, file_size, unique_id)
        else:
            await update.message.reply_text(
                "🤔 Please select a group first using `/upload` or `/bulkupload` before sending files.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        if not await is_user_authorized(user_id):
            await query.edit_message_text("🔒 You are not authorized.")
            return

        data = query.data.split(':')
        action = data[0]

        try:
            # Group Management
            if action == 'new_group':
                context.user_data['awaiting_group_name'] = True
                await query.edit_message_text("📝 **Enter the name for your new group:**")
            elif action == 'list_groups':
                offset = int(data[1])
                keyboard = await self._get_groups_keyboard(user_id, "view_group", offset)
                await query.edit_message_text("🗂️ **Your Groups**", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
            elif action == 'view_group':
                group_id, offset = int(data[1]), int(data[2])
                await self._send_files_list(query, group_id, offset)
            
            # Upload Flow
            elif action == 'select_group_upload':
                group_id, group_name = int(data[1]), data[2]
                context.user_data['upload_group_id'] = group_id
                context.user_data['upload_group_name'] = group_name
                is_bulk = context.user_data.get('is_bulk_upload', False)
                
                text = f"✅ Now sending files to group: **{group_name}**."
                if is_bulk:
                    text = f"✅ Bulk upload mode activated for group: **{group_name}**.\n\nSend all your files now. Use `/clear` when finished."
                
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)

            # Deletion Flow
            elif action == 'confirm_delete_file':
                group_name, serial = data[1], int(data[2])
                await self._perform_file_delete(query, user_id, group_name, serial)
            elif action == 'confirm_delete_group':
                group_name = data[1]
                await self._perform_group_delete(query, user_id, group_name)
            elif action == 'cancel_action':
                await query.edit_message_text("Action canceled.")

            # Admin Panel
            elif action.startswith('admin_'):
                await self._handle_admin_callbacks(query, context, data)

        except Exception as e:
            logger.error(f"Callback Error ({query.data}): {e}")
            try:
                await query.edit_message_text("An unexpected error occurred. Please try again.")
            except BadRequest:
                pass # Message might have been deleted

    # --- Internal Helper Methods ---

    async def _handle_shared_link(self, update, context, link_code):
        try:
            async with DB_POOL.acquire() as conn:
                link_data = await conn.fetchrow("SELECT * FROM file_links WHERE link_code = $1 AND is_active = TRUE", link_code)
                if not link_data:
                    await update.message.reply_text("❌ This link is invalid or has expired.")
                    return

                # Increment click count
                await conn.execute("UPDATE file_links SET clicks = clicks + 1 WHERE id = $1", link_data['id'])
                
                if link_data['link_type'] == 'file':
                    file_info = await conn.fetchrow(
                        """
                        SELECT f.*, g.name as group_name FROM files f JOIN groups g ON f.group_id = g.id
                        WHERE f.id = $1
                        """, link_data['file_id']
                    )
                    if file_info:
                        caption = await get_file_caption(file_info['file_name'], file_info['serial_number'], update.effective_user.id)
                        await context.bot.copy_message(
                            chat_id=update.effective_chat.id,
                            from_chat_id=storage_id,
                            message_id=file_info['storage_message_id'],
                            caption=caption,
                            parse_mode=ParseMode.MARKDOWN
                        )
                    else:
                        await update.message.reply_text("File not found.")
                
                elif link_data['link_type'] == 'group':
                    group_files = await conn.fetch("SELECT * FROM files WHERE group_id = $1 ORDER BY serial_number", link_data['group_id'])
                    if group_files:
                        group_name = await conn.fetchval("SELECT name FROM groups WHERE id = $1", link_data['group_id'])
                        await update.message.reply_text(f"📂 Forwarding all files from group **{group_name}**...", parse_mode=ParseMode.MARKDOWN)
                        for file in group_files:
                            async with self.upload_semaphore:
                                try:
                                    caption = await get_file_caption(file['file_name'], file['serial_number'], update.effective_user.id)
                                    await context.bot.copy_message(
                                        chat_id=update.effective_chat.id,
                                        from_chat_id=storage_id,
                                        message_id=file['storage_message_id'],
                                        caption=caption,
                                        parse_mode=ParseMode.MARKDOWN
                                    )
                                except Exception as e:
                                    logger.warning(f"Could not forward file {file['id']} from shared group link: {e}")
                                await asyncio.sleep(BULK_UPLOAD_DELAY)
                    else:
                        await update.message.reply_text("This group is empty.")

        except Exception as e:
            logger.error(f"Error handling shared link {link_code}: {e}")
            await update.message.reply_text("An error occurred processing the link.")

    async def _process_file_upload(self, update, context, group_id, file_obj, file_type, file_name, file_size, unique_id):
        user = update.effective_user
        status_msg = await update.message.reply_text(f"📤 Uploading `{file_name}`...")
        
        async with self.upload_semaphore:
            try:
                # Forward the message to the storage channel
                forwarded_msg = await update.message.forward(storage_id)
                storage_message_id = forwarded_msg.message_id
                
                # Save file metadata to the database
                async with DB_POOL.acquire() as conn:
                    async with conn.transaction():
                        # Get the next serial number for the group
                        next_serial = await conn.fetchval("SELECT COALESCE(MAX(serial_number), 0) + 1 FROM files WHERE group_id = $1", group_id)
                        
                        await conn.execute(
                            """
                            INSERT INTO files (group_id, serial_number, unique_id, file_name, file_type, file_size, telegram_file_id,
                                             uploader_id, uploader_username, storage_message_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            """,
                            group_id, next_serial, unique_id, file_name, file_type, file_size, file_obj.file_id,
                            user.id, user.username, storage_message_id
                        )
                        # Update group stats
                        await conn.execute("UPDATE groups SET total_files = total_files + 1, total_size = total_size + $1 WHERE id = $2", file_size, group_id)

                group_name = context.user_data.get('upload_group_name', '')
                await status_msg.edit_text(f"✅ **Saved!**\n\nFile: `{file_name}`\nGroup: `{group_name}`\nSerial: `#{next_serial:03d}`", parse_mode=ParseMode.MARKDOWN)

            except FloodWaitError as e:
                logger.warning(f"Flood wait of {e.retry_after}s encountered.")
                await status_msg.edit_text(f"Rate limited. Retrying in {e.retry_after} seconds...")
                await asyncio.sleep(e.retry_after)
                await self._process_file_upload(update, context, group_id, file_obj, file_type, file_name, file_size, unique_id)
            except Exception as e:
                logger.error(f"Failed to process file {file_name}: {e}")
                await status_msg.edit_text(f"❌ Failed to save file: {file_name}")
            finally:
                if not context.user_data.get('is_bulk_upload', False):
                    context.user_data.pop('upload_group_id', None)
                    context.user_data.pop('upload_group_name', None)

    async def _create_new_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        group_name = update.message.text.strip()
        context.user_data.pop('awaiting_group_name', None)

        if not group_name or len(group_name) > 50:
            await update.message.reply_text("Invalid group name. Please keep it under 50 characters.")
            return

        try:
            async with DB_POOL.acquire() as conn:
                await conn.execute("INSERT INTO groups (name, owner_id) VALUES ($1, $2)", group_name, user_id)
            await update.message.reply_text(f"✅ Group '{group_name}' created successfully!")
            await self.groups_handler(update, context) # Show the updated groups list
        except asyncpg.UniqueViolationError:
            await update.message.reply_text(f"❌ A group named '{group_name}' already exists.")
        except Exception as e:
            logger.error(f"Error creating group '{group_name}' for user {user_id}: {e}")
            await update.message.reply_text("An error occurred while creating the group.")

    async def _create_link(self, file_id, group_id, link_type, owner_id):
        link_code = generate_id()
        async with DB_POOL.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO file_links (link_code, file_id, group_id, link_type, owner_id)
                    VALUES ($1, $2, $3, $4, $5)
                    """, link_code, file_id, group_id, link_type, owner_id
                )
                return link_code
            except asyncpg.UniqueViolationError: # Extremely rare collision
                return await self._create_link(file_id, group_id, link_type, owner_id)

    async def _get_groups_keyboard(self, user_id, callback_action, offset):
        async with DB_POOL.acquire() as conn:
            groups = await conn.fetch(
                "SELECT id, name, total_files FROM groups WHERE owner_id = $1 ORDER BY name LIMIT $2 OFFSET $3",
                user_id, PAGINATION_LIMIT, offset
            )
            total_groups = await conn.fetchval("SELECT COUNT(*) FROM groups WHERE owner_id = $1", user_id)

        buttons = []
        for group in groups:
            button_text = f"{group['name']} ({group['total_files']} files)"
            callback_data = f"{callback_action}:{group['id']}"
            if callback_action == "select_group_upload":
                callback_data += f":{group['name']}" # Pass name for confirmation message
            elif callback_action == "view_group":
                callback_data += ":0" # Start at offset 0 for files list
            buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        # Pagination controls
        nav_buttons = []
        if offset > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"list_groups:{offset - PAGINATION_LIMIT}"))
        if total_groups > offset + PAGINATION_LIMIT:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"list_groups:{offset + PAGINATION_LIMIT}"))
        if nav_buttons:
            buttons.append(nav_buttons)
            
        buttons.append([InlineKeyboardButton("➕ Create New Group", callback_data="new_group")])
        return InlineKeyboardMarkup(buttons)
        
    async def _send_files_list(self, query, group_id, offset):
        async with DB_POOL.acquire() as conn:
            files = await conn.fetch(
                "SELECT serial_number, file_name, file_size FROM files WHERE group_id = $1 ORDER BY serial_number LIMIT $2 OFFSET $3",
                group_id, PAGINATION_LIMIT, offset
            )
            group_info = await conn.fetchrow("SELECT name, total_files, total_size FROM groups WHERE id = $1", group_id)

        if not group_info:
            await query.edit_message_text("Group not found.", reply_markup=None)
            return

        text = (f"**Group: {group_info['name']}**\n"
                f"Total Files: {group_info['total_files']} | Total Size: {format_size(group_info['total_size'])}\n\n")
        
        buttons = []
        if not files and offset == 0:
            text += "This group is empty. Use `/upload` to add files."
        else:
            for f in files:
                text += f"`#{f['serial_number']:03d}`: {f['file_name']} ({format_size(f['file_size'])})\n"
            
            # Pagination for files
            nav_buttons = []
            if offset > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"view_group:{group_id}:{offset - PAGINATION_LIMIT}"))
            if group_info['total_files'] > offset + PAGINATION_LIMIT:
                nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"view_group:{group_id}:{offset + PAGINATION_LIMIT}"))
            if nav_buttons:
                buttons.append(nav_buttons)
                
        buttons.append([InlineKeyboardButton("« Back to Groups", callback_data="list_groups:0")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)

    async def _perform_file_delete(self, query: CallbackQuery, user_id, group_name, serial):
        try:
            async with DB_POOL.acquire() as conn:
                async with conn.transaction():
                    file_data = await conn.fetchrow("""
                        SELECT f.id, f.storage_message_id, f.file_size, g.id as group_id 
                        FROM files f JOIN groups g ON f.group_id = g.id
                        WHERE g.owner_id = $1 AND g.name = $2 AND f.serial_number = $3
                    """, user_id, group_name, serial)

                    if not file_data:
                        await query.edit_message_text("❌ File not found or already deleted.")
                        return

                    # Delete from DB. ON DELETE CASCADE handles links.
                    await conn.execute("DELETE FROM files WHERE id = $1", file_data['id'])
                    # Update group stats
                    await conn.execute("UPDATE groups SET total_files = total_files - 1, total_size = total_size - $1 WHERE id = $2", file_data['file_size'], file_data['group_id'])

            # Try to delete from channel (best effort)
            try:
                await self.app.bot.delete_message(storage_id, file_data['storage_message_id'])
            except Exception as e:
                logger.warning(f"Could not delete message {file_data['storage_message_id']} from channel: {e}")

            await query.edit_message_text(f"✅ File #{serial} from group '{group_name}' has been deleted.")
        except Exception as e:
            logger.error(f"Error deleting file #{serial} from {group_name}: {e}")
            await query.edit_message_text("An error occurred during file deletion.")

    async def _perform_group_delete(self, query: CallbackQuery, user_id, group_name):
        try:
            async with DB_POOL.acquire() as conn:
                async with conn.transaction():
                    # Get all message_ids from the group for deletion
                    message_ids = [r['storage_message_id'] for r in await conn.fetch("""
                        SELECT f.storage_message_id FROM files f JOIN groups g ON f.group_id = g.id
                        WHERE g.owner_id = $1 AND g.name = $2
                    """, user_id, group_name)]

                    # Delete the group. ON DELETE CASCADE handles files and links.
                    result = await conn.execute("DELETE FROM groups WHERE owner_id = $1 AND name = $2", user_id, group_name)
            
            if result == 'DELETE 0':
                await query.edit_message_text(f"❌ Group '{group_name}' not found.")
                return

            await query.edit_message_text("Deleting group and files from channel... this may take a moment.")
            # Delete messages from channel
            deleted_count = 0
            for msg_id in message_ids:
                try:
                    await self.app.bot.delete_message(storage_id, msg_id)
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete message {msg_id} on group delete: {e}")
                await asyncio.sleep(0.1)

            await query.edit_message_text(f"✅ Group '{group_name}' and {deleted_count}/{len(message_ids)} associated files have been deleted.")
        except Exception as e:
            logger.error(f"Error deleting group {group_name}: {e}")
            await query.edit_message_text("An error occurred during group deletion.")

    # --- Admin Helper Methods ---

    async def _handle_admin_callbacks(self, query, context, data):
        action = data[0]
        if action == 'admin_stats':
            await self._send_bot_stats(query.message, edit=True)
        elif action == 'admin_list_users':
            offset = int(data[1])
            await self._send_user_list(query.message, offset, edit=True)
        elif action == 'admin_caption_settings':
            enabled, caption_text = await get_caption_setting()
            status = "Enabled" if enabled else "Disabled"
            buttons = [[InlineKeyboardButton(f"Toggle (Currently: {status})", callback_data="admin_toggle_caption")]]
            buttons.append([InlineKeyboardButton("Set Custom Caption", callback_data="admin_set_caption")])
            buttons.append([InlineKeyboardButton("« Back to Admin Panel", callback_data="admin_back")])
            await query.edit_message_text(f"**Caption Settings**\n\nCurrent Caption:\n`{caption_text}`", reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)
        elif action == 'admin_toggle_caption':
            async with DB_POOL.acquire() as conn:
                current_val = await conn.fetchval("SELECT value FROM bot_settings WHERE key = 'caption_enabled'")
                new_val = '0' if current_val == '1' else '1'
                await conn.execute("UPDATE bot_settings SET value = $1 WHERE key = 'caption_enabled'", new_val)
            await query.answer(f"Captions are now {'Enabled' if new_val == '1' else 'Disabled'}.")
            await self._handle_admin_callbacks(query, context, ['admin_caption_settings']) # Refresh menu
        elif action == 'admin_set_caption':
            context.user_data['awaiting_caption'] = True
            await query.edit_message_text("Please send the new custom caption text.")
        elif action == 'admin_back':
            await self.admin_panel_handler(query, context)

    async def _send_bot_stats(self, message: Message, edit=False):
        async with DB_POOL.acquire() as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM authorized_users WHERE is_active = TRUE")
            total_files = await conn.fetchval("SELECT COALESCE(SUM(total_files), 0) FROM groups")
            total_size = await conn.fetchval("SELECT COALESCE(SUM(total_size), 0) FROM groups")
            total_groups = await conn.fetchval("SELECT COUNT(*) FROM groups")
        
        stats_text = (
            "**📊 Bot Statistics**\n\n"
            f"Authorized Users: `{total_users}`\n"
            f"Total Groups: `{total_groups}`\n"
            f"Total Files Stored: `{total_files}`\n"
            f"Total Storage Used: `{format_size(total_size)}`"
        )
        
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Admin Panel", callback_data="admin_back")]])
        
        if edit:
            await message.edit_text(stats_text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
        else:
            await message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)

    async def _send_user_list(self, message: Message, offset=0, edit=False):
        async with DB_POOL.acquire() as conn:
            users = await conn.fetch(
                "SELECT user_id, first_name, is_active FROM authorized_users ORDER BY id LIMIT $1 OFFSET $2",
                PAGINATION_LIMIT, offset
            )
            total_users = await conn.fetchval("SELECT COUNT(*) FROM authorized_users")
        
        text = "👥 **Authorized Users**\n\n"
        for u in users:
            status = "✅" if u['is_active'] else "❌"
            text += f"`{u['user_id']}` - {u['first_name']} {status}\n"
            
        buttons = []
        nav_buttons = []
        if offset > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Back", callback_data=f"admin_list_users:{offset - PAGINATION_LIMIT}"))
        if total_users > offset + PAGINATION_LIMIT:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users:{offset + PAGINATION_LIMIT}"))
        if nav_buttons:
            buttons.append(nav_buttons)
        buttons.append([InlineKeyboardButton("« Back to Admin Panel", callback_data="admin_back")])

        if edit:
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)
        else:
            await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=ParseMode.MARKDOWN)


# === Health Check Server Implementation (NO CHANGES) ===
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz': self.send_response(200); self.send_header('Content-type', 'text/plain'); self.end_headers(); self.wfile.write(b"OK")
        else: self.send_response(404); self.end_headers(); self.wfile.write(b"Not Found")

def start_health_check_server():
    with socketserver.TCPServer(("", HEALTH_CHECK_PORT), HealthCheckHandler) as httpd:
        logger.info(f"Health check server serving on port {HEALTH_CHECK_PORT}")
        httpd.serve_forever()

###############################################################################
# 6 — MAIN APPLICATION RUNNER
###############################################################################
async def main_async():
    """Asynchronously sets up and runs the bot."""
    global DB_POOL
    logger.info("Starting High-Performance FileStore Bot...")
    if not all([BOT_TOKEN, BOT_USERNAME, DATABASE_URL, storage_id]):
        logger.critical("FATAL: Missing critical environment variables!")
        return
    if storage_id >= 0:
        logger.critical("FATAL: storage_id must be a negative number for a private channel!")
        return
    
    try:
        DB_POOL = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=3, max_size=10)
        await init_database()
        logger.info("Configuration validated and DB pool created successfully!")

        job_queue = JobQueue()
        application = ApplicationBuilder().token(BOT_TOKEN).job_queue(job_queue).build()
        
        bot = FileStoreBot(application)
        
        # Register all handlers
        application.add_handler(CommandHandler("start", bot.start_handler))
        application.add_handler(CommandHandler("help", bot.help_handler))
        application.add_handler(CommandHandler("clear", bot.clear_handler))
        application.add_handler(CommandHandler("upload", bot.upload_handler))
        application.add_handler(CommandHandler("bulkupload", bot.bulkupload_handler))
        application.add_handler(CommandHandler("groups", bot.groups_handler))
        application.add_handler(CommandHandler("getlink", bot.getlink_handler))
        application.add_handler(CommandHandler("deletefile", bot.deletefile_handler))
        application.add_handler(CommandHandler("deletegroup", bot.deletegroup_handler))
        application.add_handler(CommandHandler("getgrouplink", bot.getgrouplink_handler))
        application.add_handler(CommandHandler("revokelink", bot.revoke_link_handler))
        application.add_handler(CommandHandler("search", bot.search_handler))
        
        # Admin commands
        application.add_handler(CommandHandler("admin", bot.admin_panel_handler))
        application.add_handler(CommandHandler("adduser", bot.add_user_handler))
        application.add_handler(CommandHandler("removeuser", bot.remove_user_handler))
        application.add_handler(CommandHandler("listusers", bot.list_users_handler))
        application.add_handler(CommandHandler("botstats", bot.bot_stats_handler))
        
        application.add_handler(MessageHandler(
            (filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE | (filters.TEXT & ~filters.COMMAND)),
            bot.file_handler
        ))
        application.add_handler(CallbackQueryHandler(bot.callback_handler))

        logger.info(f"Bot @{BOT_USERNAME} started successfully!")
        
        async with application:
            await application.initialize()
            await application.start()
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            await asyncio.Event().wait()

    except Exception as e:
        logger.critical(f"Bot failed to start: {e}")
    finally:
        if DB_POOL: await DB_POOL.close()
        logger.info("Bot is shutting down.")

def main_sync():
    """Synchronous wrapper to run the health check server in a separate thread."""
    health_thread = threading.Thread(target=start_health_check_server, daemon=True)
    health_thread.start()
    
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")

if __name__ == "__main__":
    main_sync()