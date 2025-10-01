# filecloudsupabaseX.py - FULLY FIXED VERSION WITH ALL BUTTON ISSUES RESOLVED
import asyncio
import os
import uuid
import base64
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple, Any

# Imports for Health Check Server
import http.server
import socketserver
import threading

# Import psycopg2 for PostgreSQL (Supabase)
import psycopg2

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
)
from telegram.ext import (
    Application, ApplicationBuilder, ContextTypes,
    CommandHandler, MessageHandler, filters, CallbackQueryHandler,
    JobQueue # Import JobQueue explicitly for manual instantiation
)
from telegram.error import BadRequest # Import BadRequest for specific error handling

###############################################################################
# 1 — CONFIGURATION (MODIFIED TO USE ENVIRONMENT VARIABLES)
###############################################################################
BOT_TOKEN = os.environ.get("BOT_TOKEN") # Read from environment variable

STORAGE_CHANNEL_ID = os.environ.get("STORAGE_CHANNEL_ID")
if not STORAGE_CHANNEL_ID:
    raise RuntimeError("❌ STORAGE_CHANNEL_ID not found. Please set it in Railway Variables.")

STORAGE_CHANNEL_ID = int(STORAGE_CHANNEL_ID)
BOT_USERNAME = os.environ.get("BOT_USERNAME") # Read from environment variable

# Admin Configuration
# Split string by comma and convert to int for ADMIN_IDS
ADMIN_IDS = list(map(int, os.environ.get("ADMIN_IDS", "").split(','))) if os.environ.get("ADMIN_IDS") else []
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT") # Read from environment variable
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "t.me/movieandwebserieshub") # Read from env, with default

# Health Check Server Port
# Render typically exposes PORT via an environment variable.
# Use 8000 as a fallback for local testing if PORT is not set.
HEALTH_CHECK_PORT = int(os.environ.get("PORT", 8000))

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")  # Full PostgreSQL connection string

# Database and limits - UPGRADED TO 10GB LIMIT
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB

# Bulk Upload Delay (in seconds) - REDUCED
BULK_UPLOAD_DELAY = 0.5

###############################################################################
# 2 — ENHANCED LOGGING SYSTEM
###############################################################################
def clear_console():
    """Clear console screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def setup_logging():
    """Setup logging with Windows compatibility"""
    clear_console()

    logger = logging.getLogger("FileStoreBot")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler with UTF-8
    try:
        file_handler = logging.FileHandler('bot.log', encoding='utf-8')
        file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass

    # Console handler with safe emoji handling
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console_handler.setFormatter(console_formatter)

    # Safe emit function for Windows
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
# 3 — FIXED DATABASE INITIALIZATION (MODIFIED FOR SUPABASE/POSTGRESQL)
###############################################################################
def init_database():
    """Initialize PostgreSQL database with proper SQL syntax"""
    try:
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()

        # Create tables if not exists (adjusted for PostgreSQL with BIGINT where needed)
        logger.info("Creating authorized_users table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS authorized_users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                caption_disabled INTEGER DEFAULT 0
            )
        """)

        logger.info("Creating groups table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_files INTEGER DEFAULT 0,
                total_size BIGINT DEFAULT 0,
                UNIQUE(name, owner_id)
            )
        """)

        logger.info("Creating files table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id SERIAL PRIMARY KEY,
                group_id BIGINT NOT NULL,
                serial_number INTEGER NOT NULL,
                unique_id TEXT UNIQUE NOT NULL,
                file_name TEXT,
                file_type TEXT NOT NULL,
                file_size BIGINT DEFAULT 0,
                telegram_file_id TEXT NOT NULL,
                uploader_id BIGINT NOT NULL,
                uploader_username TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                storage_message_id BIGINT,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
                UNIQUE(group_id, serial_number)
            )
        """)

        logger.info("Creating file_links table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_links (
                id SERIAL PRIMARY KEY,
                link_code TEXT UNIQUE NOT NULL,
                file_id BIGINT,
                group_id BIGINT,
                link_type TEXT NOT NULL CHECK (link_type IN ('file', 'group')),
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                clicks BIGINT DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
            )
        """)

        logger.info("Creating bot_settings table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert default settings with ON CONFLICT
        logger.info("Inserting default bot settings...")
        cursor.execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ('caption_enabled', '1'))
        cursor.execute("INSERT INTO bot_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", ('custom_caption', CUSTOM_CAPTION))

        # Add admins to authorized users
        logger.info(f"Processing ADMIN_IDS: {ADMIN_IDS}")
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    admin_id_int = int(admin_id)  # Ensure it's an integer
                    logger.info(f"Inserting admin ID: {admin_id_int}")
                    cursor.execute("""
                        INSERT INTO authorized_users (user_id, username, first_name, added_by, is_active)
                        VALUES (%s, %s, %s, %s, 1) ON CONFLICT (user_id) DO NOTHING
                    """, (admin_id_int, f'admin_{admin_id_int}', f'Admin {admin_id_int}', admin_id_int))
                except ValueError as ve:
                    logger.error(f"Invalid admin ID {admin_id}: {ve}")
                except Exception as e:
                    logger.error(f"Error inserting admin ID {admin_id}: {e}")

        conn.commit()
        logger.info("Database initialized successfully")
        conn.close()

    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        raise e

###############################################################################
# 4 — UTILITY FUNCTIONS
###############################################################################
def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

def generate_id() -> str:
    """Generate short unique ID"""
    return base64.urlsafe_b64encode(uuid.uuid4().bytes)[:12].decode()

def format_size(size_bytes: int) -> str:
    """Format file size"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.1f} MB"
    else:
        return f"{size_bytes/(1024**3):.1f} GB"

def extract_file_data(message: Message) -> Tuple[Optional[Any], str, str, int]:
    """Extract file information from message"""
    if message.document:
        doc = message.document
        return doc, "document", doc.file_name or "document", doc.file_size or 0
    elif message.photo:
        photo = message.photo[-1]
        return photo, "photo", f"photo_{photo.file_id[:8]}.jpg", photo.file_size or 0
    elif message.video:
        video = message.video
        return video, "video", video.file_name or f"video_{video.file_id[:8]}.mp4", video.file_size or 0
    elif message.audio:
        audio = message.audio
        return audio, "audio", audio.file_name or f"audio_{audio.file_id[:8]}.mp3", audio.file_size or 0
    elif message.voice:
        voice = message.voice
        return voice, "voice", f"voice_{voice.file_id[:8]}.ogg", voice.file_size or 0
    elif message.video_note:
        vn = message.video_note
        return vn, "video_note", f"videonote_{vn.file_id[:8]}.mp4", vn.file_size or 0
    return None, "", "", 0

def get_caption_setting() -> tuple:
    """Get current caption settings from database"""
    try:
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT key, value FROM bot_settings
            WHERE key IN ('caption_enabled', 'custom_caption')
        """)
        settings = cursor.fetchall()
        conn.close()

        caption_enabled = True
        custom_caption = CUSTOM_CAPTION

        for key, value in settings:
            if key == 'caption_enabled':
                caption_enabled = value == '1'
            elif key == 'custom_caption':
                custom_caption = value

        return caption_enabled, custom_caption
    except Exception:
        return True, CUSTOM_CAPTION

def get_file_caption(file_name: str, serial_number: int = None, user_id: int = None) -> str:
    """Generate file caption with user-specific settings"""
    try:
        if user_id and not is_admin(user_id):
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT caption_disabled FROM authorized_users WHERE user_id = %s
            """, (user_id,))
            user_caption_disabled = cursor.fetchone()
            conn.close()

            if user_caption_disabled and user_caption_disabled[0]:
                return file_name

        caption_enabled, custom_caption = get_caption_setting()

        if not caption_enabled:
            return file_name

        if serial_number:
            return f"#{serial_number:03d} {file_name}\n\n{custom_caption}"
        else:
            return f"{file_name}\n\n{custom_caption}"
    except Exception:
        return file_name

def is_user_authorized(user_id: int) -> bool:
    """Check if user is authorized to use the bot"""
    if is_admin(user_id):
        return True

    try:
        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT is_active FROM authorized_users
            WHERE user_id = %s AND is_active = 1
        """, (user_id,))
        result = cursor.fetchone()
        conn.close()

        return result is not None
    except Exception:
        return False

###############################################################################
# 5 — MAIN BOT CLASS WITH COMPLETE WORKING FUNCTIONS
###############################################################################
class FileStoreBot:
    def __init__(self, application: Application):
        self.app = application
        self.bulk_sessions = {}
        self.caption_edit_pending = {} # To track pending caption edits
        init_database()

    # ================= COMMAND HANDLERS =================

    async def start_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command with authorization check"""
        user = update.effective_user

        # Handle deep-link access (anyone can access files)
        link_code = None
        if context.args:
            link_code = context.args[0]
        elif update.message and " " in update.message.text:
            link_code = update.message.text.split(maxsplit=1)[1]

        if link_code:
            await self._handle_link_access(update, context, link_code)
            return

        # Check authorization for bot usage
        if not is_user_authorized(user.id):
            keyboard = [[InlineKeyboardButton("Contact Admin 👨‍💻", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")]]
            await update.message.reply_text(
                f"Access Denied 🚫\n\n"
                f"You need permission to use this bot.\n\n"
                f"Contact Admin: {ADMIN_CONTACT}\n"
                f"Your User ID: {user.id}\n\n"
                f"Note: Anyone can access files through shared links! 🔗",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        # Show main menu
        await self._show_main_menu(update.message, user)

    async def clear_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /clear command - Admin only."""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        clear_console()
        logger.info("Console cleared by user command")
        await update.message.reply_text("Console Cleared ✅\n\nAll console logs have been cleared.")

    async def upload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /upload command"""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /upload <group_name>\n"
                "Example: /upload MyDocuments"
            )
            return

        group_name = " ".join(context.args)
        context.user_data['upload_mode'] = 'single'
        context.user_data['group_name'] = group_name

        keyboard = [[InlineKeyboardButton("Cancel Upload ❌", callback_data="cancel_upload")]]

        await update.message.reply_text(
            f"Single Upload Mode ⬆️\n\n"
            f"Group: {group_name} 📁\n"
            "Send me the file you want to upload.\n"
            "Supported: Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice \n"
            f"Max Size: {format_size(MAX_FILE_SIZE)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def bulkupload_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /bulkupload command"""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /bulkupload <group_name>\n"
                "Example: /bulkupload MyPhotos"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id
        session_id = generate_id()

        # Create bulk session with progress tracking
        self.bulk_sessions[user_id] = {
            'session_id': session_id,
            'group_name': group_name,
            'files': [],
            'total_size': 0,
            'started_at': datetime.now(),
            'progress_msg_id': None
        }

        keyboard = [
            [
                InlineKeyboardButton("Finish Upload ✅", callback_data="finish_bulk"),
                InlineKeyboardButton("Cancel Bulk ❌", callback_data="cancel_bulk")
            ]
        ]

        # Send initial progress message
        progress_msg = await update.message.reply_text(
            f"Bulk Upload Started 🚀\n\n"
            f"Group: {group_name} 📁\n"
            f"Session: {session_id}\n\n"
            f"Files: 0 | Total Size: 0 B\n"
            "Send multiple files one by one.\n"
            "Click Finish Upload when done.\n"
            f"Max Size per file: {format_size(MAX_FILE_SIZE)}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

        # Store progress message ID
        self.bulk_sessions[user_id]['progress_msg_id'] = progress_msg.message_id

    async def groups_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /groups command and 'My Groups' button with dynamic response."""
        message_to_send = update.message if update.message else update.callback_query.message
        user_id = update.effective_user.id

        if not is_user_authorized(user_id):
            await message_to_send.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, total_files, total_size, created_at
                FROM groups WHERE owner_id = %s
                ORDER BY created_at DESC LIMIT 20
            """, (user_id,))
            groups = cursor.fetchall()
            conn.close()

            text = ""
            keyboard = []

            if not groups:
                text = "No Groups Found 📂\n\n" \
                       "You haven't created any groups yet.\n" \
                       "Upload your first file to get started! ⬆️"
                keyboard = [[InlineKeyboardButton("Upload First File ⬆️", callback_data="cmd_upload")]]
            else:
                text = "Your File Groups 📂\n\n"
                for i, (group_id, name, files, size, created) in enumerate(groups):
                    created_str = created.strftime("%Y-%m-%d") if created else "N/A"  # Format datetime to string
                    text += f"{i+1}. {name}\n"
                    text += f"   {files} files, {format_size(size)}\n"
                    text += f"   {created_str}\n\n"

                    keyboard.append([
                        InlineKeyboardButton(f"View {name[:15]} ℹ️", callback_data=f"view_group_id_{group_id}"),
                        InlineKeyboardButton("Get Link 🔗", callback_data=f"link_group_id_{group_id}")
                    ])

            keyboard.append([InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")])

            if update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Groups handler error: {e}")
            if update.callback_query:
                await update.callback_query.edit_message_text("Error loading groups. Please try again. 😔")
            else:
                await update.message.reply_text("Error loading groups. Please try again. 😔")

    async def help_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        _, custom_caption = get_caption_setting()

        help_text = f"""Complete Command Reference 📚

Upload Commands:
/upload <group> - Upload single file ⬆️
/bulkupload <group> - Upload multiple files 📦

Delete Commands:
/deletefile <group> <file_no> - Delete specific file 🗑️
/deletegroup <group> - Delete entire group 💥

Link Commands:
/getlink <group> <file_no> - Get file link 🔗
/getgrouplink <group> - Get group link 🔗
/revokelink <link_code> - Revoke a specific link 🚫 (NEW!)

Info Commands:
/groups - List all your groups 📂
/clear - Clear console logs ✨
/start - Show main menu 🏠"""

        if is_admin(update.effective_user.id):
            help_text += f"""

Admin Commands 👑:
/admin - Admin panel ⚙️
/adduser <user_id> [username] - Add user ➕
/removeuser <user_id> - Remove user ➖
/listusers - List all users 👥
/botstats - Bot statistics 📊"""

        help_text += f"""

Supported Files:
Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice  (up to {format_size(MAX_FILE_SIZE)})

Branding: All files include {custom_caption}

Contact Admin: {ADMIN_CONTACT} 👨‍💻"""

        keyboard = [[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]]
        await update.message.reply_text(help_text, reply_markup=InlineKeyboardMarkup(keyboard))

    # ================= ADMIN COMMANDS =================

    async def admin_panel_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin panel command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        await self._show_admin_panel(update.message)

    async def add_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add user command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage: /adduser <user_id> [username]\n"
                "Example: /adduser 123456789 newuser"
            )
            return

        try:
            user_id = int(context.args[0])
            username = context.args[1] if len(context.args) > 1 else None
            first_name = update.message.from_user.first_name # Capture invoker's first name

            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Check if user already exists
            cursor.execute("SELECT user_id FROM authorized_users WHERE user_id = %s", (user_id,))
            existing = cursor.fetchone()

            if existing:
                conn.close()
                await update.message.reply_text(f"User {user_id} is already authorized! 👥")
                return

            # Add user
            cursor.execute("""
                INSERT INTO authorized_users (user_id, username, first_name, added_by, is_active)
                VALUES (%s, %s, %s, %s, 1)
            """, (user_id, username, first_name, update.effective_user.id))
            conn.commit()
            conn.close()

            await update.message.reply_text(
                f"User Added Successfully! ✅\n\n"
                f"User ID: {user_id}\n"
                f"Username: @{username or 'Unknown'}\n"
                f"Added by: {update.effective_user.first_name}"
            )

        except ValueError:
            await update.message.reply_text("Invalid user ID format 🔢")
        except Exception as e:
            logger.error(f"Add user error: {e}")
            await update.message.reply_text("Error adding user 😔")

    async def remove_user_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove user command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        if not context.args:
            await update.message.reply_text("Usage: /removeuser <user_id>")
            return

        try:
            user_id = int(context.args[0])

            if user_id in ADMIN_IDS:
                await update.message.reply_text("Cannot remove admin users! 👑")
                return

            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM authorized_users WHERE user_id = %s", (user_id,))
            rowcount = cursor.rowcount
            conn.commit()

            if rowcount > 0:
                await update.message.reply_text(f"User {user_id} removed successfully! ➖")
            else:
                await update.message.reply_text(f"User {user_id} not found 🤷‍♂️")

            conn.close()

        except ValueError:
            await update.message.reply_text("Invalid user ID format 🔢")
        except Exception as e:
            logger.error(f"Remove user error: {e}")
            await update.message.reply_text("Error removing user 😔")

    async def list_users_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List users command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, username, first_name, added_at, is_active, caption_disabled
                FROM authorized_users WHERE user_id NOT IN (%s, %s)
                ORDER BY added_at DESC
            """, (ADMIN_IDS[0] if ADMIN_IDS else 0, ADMIN_IDS[1] if len(ADMIN_IDS) > 1 else 0))
            users = cursor.fetchall()
            conn.close()

            if not users:
                await update.message.reply_text("No regular users found 👥")
                return

            text = "Authorized Users 👥\n\n"

            for user_id, username, first_name, added_at, is_active, caption_disabled in users:
                status = "Active ✅" if is_active else "Inactive ❌"
                caption_status = "No Caption 🚫" if caption_disabled else "With Caption ✅"

                added_at_str = added_at.strftime("%Y-%m-%d") if added_at else "N/A"  # Format datetime to string

                text += f"{first_name or 'Unknown'}\n"
                text += f"ID: {user_id}\n"
                text += f"@{username or 'None'}\n"
                text += f"Status: {status}\n"
                text += f"Caption: {caption_status}\n"
                text += f"Added: {added_at_str}\n\n"

            # Split message if too long
            if len(text) > 4000:
                messages = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for msg in messages:
                    await update.message.reply_text(msg)
            else:
                await update.message.reply_text(text)

        except Exception as e:
            logger.error(f"List users error: {e}")
            await update.message.reply_text("Error loading users 😔")

    async def bot_stats_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Bot statistics command"""
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("Unauthorized: Admin access required 🚫")
            return

        await self._show_detailed_stats(update.message)

    async def getlink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /getlink command to get a specific file link."""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /getlink <group_name> <file_number>\n"
                "Example: /getlink MyDocuments 001"
            )
            return

        group_name = context.args[0]
        try:
            file_serial_number = int(context.args[1])
            if file_serial_number <= 0:
                await update.message.reply_text("File number must be positive. 🔢")
                return
        except ValueError:
            await update.message.reply_text("Invalid file number. Please provide a positive integer. 🔢")
            return

        user_id = update.effective_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Find the group and file
            cursor.execute("""
                SELECT f.id, f.file_name, fl.link_code
                FROM files f
                JOIN groups g ON f.group_id = g.id
                LEFT JOIN file_links fl ON f.id = fl.file_id AND fl.link_type = 'file' AND fl.owner_id = %s AND fl.is_active = 1
                WHERE g.name = %s AND f.serial_number = %s AND g.owner_id = %s
            """, (user_id, group_name, file_serial_number, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                await update.message.reply_text(
                    f"File #{file_serial_number:03d} not found in group '{group_name}' or you don't own it. 🤷‍♂️"
                )
                conn.close()
                return

            file_id, file_name, existing_link_code = file_info
            link_code = existing_link_code

            if not link_code:
                # Generate new link if it doesn't exist
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, file_id, owner_id, is_active)
                    VALUES (%s, 'file', %s, %s, 1)
                """, (link_code, file_id, user_id))
                conn.commit()

            conn.close()

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share File 🔗", url=share_link)],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]

            await update.message.reply_text(
                f"Link for '{file_name}' (Group: {group_name} 📁, #{file_serial_number:03d}):\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error getting file link: {e}")
            await update.message.reply_text("An error occurred while getting the file link. Please try again. 😔")

    # === NEW COMMANDS IMPLEMENTATION ===

    async def deletefile_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /deletefile command to delete a specific file."""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /deletefile <group_name> <file_number>\n"
                "Example: /deletefile MyDocuments 001"
            )
            return

        group_name = context.args[0]
        try:
            file_serial_number = int(context.args[1])
            if file_serial_number <= 0:
                await update.message.reply_text("File number must be positive. 🔢")
                return
        except ValueError:
            await update.message.reply_text("Invalid file number. Please provide a positive integer. 🔢")
            return

        user_id = update.effective_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Find the file to delete
            cursor.execute("""
                SELECT f.id, f.file_name, f.file_size, f.group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE g.name = %s AND f.serial_number = %s AND g.owner_id = %s
            """, (group_name, file_serial_number, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                await update.message.reply_text(
                    f"File #{file_serial_number:03d} not found in group '{group_name}' or you don't own it. 🤷‍♂️"
                )
                conn.close()
                return

            file_id, file_name, file_size, group_id = file_info

            # Confirm deletion with user
            keyboard = [
                [InlineKeyboardButton("Yes, Delete ✅", callback_data=f"confirm_delete_file_{file_id}")],
                [InlineKeyboardButton("No, Cancel ❌", callback_data="main_menu")]
            ]
            await update.message.reply_text(
                f"Are you sure you want to delete '{file_name}' (File #{file_serial_number:03d}) from group '{group_name}'? 🗑️\n"
                "This action cannot be undone. All associated links will also be removed.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            conn.close() # Close connection after query

        except Exception as e:
            logger.error(f"Error handling deletefile command: {e}")
            await update.message.reply_text("An error occurred while trying to delete the file. Please try again. 😔")

    async def deletegroup_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /deletegroup command to delete an entire group."""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /deletegroup <group_name>\n"
                "Example: /deletegroup OldProject"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM groups WHERE name = %s AND owner_id = %s", (group_name, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                await update.message.reply_text(
                    f"Group '{group_name}' not found or you don't own it. 🤷‍♂️"
                )
                conn.close()
                return

            group_id = group_info[0]

            # Confirm deletion with user
            keyboard = [
                [InlineKeyboardButton("Yes, Delete Entire Group ✅", callback_data=f"confirm_delete_group_{group_id}")],
                [InlineKeyboardButton("No, Cancel ❌", callback_data="main_menu")]
            ]
            await update.message.reply_text(
                f"Are you sure you want to delete the entire group '{group_name}'? 💥\n"
                "This will permanently delete ALL files and links within this group.\n"
                "This action cannot be undone. ⚠️",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            conn.close() # Close connection after query

        except Exception as e:
            logger.error(f"Error handling deletegroup command: {e}")
            await update.message.reply_text("An error occurred while trying to delete the group. Please try again. 😔")

    async def getgrouplink_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /getgrouplink command to get a link for an entire group."""
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /getgrouplink <group_name>\n"
                "Example: /getgrouplink MyWebSeries"
            )
            return

        group_name = " ".join(context.args)
        user_id = update.effective_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Find the group
            cursor.execute("""
                SELECT id FROM groups WHERE name = %s AND owner_id = %s
            """, (group_name, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                await update.message.reply_text(
                    f"Group '{group_name}' not found or you don't own it. 🤷‍♂️"
                )
                conn.close()
                return

            group_id = group_info[0]

            # Check if group link already exists and is active
            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id, user_id))
            link_info = cursor.fetchone()

            link_code = link_info[0] if link_info else None

            if not link_code:
                # Generate new link if it doesn't exist
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, group_id, owner_id, is_active)
                    VALUES (%s, 'group', %s, %s, 1)
                """, (link_code, group_id, user_id))
                conn.commit()

            conn.close()

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share Group 🔗", url=share_link)],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]

            await update.message.reply_text(
                f"Link for group '{group_name}' 📁:\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error getting group link: {e}")
            await update.message.reply_text("An error occurred while getting the group link. Please try again. 😔")

    async def _execute_revoke_link(self, message: Message, link_code: str, user_id: int):
        """Helper to execute link revocation logic."""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            # Check if the link exists and belongs to the user or is an admin revoking any link
            cursor.execute("""
                SELECT id, link_type, file_id, group_id, owner_id FROM file_links
                WHERE link_code = %s AND is_active = 1
            """, (link_code,))
            link_info = cursor.fetchone()

            if not link_info:
                logger.info(f"Revocation failed: Link '{link_code}' not found or already inactive.")
                await message.reply_text(f"Link '{link_code}' not found or already inactive. 🤷‍♂️")
                conn.close()
                return

            link_db_id, link_type, file_id, group_id, link_owner_id = link_info

            # Only allow owner or admin to revoke
            if link_owner_id != user_id and not is_admin(user_id):
                logger.warning(f"Unauthorized revocation attempt: User {user_id} tried to revoke link {link_code} owned by {link_owner_id}.")
                await message.reply_text("You can only revoke your own links unless you are an admin. 🚫")
                conn.close()
                return

            # Invalidate the link
            cursor.execute("""
                UPDATE file_links SET is_active = 0 WHERE id = %s
            """, (link_db_id,))
            conn.commit()
            conn.close()

            logger.info(f"Link '{link_code}' (ID: {link_db_id}) successfully revoked by user {user_id}.")
            await message.reply_text(
                f"Link '{link_code}' has been successfully revoked. ✅\n"
                "It can no longer be used to access files."
            )
        except Exception as e:
            logger.error(f"Error executing link revocation for {link_code}: {e}")
            await message.reply_text("An error occurred while revoking the link. Please try again. 😔")

    async def revoke_link_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /revokelink command to invalidate a specific link."""
        # Check if user is authorized for bot usage (not just link access)
        if not is_user_authorized(update.effective_user.id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        if not context.args:
            await update.message.reply_text(
                "Usage Error ❌\n\n"
                "Correct usage: /revokelink <link_code>\n"
                "Example: /revokelink abcdef123456"
            )
            return

        link_code = context.args[0]
        user_id = update.effective_user.id
        await self._execute_revoke_link(update.effective_message, link_code, user_id)


    # ================= FILE HANDLER WITH ACTUAL PROCESSING =================

    async def file_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle file uploads with actual processing"""
        user_id = update.effective_user.id

        # Check for pending caption edit
        if user_id in self.caption_edit_pending and self.caption_edit_pending[user_id]['state'] == 'waiting_for_caption':
            new_caption = update.message.text
            if new_caption:
                await self._update_custom_caption(update, new_caption)
                return
            else:
                await update.message.reply_text("Please send the new custom caption text. To cancel, use /start. ✍️")
                return

        if not is_user_authorized(user_id):
            await update.message.reply_text(f"Unauthorized. Contact admin: {ADMIN_CONTACT} 🚫")
            return

        file_obj, file_type, file_name, file_size = extract_file_data(update.message)

        if not file_obj:
            await update.message.reply_text("Unsupported File Type 🚫\n\nSupported: Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice 🎤")
            return

        if file_size > MAX_FILE_SIZE:
            await update.message.reply_text(
                f"File Too Large 🐘\n\n"
                f"Maximum: {format_size(MAX_FILE_SIZE)}\n"
                f"Your file: {format_size(file_size)}"
            )
            return

        # Check upload mode
        if user_id in self.bulk_sessions:
            await self._handle_bulk_file(update, context, file_obj, file_type, file_name, file_size)
        elif context.user_data.get('upload_mode') == 'single':
            await self._handle_single_file(update, context, file_obj, file_type, file_name, file_size)
        else:
            keyboard = [[InlineKeyboardButton("Start Upload ⬆️", callback_data="cmd_upload")]]
            await update.message.reply_text(
                "No Active Upload Session 🚫\n\nUse /upload <group> to start uploading files.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    # ================= COMPLETE CALLBACK HANDLER =================

    async def callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all callback queries with complete working functions"""
        query = update.callback_query
        await query.answer()

        data = query.data
        user_id = query.from_user.id
        logger.info(f"Callback received: {data} from user {user_id}")

        try:
            # Admin-only callbacks
            admin_callbacks = ["admin_panel", "user_management", "caption_settings", "bot_stats", "advanced_settings",
                               "toggle_global_caption", "edit_caption_text", "user_caption_control", "toggle_user_caption_",
                               "user_info_", "remove_user_", "confirm_remove_", "help_adduser", "list_all_users",
                               "full_stats", "export_stats", "usage_report"]

            if any(data.startswith(cb) for cb in admin_callbacks):
                if not is_admin(user_id):
                    await query.edit_message_text("Unauthorized: Admin access required 🚫")
                    return

            # Handle all callbacks with complete implementations
            if data == "main_menu":
                await self._show_main_menu_callback(query, user_id)

            elif data == "cmd_upload":
                await query.edit_message_text(
                    "Upload File ⬆️\n\nTo upload a file, use:\n/upload <group_name>\n\nExample:\n/upload MyDocuments",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "cmd_bulkupload":
                await query.edit_message_text(
                    "Bulk Upload 📦\n\nTo start bulk upload, use:\n/bulkupload <group_name>\n\nExample:\n/bulkupload MyPhotos",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "cmd_groups":
                # Re-call groups_handler for fresh list
                await self.groups_handler(update, context)

            elif data == "cmd_links":
                await self._show_my_links(query, user_id)

            elif data == "cmd_help":
                await query.edit_message_text(
                    "Help 📚\n\nFor complete help, use:\n/help",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "clear_console":
                # This button is only visible to admins in the main menu, but added explicit check here.
                if not is_admin(user_id):
                    await query.edit_message_text("Unauthorized: Admin access required 🚫")
                    return
                clear_console()
                logger.info("Console cleared via button")
                await query.edit_message_text(
                    "Console Cleared ✅\n\nAll console logs have been cleared.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "cancel_upload":
                context.user_data.clear()
                await query.edit_message_text(
                    "Upload Cancelled ❌\n\nYour upload session has been cancelled.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "cancel":
                # If a caption edit was pending, clear that state
                if user_id in self.caption_edit_pending:
                    del self.caption_edit_pending[user_id]
                await query.edit_message_text(
                    "Action Cancelled ❌",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

            elif data == "finish_bulk":
                await self._finish_bulk_upload(query, context)

            elif data == "cancel_bulk":
                await self._cancel_bulk_upload(query, context)

            # Admin panel callbacks
            elif data == "admin_panel":
                await self._show_admin_panel_callback(query)

            elif data == "user_management":
                await self._show_user_management_callback(query)

            elif data == "caption_settings":
                await self._show_caption_settings_callback(query)

            elif data == "bot_stats":
                await self._show_bot_stats_callback(query)

            elif data == "advanced_settings":
                await self._show_advanced_settings_callback(query)

            # Bot stats buttons
            elif data == "full_stats":
                await self._show_full_stats_callback(query)

            elif data == "export_stats":
                await self._export_stats_callback(query)

            elif data == "usage_report":
                await self._show_usage_report_callback(query)

            elif data == "refresh_stats":
                await self._show_bot_stats_callback(query)

            # Caption controls
            elif data == "toggle_global_caption":
                await self._toggle_global_caption(query)

            elif data == "edit_caption_text":
                await self._edit_caption_text_callback(query, context)

            elif data == "user_caption_control":
                await self._show_user_caption_control(query)

            elif data.startswith("toggle_user_caption_"):
                await self._toggle_user_caption(query, data)

            # User management callbacks
            elif data.startswith("user_info_"):
                await self._show_user_info(query, data)

            elif data.startswith("remove_user_"):
                await self._confirm_user_removal(query, data)

            elif data.startswith("confirm_remove_"):
                await self._execute_user_removal(query, data)

            elif data == "help_adduser":
                await query.edit_message_text(
                    "Add User Help ➕\n\nTo add a new user, use:\n/adduser <user_id> [username]\n\nExample:\n/adduser 123456789 john",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("User Management 👥", callback_data="user_management")]])
                )

            elif data == "list_all_users":
                await self._list_all_users_callback(query)

            # Group callbacks
            elif data.startswith("view_group_id_"):
                await self._handle_view_group(query, data)

            elif data.startswith("link_group_id_"):
                await self._handle_group_link(query, data)

            elif data.startswith("gen_group_link_"):
                await self._generate_specific_group_link(query, data)

            elif data.startswith("list_files_group_"):
                await self._list_group_files(query, data)

            elif data.startswith("view_file_id_"):
                await self._view_file_details(query, data)

            elif data.startswith("add_files_to_group_"):
                await self._prepare_add_files_to_group(query, context, data)

            elif data.startswith("delete_file_"):
                await self._confirm_delete_file(query, data)

            elif data.startswith("confirm_delete_file_"):
                await self._execute_delete_file(query, data)

            elif data.startswith("delete_group_id_"):
                await self._confirm_delete_group(query, data)

            elif data.startswith("confirm_delete_group_"):
                await self._execute_delete_group(query, data)

            # Handle revoke group link from button
            elif data.startswith("revoke_group_link_"):
                link_code = data.split("_")[-1]
                # Pass query.message directly as it's the message associated with the callback
                await self._execute_revoke_link(query.message, link_code, user_id)
            # Handle revoke file link from button
            elif data.startswith("revoke_file_link_"):
                link_code = data.split("_")[-1]
                # Pass query.message directly as it's the message associated with the callback
                await self._execute_revoke_link(query.message, link_code, user_id)

            else:
                await query.edit_message_text(
                    "Unknown action. Please try again. 😔",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )

        except Exception as e:
            logger.error(f"Callback error: {e}")
            # Ensure a response is sent even on general callback error
            if query.message:
                await query.message.edit_text(
                    "Error occurred. Please try again. 😔",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )
            else:
                # Fallback if query.message is also None (highly unlikely for a callback)
                logger.error("Failed to send error message for callback as query.message is None.")

    # ================= COMPLETE HELPER METHODS - ALL WORKING =================

    async def _show_main_menu(self, message, user):
        """Show main menu with safe formatting"""
        keyboard = []

        if is_admin(user.id):
            keyboard.extend([
                [
                    InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel"),
                    InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")
                ],
                [
                    InlineKeyboardButton("User Management 👥", callback_data="user_management"),
                    InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")
                ]
            ])

        keyboard.extend([
            [
                InlineKeyboardButton("Upload File ⬆️", callback_data="cmd_upload"),
                InlineKeyboardButton("Bulk Upload 📦", callback_data="cmd_bulkupload")
            ],
            [
                InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups"),
                InlineKeyboardButton("My Links 🔗", callback_data="cmd_links")
            ],
            [
                InlineKeyboardButton("Help 📚", callback_data="cmd_help"),
                InlineKeyboardButton("Clear Console ✨", callback_data="clear_console") if is_admin(user.id) else None
            ]
        ])

        keyboard = [row for row in keyboard if row]  # Remove None rows

        _, custom_caption = get_caption_setting()
        role = "Admin 👑" if is_admin(user.id) else "User"

        welcome_text = f"""Welcome to Enhanced FileStore Bot! 👋

Hello {user.first_name or 'User'}! ({role})

Features:
- Upload files to organized groups 📁
- Generate shareable links 🔗
- Auto-captioned uploads with serial numbers #️⃣
- Bulk upload support 🚀
- Files auto-delete after 10 minutes when shared ⏳
- Custom branding: {custom_caption}

File Size Limit: {format_size(MAX_FILE_SIZE)}

Quick Commands:
/upload <group> - Upload single file ⬆️
/bulkupload <group> - Upload multiple files 📦
/groups - View your groups 📂
/getlink <group> <file_no> - Get specific file link 📄🔗
/clear - Clear console logs ✨

Choose an option below to get started! 👇"""

        try:
            await message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Error showing main menu: {e}")
            await message.reply_text("Bot is ready! Use /help for commands. 📚", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_main_menu_callback(self, query, user_id: int):
        """Show main menu via callback"""
        keyboard = []

        if is_admin(user_id):
            keyboard.extend([
                [
                    InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel"),
                    InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")
                ],
                [
                    InlineKeyboardButton("User Management 👥", callback_data="user_management"),
                    InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")
                ]
            ])

        keyboard.extend([
            [
                InlineKeyboardButton("Upload File ⬆️", callback_data="cmd_upload"),
                InlineKeyboardButton("Bulk Upload 📦", callback_data="cmd_bulkupload")
            ],
            [
                InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups"),
                InlineKeyboardButton("My Links 🔗", callback_data="cmd_links")
            ],
            [
                InlineKeyboardButton("Help 📚", callback_data="cmd_help"),
                InlineKeyboardButton("Clear Console ✨", callback_data="clear_console") if is_admin(user_id) else None
            ]
        ])

        keyboard = [row for row in keyboard if row]  # Remove None rows

        role = "Admin 👑" if is_admin(user_id) else "User"

        await query.edit_message_text(
            f"Main Menu 🏠 ({role})\n\nFile Size Limit: {format_size(MAX_FILE_SIZE)}\n\nChoose an option: 👇",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ================= ACTUAL FILE PROCESSING METHODS =================

    async def _handle_single_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_obj, file_type: str, file_name: str, file_size: int):
        """Handle single file upload with actual processing and link generation"""
        try:
            user_id = update.effective_user.id # Fixed: Get user_id here directly
            group_name = context.user_data['group_name']

            # Show progress message
            try:
                processing_msg = await update.message.reply_text("Processing file upload... ⏳")
            except Exception as e:
                logger.error(f"Error sending progress message: {e}")
                processing_msg = None

            # Save to database
            file_id, serial_number = await self._save_file_to_db(
                user_id, group_name, file_obj, file_type, file_name, file_size
            )

            # Generate caption
            caption = get_file_caption(file_name, serial_number, user_id)

            # Upload to storage channel
            try:
                storage_msg = await self._send_to_storage(file_obj, file_type, caption)

                # Update storage message ID
                conn = psycopg2.connect(SUPABASE_URL)
                cursor = conn.cursor()
                cursor.execute("UPDATE files SET storage_message_id = %s WHERE id = %s", (storage_msg.message_id, file_id))
                conn.commit()
                conn.close()

            except Exception as e:
                logger.error(f"Error sending to storage: {e}")

            # Clear upload mode
            context.user_data.clear()

            # Send success message
            keyboard = [[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]]
            await update.message.reply_text(
                f"File uploaded successfully! ✅\n\n"
                f"Group: {group_name}\n"
                f"Serial: #{serial_number:03d}\n"
                f"Size: {format_size(file_size)}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

            if processing_msg:
                await processing_msg.delete()

        except Exception as e:
            logger.error(f"Single file upload error: {e}")
            await update.message.reply_text("Error uploading file. Please try again. 😔")

    async def _handle_bulk_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, file_obj, file_type: str, file_name: str, file_size: int):
        """Handle bulk file upload with live progress update"""
        user_id = update.effective_user.id
        session = self.bulk_sessions[user_id]

        if not session:
            await update.message.reply_text("No active bulk session. Use /bulkupload to start. 🚫")
            return

        try:
            # Show processing for this file
            processing_msg = await update.message.reply_text(f"Processing '{file_name}'... ⏳")

            # Save to database
            file_id, serial_number = await self._save_file_to_db(
                user_id, session['group_name'], file_obj, file_type, file_name, file_size
            )

            # Generate caption
            caption = get_file_caption(file_name, serial_number, user_id)

            # Upload to storage channel
            try:
                storage_msg = await self._send_to_storage(file_obj, file_type, caption)

                # Update storage message ID
                conn = psycopg2.connect(SUPABASE_URL)
                cursor = conn.cursor()
                cursor.execute("UPDATE files SET storage_message_id = %s WHERE id = %s", (storage_msg.message_id, file_id))
                conn.commit()
                conn.close()

            except Exception as e:
                logger.error(f"Error sending bulk file to storage: {e}")

            # Update session
            session['files'].append({
                'id': file_id,
                'name': file_name,
                'size': file_size
            })
            session['total_size'] += file_size

            # Update live progress message
            progress_msg_id = session.get('progress_msg_id')
            if progress_msg_id:
                progress_text = (
                    f"Bulk Upload Progress 📈\n\n"
                    f"Group: {session['group_name']} 📁\n"
                    f"Session: {session['session_id']}\n\n"
                    f"Files: {len(session['files'])} | Total Size: {format_size(session['total_size'])}\n"
                    f"Last: {file_name[:30]}{'...' if len(file_name) > 30 else ''} ({format_size(file_size)})\n\n"
                    "Send more files or click Finish Upload. ⬆️"
                )
                keyboard = [
                    [
                        InlineKeyboardButton("Finish Upload ✅", callback_data="finish_bulk"),
                        InlineKeyboardButton("Cancel Bulk ❌", callback_data="cancel_bulk")
                    ]
                ]
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=progress_msg_id,
                    text=progress_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )

            # Delete processing message
            await processing_msg.delete()

            # Reduced delay
            await asyncio.sleep(BULK_UPLOAD_DELAY)

        except Exception as e:
            logger.error(f"Bulk file upload error: {e}")
            await update.message.reply_text("Error uploading file in bulk. Please try again. 😔")

    async def _finish_bulk_upload(self, query, context):
        """Finish bulk upload and show summary"""
        user_id = query.from_user.id
        session = self.bulk_sessions.get(user_id)

        if not session:
            await query.edit_message_text("No active bulk session. 🤷‍♂️")
            return

        total_files = len(session['files'])
        total_size = session['total_size']

        # Update progress to final summary
        progress_msg_id = session['progress_msg_id']
        if progress_msg_id:
            summary_text = (
                f"Bulk Upload Completed! 🎉\n\n"
                f"Group: {session['group_name']} 📁\n"
                f"Session: {session['session_id']}\n\n"
                f"Total Files: {total_files}\n"
                f"Total Size: {format_size(total_size)}\n"
                f"Time: {(datetime.now() - session['started_at']).seconds}s\n\n"
                "All files uploaded successfully! ✅"
            )
            keyboard = [[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]]
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=progress_msg_id,
                text=summary_text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        # Clean up session
        del self.bulk_sessions[user_id]

        await query.answer("Bulk upload finished!")

    async def _cancel_bulk_upload(self, query, context):
        """Cancel bulk upload"""
        user_id = query.from_user.id
        session = self.bulk_sessions.get(user_id)

        if not session:
            await query.edit_message_text("No active bulk session. 🤷‍♂️")
            return

        # Update progress to cancelled
        progress_msg_id = session['progress_msg_id']
        if progress_msg_id:
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=progress_msg_id,
                text="Bulk Upload Cancelled ❌\n\nSession cleared.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
            )

        # Clean up session
        del self.bulk_sessions[user_id]

        await query.answer("Bulk upload cancelled!")

    async def _save_file_to_db(self, user_id: int, group_name: str, file_obj: Any, file_type: str, file_name: str, file_size: int) -> Tuple[int, int]:
        """Save file info to database and return file_id, serial_number"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Get or create group
            cursor.execute("""
                INSERT INTO groups (name, owner_id) 
                VALUES (%s, %s) 
                ON CONFLICT (name, owner_id) DO NOTHING
                RETURNING id
            """, (group_name, user_id))
            result = cursor.fetchone()
            if result:
                group_id = result[0]
            else:
                cursor.execute("SELECT id FROM groups WHERE name = %s AND owner_id = %s", (group_name, user_id))
                group_id = cursor.fetchone()[0]

            # Update group totals if new file
            cursor.execute("""
                UPDATE groups SET total_files = total_files + 1, total_size = total_size + %s WHERE id = %s
            """, (file_size, group_id))

            # Get next serial number
            cursor.execute("SELECT COALESCE(MAX(serial_number), 0) + 1 FROM files WHERE group_id = %s", (group_id,))
            serial_number = cursor.fetchone()[0]

            unique_id = generate_id()
            telegram_file_id = file_obj.file_id

            # Insert file
            cursor.execute("""
                INSERT INTO files (group_id, serial_number, unique_id, file_name, file_type, file_size, telegram_file_id, uploader_id, uploader_username)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (group_id, serial_number, unique_id, file_name, file_type, file_size, telegram_file_id, user_id, file_obj.from_user.username or ''))
            file_id = cursor.fetchone()[0]

            conn.commit()
            conn.close()

            logger.info(f"File saved: ID={file_id}, Serial={serial_number}, Group={group_id}")
            return file_id, serial_number

        except Exception as e:
            logger.error(f"Database save error: {e}")
            if 'conn' in locals():
                conn.close()
            raise e

    async def _send_to_storage(self, file_obj: Any, file_type: str, caption: str = None):
        """Send file to storage channel"""
        bot = self.app.bot
        try:
            if file_type == "photo":
                msg = await bot.send_photo(STORAGE_CHANNEL_ID, file_obj.file_id, caption=caption)
            elif file_type == "video":
                msg = await bot.send_video(STORAGE_CHANNEL_ID, file_obj.file_id, caption=caption)
            elif file_type == "audio":
                msg = await bot.send_audio(STORAGE_CHANNEL_ID, file_obj.file_id, caption=caption)
            elif file_type == "voice":
                msg = await bot.send_voice(STORAGE_CHANNEL_ID, file_obj.file_id)
            elif file_type == "video_note":
                msg = await bot.send_video_note(STORAGE_CHANNEL_ID, file_obj.file_id)
            else:  # document
                msg = await bot.send_document(STORAGE_CHANNEL_ID, file_obj.file_id, caption=caption, filename=file_obj.file_name)

            logger.info(f"File sent to storage: msg_id={msg.message_id}")
            return msg
        except Exception as e:
            logger.error(f"Storage send error: {e}")
            raise e

    # ================= LINK HANDLING WITH CONCURRENT FORWARDING =================

    async def _handle_link_access(self, update: Update, context: ContextTypes.DEFAULT_TYPE, link_code: str):
        """Handle link access with actual file forwarding"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Get link info
            cursor.execute("""
                SELECT fl.link_type, fl.file_id, fl.group_id, fl.is_active,
                       f.telegram_file_id, f.file_type, f.file_name, f.uploader_id,
                       g.name as group_name, f.id as file_db_id, g.id as group_db_id
                FROM file_links fl
                LEFT JOIN files f ON fl.file_id = f.id
                LEFT JOIN groups g ON fl.group_id = g.id
                WHERE fl.link_code = %s
            """, (link_code,))
            link_info = cursor.fetchone()

            if not link_info:
                logger.info(f"Link access failed for {link_code}: Link not found in DB.")
                await update.message.reply_text(
                    "Invalid or Expired Link 🚫\n\n"
                    "This link is no longer valid or has been removed."
                )
                conn.close()
                return

            link_type, file_id, group_id, is_active, telegram_file_id, file_type, file_name, uploader_id, group_name, file_db_id, group_db_id = link_info
            logger.info(f"Link {link_code} accessed. Type: {link_type}, Active: {is_active}")

            # Check if link is active
            if not is_active:
                logger.info(f"Link access failed for {link_code}: Link is inactive.")
                await update.message.reply_text(
                    "Invalid or Expired Link 🚫\n\n"
                    "This link has been revoked or is no longer active."
                )
                conn.close()
                return

            # Additional check: Ensure the referenced file/group still exists in the database
            if link_type == "file" and file_db_id is None:
                logger.warning(f"Link {link_code} (file type) points to a non-existent file_id {file_id}.")
                await update.message.reply_text(
                    "File not found 🚫\n\n"
                    "The file associated with this link may have been deleted."
                )
                conn.close()
                return
            elif link_type == "group" and group_db_id is None:
                logger.warning(f"Link {link_code} (group type) points to a non-existent group_id {group_id}.")
                await update.message.reply_text(
                    "Group not found 🚫\n\n"
                    "The group associated with this link may have been deleted."
                )
                conn.close()
                return

            # Update clicks
            cursor.execute("""
                UPDATE file_links SET clicks = clicks + 1 WHERE link_code = %s
            """, (link_code,))
            conn.commit()
            conn.close()
            logger.info(f"Link {link_code} clicks updated.")

            if link_type == "file":
                await self._forward_single_file(update, telegram_file_id, file_type, file_name, uploader_id)
            else:  # link_type == "group"
                await self._forward_group_files(update, group_id, group_name)

        except Exception as e:
            logger.error(f"Link access error for link code {link_code}: {e}")
            await update.message.reply_text("Error accessing file. Please try again. 😔")

    async def _forward_single_file(self, update: Update, telegram_file_id: str, file_type: str, file_name: str, uploader_id: int = None):
        """Forward single file with proper caption"""
        bot = self.app.bot
        chat_id = update.effective_chat.id

        try:
            caption = get_file_caption(file_name, user_id=uploader_id)

            if file_type == "photo":
                sent_msg = await bot.send_photo(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video":
                sent_msg = await bot.send_video(chat_id, telegram_file_id, caption=caption)
            elif file_type == "audio":
                sent_msg = await bot.send_audio(chat_id, telegram_file_id, caption=caption)
            elif file_type == "voice":
                sent_msg = await bot.send_voice(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video_note":
                sent_msg = await bot.send_video_note(chat_id, telegram_file_id)
            else:  # document
                sent_msg = await bot.send_document(chat_id, telegram_file_id, caption=caption)

            # Log when auto-delete job is scheduled
            logger.info(f"Scheduling auto-delete for single file msg_id: {sent_msg.message_id} in chat {chat_id}")
            self.app.job_queue.run_once(
                self._auto_delete,
                when=600,
                data={
                    'chat_id': chat_id,
                    'message_ids': [sent_msg.message_id, update.message.message_id]
                }
            )

            _, custom_caption = get_caption_setting()

            await update.message.reply_text(
                f"File Forwarded Successfully! ✅\n\n"
                f"File: {file_name}\n"
                f"Branded with: {custom_caption}\n\n"
                f"This message will auto-delete in 10 minutes. ⏳"
            )

        except Exception as e:
            logger.error(f"Forward single file error: {e}. Check bot permissions in chat {chat_id} and if file_id is valid.")
            await update.message.reply_text(f"Error forwarding file: {e}. File might be unavailable or bot lacks permissions. 😔")

    async def _forward_group_files(self, update: Update, group_id: int, group_name: str):
        """Forward all files in a group concurrently"""
        bot = self.app.bot
        chat_id = update.effective_chat.id
        message_ids = [update.message.message_id] # Include the user's command message for auto-deletion

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT telegram_file_id, file_type, file_name, serial_number, uploader_id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC
            """, (group_id,))
            files = cursor.fetchall()
            conn.close()

            if not files:
                await update.message.reply_text(f"Group '{group_name}' is empty or files are unavailable. 🤷‍♂️")
                return

            # Send header
            header_msg = await update.message.reply_text(
                f"Forwarding {len(files)} files from '{group_name}' 📦\n\n"
                f"Auto-delete in 10 minutes... ⏳"
            )
            message_ids.append(header_msg.message_id)

            # Prepare forwarding tasks for concurrency
            forwarding_tasks = []
            for telegram_file_id, file_type, file_name, serial_number, uploader_id in files:
                task = self._forward_single_file_concurrent(bot, chat_id, telegram_file_id, file_type, file_name, serial_number, uploader_id, message_ids)
                forwarding_tasks.append(task)

            # Run concurrently with semaphore to limit concurrency (e.g., 5 at a time to avoid rate limits)
            semaphore = asyncio.Semaphore(5)
            async def limited_forward(task):
                async with semaphore:
                    try:
                        await task
                    except Exception as e:
                        logger.error(f"Concurrent forward error for {file_name}: {e}")

            concurrent_tasks = [limited_forward(task) for task in forwarding_tasks]
            await asyncio.gather(*concurrent_tasks, return_exceptions=True)

            _, custom_caption = get_caption_setting()
            await update.message.reply_text(
                f"All {len(files)} files from '{group_name}' forwarded! ✅\n"
                f"Branded with: {custom_caption}\n\n"
                f"Auto-delete in 10 minutes. ⏳"
            )

            # Auto-delete all messages after 10 minutes
            if message_ids: # Only schedule if there are messages to delete
                logger.info(f"Scheduling auto-delete for group files in chat {chat_id}: {len(message_ids)} messages")
                self.app.job_queue.run_once(
                    self._auto_delete,
                    when=600, # 10 minutes
                    data={
                        'chat_id': chat_id,
                        'message_ids': message_ids
                    }
                )

        except Exception as e:
            logger.error(f"Overall group forward error for group {group_id} ({group_name}): {e}")
            await update.message.reply_text(f"An unexpected error occurred while processing group files: {e}. 😔")

    async def _forward_single_file_concurrent(self, bot, chat_id: int, telegram_file_id: str, file_type: str, file_name: str, serial_number: int, uploader_id: int, message_ids: list):
        """Concurrent version of single file forward, appends to message_ids if successful"""
        try:
            caption = get_file_caption(file_name, serial_number, uploader_id)

            if file_type == "photo":
                sent_msg = await bot.send_photo(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video":
                sent_msg = await bot.send_video(chat_id, telegram_file_id, caption=caption)
            elif file_type == "audio":
                sent_msg = await bot.send_audio(chat_id, telegram_file_id, caption=caption)
            elif file_type == "voice":
                sent_msg = await bot.send_voice(chat_id, telegram_file_id, caption=caption)
            elif file_type == "video_note":
                sent_msg = await bot.send_video_note(chat_id, telegram_file_id)
            else:  # document
                sent_msg = await bot.send_document(chat_id, telegram_file_id, caption=caption)

            message_ids.append(sent_msg.message_id)
            logger.info(f"Concurrently forwarded: {file_name} (msg_id: {sent_msg.message_id})")

            # Small delay to avoid overwhelming
            await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Concurrent forward failed for {file_name}: {e}")

    async def _auto_delete(self, context):
        """Auto-delete messages"""
        data = context.job.data
        chat_id = data['chat_id']
        message_ids = data['message_ids']
        logger.info(f"Auto-delete job triggered for chat {chat_id}. Messages to delete: {len(message_ids)}")

        for msg_id in message_ids:
            try:
                await self.app.bot.delete_message(chat_id, msg_id)
                logger.info(f"Successfully deleted message {msg_id} in chat {chat_id}")
            except BadRequest as e:
                # Catch specific Telegram API errors for better diagnosis
                if "Message to delete not found" in str(e) or "message can't be deleted" in str(e):
                    logger.warning(f"Failed to delete message {msg_id} in chat {chat_id}: {e}. Message likely already deleted or too old.")
                else:
                    logger.error(f"Telegram API error deleting message {msg_id} in chat {chat_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error deleting message {msg_id} in chat {chat_id}: {e}")

    async def _show_caption_settings_callback(self, query):
        """Show caption settings"""
        caption_enabled, custom_caption = get_caption_setting()
        status = "Enabled ✅" if caption_enabled else "Disabled ❌"

        await query.edit_message_text(
            f"Caption Settings ✍️\n\nGlobal Caption: {status}\nText: {custom_caption}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Toggle Global 🌐", callback_data="toggle_global_caption")],
                [InlineKeyboardButton("Edit Caption Text ✏️", callback_data="edit_caption_text")],
                [InlineKeyboardButton("User Specific Caption 👥", callback_data="user_caption_control")],
                [InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel")]
            ])
        )

    async def _toggle_global_caption(self, query):
        """Toggle global caption"""
        caption_enabled, custom_caption = get_caption_setting()
        new_status = not caption_enabled

        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("UPDATE bot_settings SET value = %s WHERE key = 'caption_enabled'", ('1' if new_status else '0',))
        conn.commit()
        conn.close()

        await query.edit_message_text(
            f"Caption {'Enabled ✅' if new_status else 'Disabled ❌'} Globally",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")]
            ])
        )

    async def _toggle_user_caption(self, query, data):
        """Toggle user caption"""
        user_id = int(data.split("_")[-1])

        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT caption_disabled, first_name FROM authorized_users WHERE user_id = %s", (user_id,))
        current = cursor.fetchone()

        if current:
            new_status = not current[0]
            cursor.execute("UPDATE authorized_users SET caption_disabled = %s WHERE user_id = %s", (new_status, user_id))
            conn.commit()

            await query.edit_message_text(
                f"Caption {'Disabled ❌' if new_status else 'Enabled ✅'} for {current[1] or 'User'}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("User Management 👥", callback_data="user_management")]
                ])
            )
        else:
            await query.edit_message_text("User not found 🤷‍♂️")

        conn.close()

    async def _show_advanced_settings_callback(self, query):
        """Show advanced settings - now functional placeholder"""
        text = """Advanced Settings 🔧

This section is for future advanced configurations.

Current Bot Version: 1.0
Developed by: [Your Name/Team]
Source: [Link to GitHub if open source]

No configurable options here yet.
        """
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel")]]))

    async def _edit_caption_text_callback(self, query, context):
        """Prompt admin to send new caption text"""
        user_id = query.from_user.id
        self.caption_edit_pending[user_id] = {'state': 'waiting_for_caption'}
        await query.edit_message_text(
            "Please send the new custom caption text you want to set. ✏️\n\n"
            "Example: `t.me/NewChannelLink`\n\n"
            "To cancel, type /start",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel")]])
        )

    async def _update_custom_caption(self, update: Update, new_caption: str):
        """Update the custom caption in the database"""
        user_id = update.effective_user.id
        if user_id in self.caption_edit_pending and self.caption_edit_pending[user_id]['state'] == 'waiting_for_caption':
            try:
                conn = psycopg2.connect(SUPABASE_URL)
                cursor = conn.cursor()
                cursor.execute("UPDATE bot_settings SET value = %s, updated_at = CURRENT_TIMESTAMP WHERE key = 'custom_caption'", (new_caption,))
                conn.commit()
                conn.close()
                del self.caption_edit_pending[user_id] # Clear state
                await update.message.reply_text(
                    f"Custom caption updated successfully to: ✅\n`{new_caption}`",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")]])
                )
            except Exception as e:
                logger.error(f"Error updating custom caption: {e}")
                await update.message.reply_text("Failed to update caption. Please try again. 😔")
        else:
            await update.message.reply_text("No pending caption edit session. 🚫")

    async def _show_user_caption_control(self, query):
        """Display list of users to toggle their caption settings"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, first_name, username, caption_disabled
                FROM authorized_users
                WHERE user_id NOT IN ({})
                ORDER BY first_name ASC
            """.format(','.join(['%s'] * len(ADMIN_IDS))), ADMIN_IDS)
            users = cursor.fetchall()
            conn.close()

            text = "User Specific Caption Control 👥\n\n"
            keyboard = []
            for user_id, first_name, username, caption_disabled in users:
                status = "OFF ❌" if caption_disabled else "ON ✅"
                display_name = first_name or username or str(user_id)
                text += f"- {display_name} (Caption: {status})\n"
                keyboard.append([
                    InlineKeyboardButton(f"{display_name} (Toggle {status})", callback_data=f"toggle_user_caption_{user_id}")
                ])

            keyboard.append([InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")])

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error displaying user caption control: {e}")
            await query.edit_message_text("Error loading user caption control. 😔")

    async def _show_user_info(self, query, data):
        """Show detailed information about a specific user."""
        user_id = int(data.split("_")[-1])
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, username, first_name, added_by, added_at, is_active, caption_disabled
                FROM authorized_users WHERE user_id = %s
            """, (user_id,))
            user_info = cursor.fetchone()
            conn.close()

            if user_info:
                u_id, username, first_name, added_by, added_at, is_active, caption_disabled = user_info

                added_at_str = added_at.strftime("%Y-%m-%d %H:%M") if added_at else "N/A"  # Format datetime to string

                # Fetch added_by_admin_name
                added_by_admin_name = "Unknown Admin"
                if added_by:
                    conn = psycopg2.connect(SUPABASE_URL)
                    cursor = conn.cursor()
                    cursor.execute("SELECT first_name FROM authorized_users WHERE user_id = %s", (added_by,))
                    admin_name_row = cursor.fetchone()
                    conn.close()
                    if admin_name_row:
                        added_by_admin_name = admin_name_row[0] or f"Admin {added_by}"

                text = f"""User Information ℹ️:
ID: {u_id}
First Name: {first_name or 'N/A'}
Username: @{username or 'N/A'}
Status: {'Active ✅' if is_active else 'Inactive ❌'}
Caption Enabled: {'Yes ✅' if not caption_disabled else 'No ❌'}
Added By: {added_by_admin_name} 👨‍💻
Added At: {added_at_str}"""

                keyboard = [
                    [InlineKeyboardButton("Toggle Caption ✍️", callback_data=f"toggle_user_caption_{user_id}")],
                    [InlineKeyboardButton("Remove User ➖", callback_data=f"remove_user_{user_id}")],
                    [InlineKeyboardButton("User Management 👥", callback_data="user_management")]
                ]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await query.edit_message_text("User not found. 🤷‍♂️")
        except Exception as e:
            logger.error(f"Error showing user info: {e}")
            await query.edit_message_text("Error retrieving user information. 😔")

    async def _confirm_user_removal(self, query, data):
        """Confirm user removal before execution."""
        user_id_to_remove = int(data.split("_")[-1])

        # Prevent removing admins via this menu
        if user_id_to_remove in ADMIN_IDS:
            await query.edit_message_text("Cannot remove an admin user from this panel. 👑",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("User Management 👥", callback_data="user_management")]]))
            return

        conn = psycopg2.connect(SUPABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT first_name FROM authorized_users WHERE user_id = %s", (user_id_to_remove,))
        user_name = cursor.fetchone()
        conn.close()

        display_name = user_name[0] if user_name else f"User {user_id_to_remove}"

        await query.edit_message_text(
            f"Are you sure you want to remove {display_name} (ID: {user_id_to_remove})? ❓\n"
            "This action cannot be undone. ⚠️",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Yes, Remove User ✅", callback_data=f"confirm_remove_{user_id_to_remove}")],
                [InlineKeyboardButton("No, Cancel ❌", callback_data="user_management")]
            ])
        )

    async def _execute_user_removal(self, query, data):
        """Execute user removal after confirmation."""
        user_id_to_remove = int(data.split("_")[-1])

        # Double check to prevent removing admins
        if user_id_to_remove in ADMIN_IDS:
            await query.edit_message_text("Cannot remove an admin user. 👑",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("User Management 👥", callback_data="user_management")]]))
            return

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM authorized_users WHERE user_id = %s", (user_id_to_remove,))
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()

            if rowcount > 0:
                await query.edit_message_text(
                    f"User {user_id_to_remove} has been successfully removed. ✅",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("User Management 👥", callback_data="user_management")]])
                )
            else:
                await query.edit_message_text(
                    f"User {user_id_to_remove} was not found or already removed. 🤷‍♂️",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("User Management 👥", callback_data="user_management")]])
                )
        except Exception as e:
            logger.error(f"Error executing user removal: {e}")
            await query.edit_message_text("An error occurred while removing the user. 😔")

    async def _list_all_users_callback(self, query):
        """List all authorized users with pagination if needed."""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            if ADMIN_IDS:
                placeholders = ','.join(['%s'] * len(ADMIN_IDS))
                cursor.execute(f"""
                    SELECT user_id, username, first_name, added_at, is_active, caption_disabled
                    FROM authorized_users WHERE user_id NOT IN ({placeholders})
                    ORDER BY added_at DESC
                """, ADMIN_IDS)
            else:
                cursor.execute("""
                    SELECT user_id, username, first_name, added_at, is_active, caption_disabled
                    FROM authorized_users
                    ORDER BY added_at DESC
                """)
            users = cursor.fetchall()
            conn.close()

            text = "All Authorized Users 📜:\n\n"
            keyboard = []
            if not users:
                text = "No regular users found. 🤷‍♂️"
                keyboard = [[InlineKeyboardButton("User Management 👥", callback_data="user_management")]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                return

            for user_id, username, first_name, added_at, is_active, caption_disabled in users:
                status = "Active ✅" if is_active else "Inactive ❌"
                caption_status = "No Caption 🚫" if caption_disabled else "With Caption ✅"

                added_at_str = added_at.strftime("%Y-%m-%d") if added_at else "N/A"  # Format datetime to string

                text += (f"{first_name or 'Unknown'} (ID: {user_id})\n"
                         f"@{username or 'None'} | Status: {status} | Caption: {caption_status}\n"
                         f"Added: {added_at_str}\n\n")

            # Telegram message limit is 4096 characters for text messages.
            # Split and send if too long.
            if len(text) > 4000:
                chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        await query.edit_message_text(chunk, reply_markup=InlineKeyboardMarkup(keyboard))
                    else:
                        await query.message.reply_text(chunk)
            else:
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error listing all users: {e}")
            await query.edit_message_text("Error retrieving all users. 😔")

    async def _handle_view_group(self, query, data):
        """Display details of a selected group, including a list of its files."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name, total_files, total_size, created_at
                FROM groups WHERE id = %s AND owner_id = %s
            """, (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            name, total_files, total_size, created_at = group_info

            created_at_str = created_at.strftime("%Y-%m-%d") if created_at else "N/A"  # Format datetime to string

            text = f"""Group Details: {name} ℹ️
Total Files: {total_files} 📄
Total Size: {format_size(total_size)}
Created On: {created_at_str} 🗓️

Files in this group (first 10):"""

            cursor.execute("""
                SELECT serial_number, file_name, file_size, id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC LIMIT 10
            """, (group_id,))
            files = cursor.fetchall()

            # Get the active group link for this group, if it exists
            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id, user_id))
            group_link_info = cursor.fetchone()
            conn.close()

            group_link_code = group_link_info[0] if group_link_info else None

            if files:
                for serial_number, file_name, file_size, file_id in files:
                    text += f"\n- #{serial_number:03d} {file_name} ({format_size(file_size)})"

                if total_files > 10:
                    text += "\n\n... and more. Use 'List All Files' to see full list. 📜"

            else:
                text += "\nNo files in this group yet. 🤷‍♂️"

            keyboard = [
                [InlineKeyboardButton("List All Files 📜", callback_data=f"list_files_group_{group_id}")],
                [InlineKeyboardButton("Add More Files ➕", callback_data=f"add_files_to_group_{group_id}")],
                [InlineKeyboardButton("Get Group Link 🔗", callback_data=f"link_group_id_{group_id}")],
                [InlineKeyboardButton("Delete Group 💥", callback_data=f"delete_group_id_{group_id}")],
            ]

            # Add revoke button if a group link exists
            if group_link_code:
                keyboard.append([InlineKeyboardButton("Revoke Group Link 🚫", callback_data=f"revoke_group_link_{group_link_code}")])

            keyboard.append([InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")])

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error viewing group details: {e}")
            await query.edit_message_text("Error loading group details. 😔")

    async def _handle_group_link(self, query, data):
        """Generate and provide the shareable link for a group."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Find the group
            cursor.execute("""
                SELECT name FROM groups WHERE id = %s AND owner_id = %s
            """, (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                await query.edit_message_text("Group not found. 🤷‍♂️",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            group_name = group_info[0]

            # Check if group link already exists and is active
            cursor.execute("""
                SELECT link_code FROM file_links
                WHERE group_id = %s AND owner_id = %s AND link_type = 'group' AND is_active = 1
            """, (group_id, user_id))
            link_info = cursor.fetchone()

            link_code = link_info[0] if link_info else None

            if not link_code:
                # Generate new link if it doesn't exist
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, group_id, owner_id, is_active)
                    VALUES (%s, 'group', %s, %s, 1)
                """, (link_code, group_id, user_id))
                conn.commit()

            conn.close()

            share_link = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
            keyboard = [
                [InlineKeyboardButton("Share Group 🔗", url=share_link)],
                [InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}")],
                [InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]
            ]

            await query.edit_message_text(
                f"Link for group '{group_name}' 📁:\n\n"
                f"{share_link}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except Exception as e:
            logger.error(f"Error generating group link: {e}")
            await query.edit_message_text("Error generating group link. Please try again. 😔")

    async def _generate_specific_group_link(self, query, data):
        """This function is a direct call for generating a group link, similar to _handle_group_link but with a specific callback data."""
        await self._handle_group_link(query, data)

    async def _list_group_files(self, query, data):
        """List all files in a specified group."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id, user_id))
            group_info = cursor.fetchone()

            if not group_info:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            group_name = group_info[0]

            cursor.execute("""
                SELECT serial_number, file_name, file_size, id
                FROM files WHERE group_id = %s
                ORDER BY serial_number ASC
            """, (group_id,))
            files = cursor.fetchall()
            conn.close()

            if not files:
                await query.edit_message_text(f"Group '{group_name}' has no files. 🤷‍♂️",
                                              reply_markup=InlineKeyboardMarkup([
                                                  [InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}")],
                                                  [InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]
                                              ]))
                return

            text = f"Files in Group: {group_name} 📄\n\n"
            keyboard = []
            for serial_number, file_name, file_size, file_id in files:
                text += f"#{serial_number:03d} {file_name} ({format_size(file_size)})\n"
                keyboard.append([InlineKeyboardButton(f"#{serial_number:03d} {file_name[:25]}", callback_data=f"view_file_id_{file_id}")])

            keyboard.append([
                InlineKeyboardButton("View Group Details ℹ️", callback_data=f"view_group_id_{group_id}"),
                InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")
            ])

            # Send message, splitting if too long
            if len(text) > 4000:
                chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        await query.edit_message_text(chunk, reply_markup=InlineKeyboardMarkup(keyboard))
                    else:
                        await query.message.reply_text(chunk)
            else:
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error listing group files: {e}")
            await query.edit_message_text("Error retrieving group files. 😔")

    async def _view_file_details(self, query, data):
        """View details of a specific file."""
        file_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.file_name, f.file_type, f.file_size, f.uploaded_at, f.serial_number,
                       g.name as group_name, f.telegram_file_id, g.id as group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                await query.edit_message_text("File not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            file_name, file_type, file_size, uploaded_at, serial_number, group_name, telegram_file_id, group_id = file_info

            uploaded_at_str = uploaded_at.strftime("%Y-%m-%d %H:%M") if uploaded_at else "N/A"  # Format datetime to string

            # Get or create file specific link
            cursor.execute("""
                SELECT link_code FROM file_links WHERE file_id = %s AND link_type = 'file' AND owner_id = %s AND is_active = 1
            """, (file_id, user_id))
            file_link_row = cursor.fetchone()

            file_link_text = "N/A"
            share_button = []
            revoke_button = []
            if file_link_row:
                link_code = file_link_row[0]
                file_link_text = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
                share_button = [InlineKeyboardButton("Share File Link 🔗", url=file_link_text)]
                revoke_button = [InlineKeyboardButton("Revoke File Link 🚫", callback_data=f"revoke_file_link_{link_code}")]
            else:
                # If no link exists, create one
                link_code = generate_id()
                cursor.execute("""
                    INSERT INTO file_links (link_code, link_type, file_id, owner_id, is_active)
                    VALUES (%s, 'file', %s, %s, 1)
                """, (link_code, file_id, user_id))
                conn.commit() # Commit the new link creation
                file_link_text = f"https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}"
                share_button = [InlineKeyboardButton("Share File Link 🔗", url=file_link_text)]
                revoke_button = [InlineKeyboardButton("Revoke File Link 🚫", callback_data=f"revoke_file_link_{link_code}")]

            conn.close() # Close connection after all DB operations

            text = f"""File Details ℹ️:
Name: {file_name}
Group: {group_name} 📁
Serial No: #{serial_number:03d}
Type: {file_type.capitalize()}
Size: {format_size(file_size)}
Uploaded: {uploaded_at_str} 🗓️

File Link: {file_link_text}"""

            keyboard = [
                share_button,
                revoke_button,
                [InlineKeyboardButton("Delete File 🗑️", callback_data=f"delete_file_{file_id}")],
                [InlineKeyboardButton("Back to Group Files 📜", callback_data=f"list_files_group_{group_id}")],
                [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
            ]
            keyboard = [row for row in keyboard if row] # Remove empty sublists

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Error viewing file details: {e}")
            await query.edit_message_text("Error retrieving file details. 😔")

    async def _confirm_delete_file(self, query, data):
        """Confirm file deletion before execution."""
        file_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.file_name, g.name, g.id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id_to_delete, user_id))
            file_info = cursor.fetchone()
            conn.close()

            if file_info:
                file_name, group_name, group_id = file_info
                await query.edit_message_text(
                    f"Are you sure you want to delete '{file_name}' from group '{group_name}'? 🗑️\n"
                    "This action cannot be undone. ⚠️",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Yes, Delete File ✅", callback_data=f"confirm_delete_file_{file_id_to_delete}")],
                        [InlineKeyboardButton("No, Cancel ❌", callback_data=f"view_file_id_{file_id_to_delete}")]
                    ])
                )
            else:
                await query.edit_message_text("File not found or you don't have permission to delete it. 🚫")
        except Exception as e:
            logger.error(f"Error confirming file deletion: {e}")
            await query.edit_message_text("An error occurred while preparing for file deletion. 😔")

    async def _execute_delete_file(self, query, data):
        """Execute file deletion after confirmation."""
        file_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Get file info before deleting to update group stats
            cursor.execute("""
                SELECT f.file_name, f.file_size, f.group_id
                FROM files f
                JOIN groups g ON f.group_id = g.id
                WHERE f.id = %s AND g.owner_id = %s
            """, (file_id_to_delete, user_id))
            file_info = cursor.fetchone()

            if not file_info:
                await query.edit_message_text("File not found or you don't have permission to delete it. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            file_name, file_size, group_id = file_info

            # Delete file record
            cursor.execute("DELETE FROM files WHERE id = %s", (file_id_to_delete,))
            rowcount = cursor.rowcount

            if rowcount > 0:
                # Update group statistics
                cursor.execute("""
                    UPDATE groups SET total_files = total_files - 1, total_size = total_size - %s
                    WHERE id = %s
                """, (file_size, group_id))

                conn.commit()
                conn.close()

                await query.edit_message_text(
                    f"File '{file_name}' deleted successfully! ✅",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Group Files 📜", callback_data=f"list_files_group_{group_id}")]])
                )
            else:
                conn.close()
                await query.edit_message_text(
                    f"File '{file_name}' not found or could not be deleted. 🤷‍♂️",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                )
        except Exception as e:
            logger.error(f"Error executing file deletion: {e}")
            await query.edit_message_text("An error occurred while deleting the file. 😔")

    async def _confirm_delete_group(self, query, data):
        """Confirm group deletion before execution."""
        group_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id_to_delete, user_id))
            group_name_row = cursor.fetchone()
            conn.close()

            if group_name_row:
                group_name = group_name_row[0]
                await query.edit_message_text(
                    f"Are you sure you want to delete the entire group '{group_name}'? 💥\n"
                    "This will delete all files and links associated with this group.\n"
                    "This action cannot be undone. ⚠️",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Yes, Delete Group ✅", callback_data=f"confirm_delete_group_{group_id_to_delete}")],
                        [InlineKeyboardButton("No, Cancel ❌", callback_data=f"view_group_id_{group_id_to_delete}")]
                    ])
                )
            else:
                await query.edit_message_text("Group not found or you don't have permission to delete it. 🚫")
        except Exception as e:
            logger.error(f"Error confirming group deletion: {e}")
            await query.edit_message_text("An error occurred while preparing for group deletion. 😔")

    async def _execute_delete_group(self, query, data):
        """Execute group deletion after confirmation."""
        group_id_to_delete = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()

            # Verify ownership before deleting
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id_to_delete, user_id))
            group_name_row = cursor.fetchone()

            if not group_name_row:
                await query.edit_message_text("Group not found or you don't have permission to delete it. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                conn.close()
                return

            group_name = group_name_row[0]

            # Delete group record. ON DELETE CASCADE will handle files and links.
            cursor.execute("DELETE FROM groups WHERE id = %s", (group_id_to_delete,))
            rowcount = cursor.rowcount

            if rowcount > 0:
                conn.commit()
                conn.close()

                await query.edit_message_text(
                    f"Group '{group_name}' and all its contents deleted successfully! ✅",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                )
            else:
                conn.close()
                await query.edit_message_text(
                    f"Group '{group_name}' not found or could not be deleted. 🤷‍♂️",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]])
                )
        except Exception as e:
            logger.error(f"Error executing group deletion: {e}")
            await query.edit_message_text("An error occurred while deleting the group. 😔")

    async def _prepare_add_files_to_group(self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Prepares the bot for adding multiple files to an existing group via a bulk session."""
        group_id = int(data.split("_")[-1])
        user_id = query.from_user.id

        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM groups WHERE id = %s AND owner_id = %s", (group_id, user_id))
            group_info = cursor.fetchone()
            conn.close()

            if not group_info:
                await query.edit_message_text("Group not found or you don't have access. 🚫",
                                              reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("My Groups 📂", callback_data="cmd_groups")]]))
                return

            group_name = group_info[0]
            session_id = generate_id() # Generate a session ID for the bulk upload

            # Start a new bulk session for adding files to this existing group
            self.bulk_sessions[user_id] = {
                'session_id': session_id,
                'group_name': group_name,
                'files': [],
                'total_size': 0,
                'started_at': datetime.now(),
                'progress_msg_id': None
            }

            keyboard = [
                [
                    InlineKeyboardButton("Finish Upload ✅", callback_data="finish_bulk"),
                    InlineKeyboardButton("Cancel Bulk ❌", callback_data="cancel_bulk")
                ]
            ]

            # Send initial progress message
            progress_msg = await query.message.reply_text(
                f"Bulk Add Files Started 🚀\n\n"
                f"Group: {group_name} 📁\n"
                f"Session: {session_id}\n\n"
                f"Files: 0 | Total Size: 0 B\n"
                "Send multiple files one by one to add them to this group.\n"
                "Supported: Photos 📸, Videos 🎬, Documents 📄, Audio 🎵, Voice \n"
                f"Max Size per file: {format_size(MAX_FILE_SIZE)}\n\n"
                "Click 'Finish Upload' when done.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

            # Store progress message ID
            self.bulk_sessions[user_id]['progress_msg_id'] = progress_msg.message_id

        except Exception as e:
            logger.error(f"Error preparing to add files to group: {e}")
            await query.edit_message_text("An error occurred while preparing to add files. 😔")

    async def _show_admin_panel(self, message):
        """Show admin panel"""
        keyboard = [
            [InlineKeyboardButton("User Management 👥", callback_data="user_management")],
            [InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")],
            [InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")],
            [InlineKeyboardButton("Advanced Settings 🔧", callback_data="advanced_settings")],
            [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
        ]
        await message.reply_text("Admin Panel ⚙️\n\nChoose an option:", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_admin_panel_callback(self, query):
        """Show admin panel via callback"""
        keyboard = [
            [InlineKeyboardButton("User Management 👥", callback_data="user_management")],
            [InlineKeyboardButton("Caption Settings ✍️", callback_data="caption_settings")],
            [InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")],
            [InlineKeyboardButton("Advanced Settings 🔧", callback_data="advanced_settings")],
            [InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]
        ]
        await query.edit_message_text("Admin Panel ⚙️\n\nChoose an option:", reply_markup=InlineKeyboardMarkup(keyboard))

    async def _show_user_management_callback(self, query):
        """Show user management"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            if ADMIN_IDS:
                placeholders = ','.join(['%s'] * len(ADMIN_IDS))
                cursor.execute(f"""
                    SELECT user_id, first_name, username, is_active, caption_disabled
                    FROM authorized_users WHERE user_id NOT IN ({placeholders})
                    ORDER BY first_name ASC
                """, ADMIN_IDS)
            else:
                cursor.execute("""
                    SELECT user_id, first_name, username, is_active, caption_disabled
                    FROM authorized_users
                    ORDER BY first_name ASC
                """)
            users = cursor.fetchall()
            conn.close()

            text = "User Management 👥\n\n"
            keyboard = []
            if users:
                for user_id, first_name, username, is_active, caption_disabled in users:
                    status = "Active ✅" if is_active else "Inactive ❌"
                    caption_status = "No Caption 🚫" if caption_disabled else "With Caption ✅"
                    display_name = first_name or username or str(user_id)
                    text += f"- {display_name} (Status: {status}, Caption: {caption_status})\n"
                    keyboard.append([
                        InlineKeyboardButton(f"View {display_name[:20]} ℹ️", callback_data=f"user_info_{user_id}"),
                        InlineKeyboardButton(f"Toggle Caption {caption_status}", callback_data=f"toggle_user_caption_{user_id}")
                    ])
                    keyboard.append([
                        InlineKeyboardButton("Remove ➖", callback_data=f"remove_user_{user_id}")
                    ])
            else:
                text += "No regular users found 🤷‍♂️\n"

            keyboard.extend([
                [
                    InlineKeyboardButton("Add User (Use /adduser) ➕", callback_data="help_adduser"),
                    InlineKeyboardButton("All Users 📜", callback_data="list_all_users")
                ],
                [
                    InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel"),
                    InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")
                ]
            ])

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"User management error: {e}")
            await query.edit_message_text("Error loading user management 😔")

    async def _show_detailed_stats(self, message):
        """Show detailed bot statistics"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM authorized_users")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(total_files) FROM groups")
            total_files = cursor.fetchone()[0] or 0
            cursor.execute("SELECT SUM(total_size) FROM groups")
            total_size = cursor.fetchone()[0] or 0
            cursor.execute("SELECT COUNT(*) FROM file_links WHERE is_active = 1")
            active_links = cursor.fetchone()[0]
            conn.close()

            text = f"""Bot Statistics 📊

Total Authorized Users: {total_users}
Total Files Stored: {total_files}
Total Storage Used: {format_size(total_size)}
Active Links: {active_links}"""
            await message.reply_text(text)
        except Exception as e:
            logger.error(f"Stats error: {e}")
            await message.reply_text("Error loading stats 😔")

    async def _show_bot_stats_callback(self, query):
        """Show bot stats via callback"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM authorized_users")
            total_users = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(total_files) FROM groups")
            total_files = cursor.fetchone()[0] or 0
            cursor.execute("SELECT SUM(total_size) FROM groups")
            total_size = cursor.fetchone()[0] or 0
            cursor.execute("SELECT COUNT(*) FROM file_links WHERE is_active = 1")
            active_links = cursor.fetchone()[0]
            conn.close()

            text = f"""Bot Statistics 📊

Total Authorized Users: {total_users}
Total Files Stored: {total_files}
Total Storage Used: {format_size(total_size)}
Active Links: {active_links}"""

            keyboard = [
                [InlineKeyboardButton("Full Stats 📈", callback_data="full_stats")],
                [InlineKeyboardButton("Export Stats 📤", callback_data="export_stats")],
                [InlineKeyboardButton("Usage Report 📋", callback_data="usage_report")],
                [InlineKeyboardButton("Refresh 🔄", callback_data="refresh_stats")],
                [InlineKeyboardButton("Admin Panel ⚙️", callback_data="admin_panel")]
            ]

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception as e:
            logger.error(f"Stats callback error: {e}")
            await query.edit_message_text("Error loading stats 😔")

    async def _show_full_stats_callback(self, query):
        """Show full stats"""
        await query.edit_message_text("Full Stats 📈\n\nDetailed stats under development.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")]]))

    async def _export_stats_callback(self, query):
        """Export stats"""
        await query.edit_message_text("Export Stats 📤\n\nExport feature under development.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")]]))

    async def _show_usage_report_callback(self, query):
        """Show usage report"""
        await query.edit_message_text("Usage Report 📋\n\nUsage report under development.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Bot Stats 📊", callback_data="bot_stats")]]))

    async def _show_my_links(self, query, user_id):
        """Show user's links - WORKING VERSION"""
        try:
            conn = psycopg2.connect(SUPABASE_URL)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT fl.link_code, fl.link_type, fl.clicks, fl.created_at,
                       f.file_name, g.name as group_name
                FROM file_links fl
                LEFT JOIN files f ON fl.file_id = f.id
                LEFT JOIN groups g ON fl.group_id = g.id
                WHERE fl.owner_id = %s AND fl.is_active = 1
                ORDER BY fl.created_at DESC
                LIMIT 10
            """, (user_id,))
            links = cursor.fetchall()
            conn.close()

            if not links:
                await query.edit_message_text(
                    "My Links 🔗\n\nNo links found. 🤷‍♂️\nUpload files to generate links. ⬆️",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")]])
                )
                return

            text = "My Links 🔗\n\n"
            keyboard = []
            for link_code, link_type, clicks, created_at, file_name, group_name in links:
                name = file_name if link_type == "file" else group_name
                # Determine the correct callback prefix based on link_type
                callback_prefix = "revoke_file_link_" if link_type == "file" else "revoke_group_link_"
                
                created_at_str = created_at.strftime("%Y-%m-%d") if created_at else "N/A"  # Format datetime to string

                text += f"{link_type.title()}: {name[:20]}{'...' if len(name or '') > 20 else ''}\n"
                text += f"Clicks: {clicks} | Created: {created_at_str}\n"
                text += f"Link: https://t.me/{BOT_USERNAME.replace('@', '')}?start={link_code}\n\n"
                # Add a revoke button for each link in this view with the correct callback_data
                keyboard.append([InlineKeyboardButton(f"Revoke {name[:15]} 🚫", callback_data=f"{callback_prefix}{link_code}")])

            keyboard.append([InlineKeyboardButton("Refresh 🔄", callback_data="cmd_links")])
            keyboard.append([InlineKeyboardButton("Main Menu 🏠", callback_data="main_menu")])

            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

        except Exception as e:
            logger.error(f"Show links error: {e}")
            await query.edit_message_text("Error loading links 😔")


# === Health Check Server Implementation ===
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # A simple GET request handler for health checks
        if self.path == '/healthz': # Define a specific path for the health check
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            # For any other path, return a 404
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

def start_health_check_server():
    """Starts a simple HTTP server for health checks."""
    # Binding to 0.0.0.0 makes it accessible from outside the container
    # Use HEALTH_CHECK_PORT from config, which will get Render's $PORT env var
    with socketserver.TCPServer(("", HEALTH_CHECK_PORT), HealthCheckHandler) as httpd:
        logger.info(f"Health check server serving on port {HEALTH_CHECK_PORT}")
        httpd.serve_forever()

###############################################################################
# 6 — MAIN APPLICATION RUNNER
###############################################################################
def main():
    """Run the bot with all fixes and complete functionality"""
    print("Starting Complete Enhanced FileStore Bot...")

    # Validate configuration
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return
    if not BOT_TOKEN.startswith(("1", "2", "5", "6", "7")):
        logger.error("Invalid BOT_TOKEN format!")
        return

    # Corrected validation for STORAGE_CHANNEL_ID: it must be negative
    if STORAGE_CHANNEL_ID >= 0:
        logger.error("Invalid STORAGE_CHANNEL_ID! Must be negative (e.g., -100xxxxxxxxxx).")
        return

    if not BOT_USERNAME:
        logger.error("BOT_USERNAME environment variable not set!")
        return

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS environment variable not set or is empty. No admins configured!")

    if not ADMIN_CONTACT:
        logger.warning("ADMIN_CONTACT environment variable not set. Admin contact information will be missing.")

    if not SUPABASE_URL:
        logger.error("SUPABASE_URL environment variable not set!")
        return

    logger.info("Configuration validated successfully!")

    try:
        # Manually create JobQueue instance
        job_queue = JobQueue()

        # Create application and pass the job_queue instance directly
        application = ApplicationBuilder().token(BOT_TOKEN).job_queue(job_queue).build()

        # Initialize bot
        bot = FileStoreBot(application)

        # Start health check server in a separate thread
        # This allows the bot to run_polling in the main thread while the HTTP server listens
        health_thread = threading.Thread(target=start_health_check_server, daemon=True)
        health_thread.start()
        logger.info(f"Health check server thread started on port {HEALTH_CHECK_PORT}.")

        # Add all handlers
        application.add_handler(CommandHandler("start", bot.start_handler))
        application.add_handler(CommandHandler("help", bot.help_handler))
        application.add_handler(CommandHandler("clear", bot.clear_handler))
        application.add_handler(CommandHandler("upload", bot.upload_handler))
        application.add_handler(CommandHandler("bulkupload", bot.bulkupload_handler))
        application.add_handler(CommandHandler("groups", bot.groups_handler))
        application.add_handler(CommandHandler("getlink", bot.getlink_handler))
        
        # === REGISTERING NEWLY IMPLEMENTED COMMANDS ===
        application.add_handler(CommandHandler("deletefile", bot.deletefile_handler))
        application.add_handler(CommandHandler("deletegroup", bot.deletegroup_handler))
        application.add_handler(CommandHandler("getgrouplink", bot.getgrouplink_handler))
        application.add_handler(CommandHandler("revokelink", bot.revoke_link_handler)) # NEW COMMAND
        # ===============================================

        # Admin commands
        application.add_handler(CommandHandler("admin", bot.admin_panel_handler))
        application.add_handler(CommandHandler("adduser", bot.add_user_handler))
        application.add_handler(CommandHandler("removeuser", bot.remove_user_handler))
        application.add_handler(CommandHandler("listusers", bot.list_users_handler))
        application.add_handler(CommandHandler("botstats", bot.bot_stats_handler))

        # Message handler for files and for new caption text input
        application.add_handler(MessageHandler(
            filters.Document.ALL | filters.PHOTO | filters.VIDEO |
            filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE | (filters.TEXT & (~filters.COMMAND)),
            bot.file_handler # This handler now also processes text for caption updates
        ))

        # Callback handler
        application.add_handler(CallbackQueryHandler(bot.callback_handler))

        logger.info("Complete Enhanced FileStore Bot started successfully!")
        logger.info(f"Bot Username: {BOT_USERNAME}")
        logger.info(f"Storage Channel: {STORAGE_CHANNEL_ID}")
        logger.info(f"Admin IDs: {', '.join(map(str, ADMIN_IDS))}")
        logger.info(f"Admin Contact: {ADMIN_CONTACT}")
        logger.info(f"File Size Limit: {format_size(MAX_FILE_SIZE)}")

        print("Bot is running with complete functionality! Press Ctrl+C to stop.")

        # Run bot
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"Bot startup error: {e}")
        print(f"Error starting bot: {e}")
    except KeyboardInterrupt:
        clear_console()
        print("Bot stopped by user")
        logger.info("Bot stopped by user")

if __name__ == "__main__":
    main()
