# FileStore Bot Deployment (Render with Docker, Fixed)

This is a Telegram FileStore Bot ready to deploy on Render with Docker.

## Fixes
- Replaced deprecated `FloodWait` with `RetryAfter` for PTB v20 compatibility.
- Ensures Python 3.11 via Dockerfile so asyncpg builds correctly.

## Steps to Deploy
1. Create a **Worker Service** on Render.
2. Upload this repo or connect GitHub.
3. Render builds using Dockerfile (Python 3.11-slim).
4. Add env vars in Render Dashboard:
   - BOT_TOKEN
   - BOT_USERNAME
   - DATABASE_URL
   - storage_id
   - ADMIN_IDS
   - ADMIN_CONTACT
   - CUSTOM_CAPTION (optional)
5. Deploy 🚀
