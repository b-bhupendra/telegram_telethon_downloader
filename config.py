# config.py - Telethon MTProto Downloader Configuration

from pathlib import Path

# Telegram API Credentials (Extracted directly via my.telegram.org)
API_ID = 35099334
API_HASH = "ca0ab58622cd30a72ed5d11553eb26af"

# Target Group / Channel Configuration
TARGET_CHANNEL_ID = -1002600547010

# Output Storage Routing on Drive D: (0 MB used on C:)
DOWNLOAD_ROOT = Path("D:/Telegram_Downloads/Codebasic_Bootcamp")
SESSION_FILE = DOWNLOAD_ROOT / "telegram_user.session"
MANIFEST_FILE = DOWNLOAD_ROOT / "download_manifest.json"

# Ensure output directory exists
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
