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

# Rate Limit & Anti-Ban Bounds
MAX_CONCURRENT_DOWNLOADS = 5       # 5 parallel workers (Maximum safe speed limit)
LARGE_FILE_THRESHOLD_MB = 20        # Files > 20MB use FastTelethon parallel chunk streaming
WORKER_JITTER_DELAY_SEC = 0.3      # Stagger delay between worker dispatches
RETRY_ATTEMPTS = 3

# Ensure output directory exists
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
