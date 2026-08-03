# config.py - Telethon MTProto Downloader Configuration
from pathlib import Path

# Telegram API Credentials
API_ID = 35099334
API_HASH = "ca0ab58622cd30a72ed5d11553eb26af"

# Target Group / Channel Configuration
TARGET_CHANNEL_ID = -1002657449420

# Output Storage Routing on Drive D:
DOWNLOAD_ROOT = Path("D:/Telegram_Downloads/namastesql_hin_eng")
SESSION_FILE = DOWNLOAD_ROOT / "telegram_user.session"
MANIFEST_FILE = DOWNLOAD_ROOT / "download_manifest_namastesql.json"

# Rate Limit & Anti-Ban Speed Settings
MAX_CONCURRENT_DOWNLOADS = 5       # Concurrent files downloading in parallel
PARALLEL_CHUNKS_PER_FILE = 4       # Concurrent chunk streams per large file
CHUNK_SIZE = 1024 * 1024           # 1 MB per chunk (Optimal MTProto size)
LARGE_FILE_THRESHOLD_MB = 20       # Files >= 20 MB use FastTelethon parallel chunking

# Connection & Anti-Ban Bounds
FLOOD_SLEEP_THRESHOLD = 60         # Native auto-sleep on Telegram flood waits < 60s
CONNECTION_RETRIES = 5             # Socket reconnection retry limit
RETRY_ATTEMPTS = 3                 # Max task retries per file

# Ensure output directory exists
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
