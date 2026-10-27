# config.py - Telethon MTProto Downloader Configuration
from pathlib import Path

# Telegram API Credentials
API_ID = 35099334
API_HASH = "ca0ab58622cd30a72ed5d11553eb26af"
PHONE_NUMBER = "+916397123756"

# Target Group / Channel Configuration
TARGET_CHANNEL_ID = -1002657449420

import re

# Output Storage Routing & Portable Session Detection
PROJECT_ROOT = Path(__file__).parent.resolve()
BASE_DOWNLOAD_DIR = Path("D:/Telegram_Downloads")
BASE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_ROOT = BASE_DOWNLOAD_DIR / "namastesql_hin_eng"

# Channel / Group Filtering (skip spam/NSFW from main dialog list)
IGNORED_DIALOG_KEYWORDS = [
    "🔞", "moneymakerr", "cashout", "dark methods", "herbalist"
]

# Noise Topic Filtering for Forum Supergroups (skip casual chatter topics)
IGNORED_TOPIC_KEYWORDS = [
    "baat chit", "chat", "general chat", "discussion", "offtopic", "off-topic"
]

def clean_name(name: str, max_length: int = 60) -> str:
    """Sanitizes names for Windows NTFS paths without ugly repeated underscores."""
    if not name:
        return "Unnamed"
    # Replace invalid Windows chars
    s = re.sub(r'[<>:"/\\|?*]', ' ', name)
    # Collapse whitespace and decorative symbols
    s = re.sub(r'[\s_\*#]+', ' ', s).strip(' ._-')
    return s[:max_length].strip() or "Unnamed"

def resolve_session_file() -> Path:
    """Dynamically locates any portable .session file in the project folder or download root.
    
    If you copy-paste any .session file into this project folder, it will automatically
    be picked up as the active Telegram session.
    """
    # 1. Check project folder for any .session file (prefer telegram_user.session)
    local_sessions = sorted(list(PROJECT_ROOT.glob("*.session")), key=lambda p: (p.stem != "telegram_user", p.name))
    if local_sessions:
        return local_sessions[0]

    # 2. Check local 'sessions' folder if present
    sessions_dir = PROJECT_ROOT / "sessions"
    if sessions_dir.exists():
        sub_sessions = list(sessions_dir.glob("*.session"))
        if sub_sessions:
            return sub_sessions[0]

    # 3. Check DOWNLOAD_ROOT
    if DOWNLOAD_ROOT.exists():
        dl_sessions = sorted(list(DOWNLOAD_ROOT.glob("*.session")), key=lambda p: (p.stem != "telegram_user", p.name))
        if dl_sessions:
            return dl_sessions[0]

    # 4. Fallback to portable project root
    return PROJECT_ROOT / "telegram_user.session"

SESSION_FILE = resolve_session_file()
MANIFEST_FILE = DOWNLOAD_ROOT / "download_manifest_namastesql.json"

# Rate Limit & Anti-Ban Speed Settings
MAX_CONCURRENT_DOWNLOADS = 2       # Concurrent files downloading in parallel
PARALLEL_CHUNKS_PER_FILE = 4       # Concurrent chunk streams per large file
CHUNK_SIZE = 1024 * 1024           # 1 MB per chunk (Optimal MTProto size)
LARGE_FILE_THRESHOLD_MB = 20       # Files >= 20 MB use FastTelethon parallel chunking

# Connection & Anti-Ban Bounds
FLOOD_SLEEP_THRESHOLD = 60         # Native auto-sleep on Telegram flood waits < 60s
CONNECTION_RETRIES = 5             # Socket reconnection retry limit
RETRY_ATTEMPTS = 3                 # Max task retries per file

# Ensure output directory exists
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
