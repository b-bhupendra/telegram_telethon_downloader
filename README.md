# Telegram Telethon MTProto Downloader

A high-performance, server-direct MTProto downloader project for Telegram media and files using Telethon and parallel chunk streaming.

---

## Project Structure

```
telegram_telethon_downloader/
├── .venv/               # Virtual environment (Telethon, Rich)
├── config.py            # API_ID, API_HASH, target channel ID, D: drive paths
├── downloader.py        # Core Telethon MTProto download engine with Rich UI
├── fast_telethon.py     # Parallel 1MB byte-range chunk downloader
├── main.py              # CLI entry point (supports --clean flag)
├── README.md            # Usage guide
└── requirements.txt     # Python dependencies (telethon, rich)
```

---

## Features

- **FastTelethon Parallel Chunking**: Streams 1MB chunks concurrently per large file (≥20MB) to saturate full bandwidth.
- **Interactive Channel Selection**: Lists dialogs/channels with a formatted Rich table if `TARGET_CHANNEL_ID` is not preset or dynamic routing is desired.
- **Portable Session Auto-Discovery**: Automatically discovers any `.session` file in the project folder (or `sessions/` or download directory). If you copy-paste any `.session` file into the project folder, it will instantly pick up that account without re-prompting for OTP.
- **Safe Session Preservation**: Session files (`*.session`) are protected from clean-up commands (`main.py --clean`) and ignored by git (`.gitignore`).
- **Drive D: Storage Isolation**: All downloads and logs are isolated on `D:\` drive.

---

## How to Run

1. **Activate the Virtual Environment**:
   ```powershell
   cd D:\Projects\telegram_telethon_downloader
   .\.venv\Scripts\activate
   ```

2. **Run the Downloader**:
   ```powershell
   python main.py
   ```

3. **Fresh Download (Cleans old files while preserving login session)**:
   ```powershell
   python main.py --clean
   ```

4. **First Run Authentication**:
   - Enter your Telegram phone number (e.g. `+91XXXXXXXXXX`).
   - Enter the OTP code received in your Telegram app.
   - The downloader streams files directly into the configured folder on `D:\`!
