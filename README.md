# Telegram Telethon MTProto Downloader

A high-performance, server-direct MTProto downloader project for Telegram media and files using Telethon and parallel chunk streaming.

---

## Project Structure

```
telegram_telethon_downloader/
├── .venv/               # Virtual environment (Telethon, Rich, cryptg)
├── config.py            # API_ID, API_HASH, target channel ID, D: drive paths
├── downloader.py        # Core Telethon MTProto download engine with Rich UI
├── fast_telethon.py     # Multi-connection parallel chunk streamer
├── main.py              # CLI entry point (supports --clean flag)
├── README.md            # Usage guide
└── requirements.txt     # Python dependencies (telethon, rich, cryptg)
```

---

## Features

- **`cryptg` C/Assembly Acceleration**: Uses hardware AES-NI instructions for MTProto encryption/decryption, avoiding the ~200–300 kB/s CPU bottleneck of pure-Python `pyaes` (delivers 10×–50× faster throughput).
- **FastTelethon Multi-Socket Streaming**: Opens parallel TCP connections (`ParallelTransferrer`) directly to Telegram Data Centers using optimal 512 KB native chunks for large files.
- **Forum Supergroup Topic Support**: Automatically identifies Telegram Forum Supergroups, discovers subchannels/topics, and downloads into clean topic subdirectories (`D:\Telegram_Downloads\<Group>\<Topic>\`).
- **Resilient Manifest Tracking**: `download_manifest.json` tracks Telegram IDs, topic IDs, file sizes, and status for instant glitch-proof resumption.
- **Interactive Channel & Topic Selection**: Lists channels with spam filters and allows selecting specific topics or downloading all course topics with one keystroke.
- **Portable Session Auto-Discovery**: Automatically discovers any `.session` file in the project folder (or `sessions/` or download directory). If you copy-paste any `.session` file into the project folder, it will instantly pick up that account without re-prompting for OTP.
- **Safe Session Preservation**: Session files (`*.session`) are protected from clean-up commands (`main.py --clean`) and ignored by git (`.gitignore`).
- **Drive D: Storage Isolation**: All downloads and logs are isolated on `D:\` drive.

---

## How to Run

1. **Activate the Virtual Environment & Install Requirements**:
   ```powershell
   cd D:\Projects\telegram_telethon_downloader
   .\.venv\Scripts\activate
   pip install -r requirements.txt
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
