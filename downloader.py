# downloader.py - High-Performance Telethon MTProto Channel Downloader

import os
import sys
import json
import asyncio
from pathlib import Path

try:
    from telethon import TelegramClient
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, DocumentAttributeFilename
except ImportError:
    print("[Dependency] Installing telethon...")
    os.system("pip install telethon")
    from telethon import TelegramClient
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, DocumentAttributeFilename

from config import (
    API_ID, API_HASH, TARGET_CHANNEL_ID,
    DOWNLOAD_ROOT, SESSION_FILE, MANIFEST_FILE
)

manifest = {}
if MANIFEST_FILE.exists():
    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception:
        manifest = {}

def save_manifest():
    try:
        with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        print(f"[Error saving manifest]: {e}")

def progress_callback(current, total):
    pct = round((current / total) * 100) if total > 0 else 0
    curr_mb = round(current / (1024 * 1024), 2)
    tot_mb = round(total / (1024 * 1024), 2)
    sys.stdout.write(f"\r   Downloading: [{pct:3d}%] {curr_mb} MB / {tot_mb} MB")
    sys.stdout.flush()

async def run_telethon_downloader():
    print("==================================================================")
    print("TELEGRAM NATIVE MTPROTO ALL-FILES DOWNLOADER (DRIVE D: ISOLATED)")
    print("==================================================================")
    print(f"Output Folder:    {DOWNLOAD_ROOT}")
    print(f"Target Channel:   {TARGET_CHANNEL_ID}")
    print(f"API ID:           {API_ID}")
    print("==================================================================\n")

    client = TelegramClient(
        str(SESSION_FILE),
        API_ID,
        API_HASH,
        device_model="Desktop",
        system_version="Windows 11",
        app_version="4.16.30"
    )
    
    await client.start()

    print("\nAuthenticated with Telegram MTProto API successfully!")
    print("Resolving channel metadata...")

    entity = None
    try:
        entity = await client.get_entity(TARGET_CHANNEL_ID)
        print(f"Connected to Channel: {getattr(entity, 'title', TARGET_CHANNEL_ID)}")
    except Exception:
        print("Searching channel in user dialogs list...")
        async for dialog in client.iter_dialogs():
            if dialog.id == TARGET_CHANNEL_ID or str(TARGET_CHANNEL_ID) in str(dialog.id):
                entity = dialog.entity
                print(f"Found Channel: {dialog.name}")
                break

    if not entity:
        print("Could not resolve channel entity. Ensure your account is a member of the group.")
        return

    current_section = "00_General_Files"
    total_found = 0
    total_downloaded = 0
    total_skipped = 0

    print("\nScanning message history from oldest to newest (ALL files & media)...\n")

    async for message in client.iter_messages(entity, reverse=True):
        text = message.text or ""

        # Section Heading Detection
        if text and len(text) < 120 and any(kw in text.lower() for kw in ["module", "day", "chapter", "section", "#", "part", "lecture", "assignment"]):
            clean_heading = "".join([c if c.isalnum() or c in " _-" else "_" for c in text.strip()])
            current_section = clean_heading[:60].strip()
            print(f"\nSECTION: {current_section}")

        # Process ALL media items (Videos, Documents, Zip, PDF, Code, Photos, Audio)
        if message.media:
            total_found += 1
            msg_id = str(message.id)

            target_folder = DOWNLOAD_ROOT / current_section
            target_folder.mkdir(parents=True, exist_ok=True)

            filename = f"file_msg_{message.id}"
            doc = getattr(message.media, "document", None)

            if doc:
                filename_attr = next((a.file_name for a in doc.attributes if isinstance(a, DocumentAttributeFilename) and a.file_name), None)
                if filename_attr:
                    filename = filename_attr
                else:
                    mime = getattr(doc, "mime_type", "")
                    ext = ".mp4" if "video" in mime else (".pdf" if "pdf" in mime else (".zip" if "zip" in mime else ".bin"))
                    filename = f"file_msg_{message.id}{ext}"
            elif isinstance(message.media, MessageMediaPhoto):
                filename = f"photo_{message.id}.jpg"

            clean_filename = "".join([c if c.isalnum() or c in " ._-" else "_" for c in filename])
            target_file = target_folder / clean_filename

            # Resumability check
            if target_file.exists():
                file_size = target_file.stat().st_size
                if msg_id in manifest and manifest[msg_id].get("status") == "complete" and file_size > 0:
                    print(f"[Skip] Msg {message.id} ({clean_filename}) - Already on D:")
                    total_skipped += 1
                    continue

            print(f"\n[Download] Msg {message.id}: {clean_filename}")
            try:
                await client.download_media(message, file=str(target_file), progress_callback=progress_callback)
                print("") # newline after progress bar

                manifest[msg_id] = {
                    "status": "complete",
                    "path": str(target_file),
                    "filename": clean_filename,
                    "section": current_section,
                    "size_bytes": target_file.stat().st_size if target_file.exists() else 0
                }
                save_manifest()
                total_downloaded += 1

            except Exception as e:
                print(f"\nError downloading Msg {message.id}: {e}")

    print("\n==================================================================")
    print("ALL FILES BATCH DOWNLOAD COMPLETE!")
    print(f"Total Files/Media Found: {total_found}")
    print(f"Successfully Downloaded: {total_downloaded}")
    print(f"Skipped (Already on D:): {total_skipped}")
    print(f"All Files saved in:       {DOWNLOAD_ROOT}")
    print("==================================================================")
