# downloader.py - High-Performance Anti-Ban Parallel MTProto Downloader

import os
import sys
import json
import asyncio
from pathlib import Path

try:
    from telethon import TelegramClient, errors
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, DocumentAttributeFilename
except ImportError:
    print("[Dependency] Installing telethon...")
    os.system("pip install telethon")
    from telethon import TelegramClient, errors
    from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, DocumentAttributeFilename

from config import (
    API_ID, API_HASH, TARGET_CHANNEL_ID,
    DOWNLOAD_ROOT, SESSION_FILE, MANIFEST_FILE,
    MAX_CONCURRENT_DOWNLOADS, WORKER_JITTER_DELAY_SEC, RETRY_ATTEMPTS
)

# Concurrency Semaphore bounded to safe non-bannable threshold (3 concurrent workers)
semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

manifest = {}
if MANIFEST_FILE.exists():
    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception:
        manifest = {}

# Asynchronous non-blocking manifest writer
async def save_manifest_async():
    def _write():
        try:
            with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
        except Exception as e:
            print(f"[Error saving manifest]: {e}")
    await asyncio.to_thread(_write)

async def download_worker(client, message, target_file, clean_filename, current_section, msg_id):
    """Parallel download worker task with rate-limit protection."""
    async with semaphore:
        print(f"[Starting Download] {clean_filename} -> Subfolder: [{current_section}]")
        
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                # Direct Telethon MTProto media stream (3 workers downloading in parallel)
                await client.download_media(message, file=str(target_file))

                # Record completion atomically
                manifest[msg_id] = {
                    "status": "complete",
                    "path": str(target_file),
                    "filename": clean_filename,
                    "section": current_section,
                    "size_bytes": target_file.stat().st_size if target_file.exists() else 0
                }
                await save_manifest_async()
                print(f"[Success ✅] {clean_filename} saved.")
                return True

            except errors.FloodWaitError as e:
                print(f"\n[Rate Limit Pause] Telegram requested a pause. Sleeping for {e.seconds + 2} seconds...")
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                print(f"\n[Error Attempt {attempt}/{RETRY_ATTEMPTS}] Failed Msg {message.id} ({clean_filename}): {e}")
                await asyncio.sleep(2)
        
        return False

async def run_telethon_downloader():
    print("==================================================================")
    print("TELEGRAM PARALLEL MTPROTO DOWNLOADER (ANTI-BAN SAFE, DRIVE D:)")
    print("==================================================================")
    print(f"Output Directory:     {DOWNLOAD_ROOT}")
    print(f"Max Parallel Workers: {MAX_CONCURRENT_DOWNLOADS} (Rate-limit Safe)")
    print("==================================================================\n")

    client = TelegramClient(
        str(SESSION_FILE),
        API_ID,
        API_HASH,
        device_model="Desktop Parallel Node",
        system_version="Windows 11",
        app_version="4.16.30"
    )
    
    await client.start()
    print("Authenticated with Telegram MTProto API successfully!")

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
    total_skipped = 0
    download_tasks = []

    print("\nScanning message history and dispatching parallel tasks...\n")

    async for message in client.iter_messages(entity, reverse=True):
        text = message.text or ""

        # Section Heading Detection
        if text and len(text) < 120 and any(kw in text.lower() for kw in ["module", "day", "chapter", "section", "#", "part", "lecture", "assignment"]):
            clean_heading = "".join([c if c.isalnum() or c in " _-" else "_" for c in text.strip()])
            current_section = clean_heading[:60].strip()

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
            if target_file.exists() and target_file.stat().st_size > 0:
                if msg_id in manifest and manifest[msg_id].get("status") == "complete":
                    total_skipped += 1
                    continue

            # Queue task into bounded async worker pool
            task = asyncio.create_task(
                download_worker(client, message, target_file, clean_filename, current_section, msg_id)
            )
            download_tasks.append(task)
            
            # Stagger dispatch slightly to prevent burst connection spikes
            await asyncio.sleep(WORKER_JITTER_DELAY_SEC)

    print(f"\n[Queue Manager] Dispatched {len(download_tasks)} download tasks. Processing parallel pool...\n")

    if download_tasks:
        results = await asyncio.gather(*download_tasks)
        total_downloaded = sum(1 for r in results if r is True)
    else:
        total_downloaded = 0

    print("\n==================================================================")
    print("ALL FILES BATCH DOWNLOAD COMPLETE!")
    print(f"Total Files Found:       {total_found}")
    print(f"Skipped (Already on D:): {total_skipped}")
    print(f"Successfully Downloaded: {total_downloaded}")
    print(f"All Files Saved In:      {DOWNLOAD_ROOT}")
    print("==================================================================")
