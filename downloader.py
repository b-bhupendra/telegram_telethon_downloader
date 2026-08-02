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

try:
    from tqdm import tqdm
except ImportError:
    print("[Dependency] Installing tqdm...")
    os.system("pip install tqdm")
    from tqdm import tqdm

from config import (
    API_ID, API_HASH, TARGET_CHANNEL_ID,
    DOWNLOAD_ROOT, SESSION_FILE, MANIFEST_FILE,
    MAX_CONCURRENT_DOWNLOADS, LARGE_FILE_THRESHOLD_MB,
    FLOOD_SLEEP_THRESHOLD, CONNECTION_RETRIES, RETRY_ATTEMPTS
)
from fast_telethon import download_file_parallel

semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
worker_slots = asyncio.Queue()
for i in range(1, MAX_CONCURRENT_DOWNLOADS + 1):
    worker_slots.put_nowait(i)

manifest = {}
if MANIFEST_FILE.exists():
    try:
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception:
        manifest = {}

async def save_manifest_async():
    def _write():
        try:
            with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
        except Exception as e:
            print(f"[Error saving manifest]: {e}")
    await asyncio.to_thread(_write)

async def download_worker(client, message, target_file, clean_filename, current_section, msg_id, overall_pbar):
    """Parallel download worker routing tasks dynamically between standard & chunk engines."""
    async with semaphore:
        slot_id = await worker_slots.get()
        disp_name = clean_filename if len(clean_filename) <= 32 else (clean_filename[:29] + "...")
        
        file_size = getattr(message, "file", None).size if getattr(message, "file", None) else 0
        if not file_size and hasattr(message.media, "document"):
            file_size = getattr(message.media.document, "size", 0)

        pbar = tqdm(
            total=file_size if file_size > 0 else 0,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Worker {slot_id} | {disp_name}",
            position=slot_id,
            leave=False
        )

        def progress_callback(current, total):
            if total > 0 and pbar.total != total:
                pbar.reset(total=total)
            pbar.update(current - pbar.n)

        success = False
        large_file_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                # Fast parallel chunk engine for large files; single stream for small files
                if file_size >= large_file_bytes and message.media:
                    success = await download_file_parallel(
                        client, 
                        message.media, 
                        target_file, 
                        file_size, 
                        progress_callback=progress_callback
                    )
                else:
                    await client.download_media(message, file=str(target_file), progress_callback=progress_callback)
                    success = True

                if success and target_file.exists():
                    manifest[msg_id] = {
                        "status": "complete",
                        "path": str(target_file),
                        "filename": clean_filename,
                        "section": current_section,
                        "size_bytes": target_file.stat().st_size
                    }
                    await save_manifest_async()
                    break
            except errors.FloodWaitError as e:
                pbar.set_description(f"Worker {slot_id} | Paused {e.seconds}s (RateLimit)")
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                pbar.set_description(f"Worker {slot_id} | Retry {attempt}/{RETRY_ATTEMPTS}")
                await asyncio.sleep(2)

        pbar.close()
        worker_slots.put_nowait(slot_id)
        
        if overall_pbar:
            overall_pbar.update(1)
            
        return success

async def run_telethon_downloader():
    print("==================================================================")
    print("TELEGRAM OPTIMIZED MTPROTO PARALLEL DOWNLOADER (ANTI-BAN SAFE)")
    print("==================================================================")
    print(f"Output Directory:     {DOWNLOAD_ROOT}")
    print(f"Max Concurrent Files: {MAX_CONCURRENT_DOWNLOADS}")
    print(f"Large File Threshold: {LARGE_FILE_THRESHOLD_MB} MB (Parallel Chunking Active)")
    print("==================================================================\n")

    client = TelegramClient(
        str(SESSION_FILE),
        API_ID,
        API_HASH,
        device_model="Desktop Parallel Node",
        system_version="Windows 11",
        app_version="4.16.30",
        connection_retries=CONNECTION_RETRIES,
        retry_delay=2,
        flood_sleep_threshold=FLOOD_SLEEP_THRESHOLD
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
    pending_tasks_data = []

    print("\nScanning message history and cataloging media items...\n")
    async for message in client.iter_messages(entity, reverse=True):
        text = message.text or ""
        if text and len(text) < 120 and any(kw in text.lower() for kw in ["module", "day", "chapter", "section", "#", "part", "lecture", "assignment"]):
            clean_heading = "".join([c if c.isalnum() or c in " _-" else "_" for c in text.strip()])
            current_section = clean_heading[:60].strip()

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

            # Strict Resumability Verification
            expected_size = getattr(message, "file", None).size if getattr(message, "file", None) else 0
            if target_file.exists() and target_file.stat().st_size > 0:
                if msg_id in manifest and manifest[msg_id].get("status") == "complete":
                    total_skipped += 1
                    continue
                elif expected_size > 0 and target_file.stat().st_size == expected_size:
                    manifest[msg_id] = {
                        "status": "complete",
                        "path": str(target_file),
                        "filename": clean_filename,
                        "section": current_section,
                        "size_bytes": target_file.stat().st_size
                    }
                    total_skipped += 1
                    continue

            pending_tasks_data.append((message, target_file, clean_filename, current_section, msg_id))

    await save_manifest_async()

    print(f"\nCatalog Summary:")
    print(f"   Total Media Found:   {total_found}")
    print(f"   Already Saved (D:):  {total_skipped}")
    print(f"   Pending Downloads:   {len(pending_tasks_data)}\n")

    if not pending_tasks_data:
        print("  All channel media files are already downloaded and synchronized on Drive D:!")
        return

    overall_pbar = tqdm(
        total=len(pending_tasks_data),
        desc="Overall Channel Progress",
        unit="file",
        position=0,
        leave=True
    )

    # Instant non-blocking dispatch governed strictly by Semaphore limits
    download_tasks = [
        asyncio.create_task(
            download_worker(client, message, target_file, clean_filename, sec, msg_id, overall_pbar)
        )
        for message, target_file, clean_filename, sec, msg_id in pending_tasks_data
    ]

    results = await asyncio.gather(*download_tasks)
    overall_pbar.close()

    total_downloaded = sum(1 for r in results if r is True)
    print("\n==================================================================")
    print("ALL FILES BATCH DOWNLOAD COMPLETE!")
    print(f"Total Files Found:       {total_found}")
    print(f"Skipped (Already on D:): {total_skipped}")
    print(f"Successfully Downloaded: {total_downloaded}")
    print(f"All Files Saved In:      {DOWNLOAD_ROOT}")
    print("==================================================================")
