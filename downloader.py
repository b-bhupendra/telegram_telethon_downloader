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
    from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
    from rich.console import Console
    from rich.panel import Panel
except ImportError:
    print("[Dependency] Installing rich for advanced UI...")
    os.system("pip install rich")
    from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
    from rich.console import Console
    from rich.panel import Panel

from config import (
    API_ID, API_HASH, TARGET_CHANNEL_ID,
    DOWNLOAD_ROOT, SESSION_FILE, MANIFEST_FILE,
    MAX_CONCURRENT_DOWNLOADS, LARGE_FILE_THRESHOLD_MB,
    FLOOD_SLEEP_THRESHOLD, CONNECTION_RETRIES, RETRY_ATTEMPTS
)
from fast_telethon import download_file_parallel

console = Console()
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
            console.print(f"[red][Error saving manifest]: {e}[/red]")
    await asyncio.to_thread(_write)

async def download_worker(client, message, target_file, clean_filename, current_section, msg_id, progress, overall_task_id):
    """Parallel download worker using Rich progress bars."""
    async with semaphore:
        slot_id = await worker_slots.get()
        disp_name = clean_filename if len(clean_filename) <= 30 else (clean_filename[:27] + "...")
        
        file_size = getattr(message, "file", None).size if getattr(message, "file", None) else 0
        if not file_size and hasattr(message.media, "document"):
            file_size = getattr(message.media.document, "size", 0)

        # Add a new progress bar task for this specific file
        task_id = progress.add_task("download", filename=disp_name, total=file_size if file_size > 0 else None)

        def progress_callback(current, total):
            if total > 0 and progress.tasks[task_id].total != total:
                progress.update(task_id, total=total)
            progress.update(task_id, completed=current)

        success = False
        large_file_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
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
                progress.update(task_id, filename=f"[yellow]Paused {e.seconds}s (RateLimit)[/yellow]")
                await asyncio.sleep(e.seconds + 2)
                progress.update(task_id, filename=disp_name) # Restore name
            except Exception as e:
                progress.update(task_id, filename=f"[red]Retry {attempt}/{RETRY_ATTEMPTS}[/red]")
                await asyncio.sleep(2)
                progress.update(task_id, filename=disp_name)

        # Hide the bar when finished to keep the terminal clean
        progress.update(task_id, visible=False)
        worker_slots.put_nowait(slot_id)
        
        if overall_task_id is not None:
            progress.update(overall_task_id, advance=1)
            
        return success

async def run_telethon_downloader():
    # \U0001f680 Funky Neon Startup Banner
    banner_text = (
        f"[bold #00FFFF]\U0001f4c2 Output Directory:[/bold #00FFFF]     [italic #FFFFFF]{DOWNLOAD_ROOT}[/]\n"
        f"[bold #FF00FF]\u26a1 Max Concurrent Files:[/bold #FF00FF] [#00FF00]{MAX_CONCURRENT_DOWNLOADS}[/]\n"
        f"[bold #FFA500]\U0001f525 Large File Threshold:[/bold #FFA500] [#00FF00]{LARGE_FILE_THRESHOLD_MB} MB[/] [italic](Parallel Chunking Active)[/]"
    )
    console.print(Panel(
        banner_text, 
        title="[bold blink #39FF14]\U0001f47e TELEGRAM MTPROTO DOWNLOADER \U0001f47e[/]", 
        border_style="#FF00FF",
        expand=False
    ))

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
    console.print("[bold #00FF00]\U0001f513 Authenticated with Telegram MTProto API successfully![/]\n")

    entity = None
    try:
        entity = await client.get_entity(TARGET_CHANNEL_ID)
        console.print(f"\U0001f4e1 Connected to Channel: [bold #FFD700]{getattr(entity, 'title', TARGET_CHANNEL_ID)}[/]")
    except Exception:
        console.print("\U0001f50d Searching channel in user dialogs list...")
        async for dialog in client.iter_dialogs():
            if dialog.id == TARGET_CHANNEL_ID or str(TARGET_CHANNEL_ID) in str(dialog.id):
                entity = dialog.entity
                console.print(f"\U0001f50d Found Channel: [bold #FFD700]{dialog.name}[/]")
                break

    if not entity:
        console.print("[bold red]Could not resolve channel entity. Ensure your account is a member of the group.[/bold red]")
        return

    current_section = "00_General_Files"
    total_found = 0
    total_skipped = 0
    pending_tasks_data = []

    console.print("[bold #00FFFF]\U0001f300 Scanning message history and cataloging media items...[/]")
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

    console.print(f"\n[bold]Catalog Summary:[/bold]")
    console.print(f"   Total Media Found:   {total_found}")
    console.print(f"   Already Saved (D:):  {total_skipped}")
    console.print(f"   Pending Downloads:   {len(pending_tasks_data)}\n")

    if not pending_tasks_data:
        console.print("[bold green]All channel media files are already downloaded and synchronized on Drive D:![/bold green]")
        return

    # \U0001f308 Funky Custom Progress Bar Settings
    with Progress(
        TextColumn("[bold #FFD700]{task.fields[filename]}", justify="right"),
        BarColumn(
            bar_width=None, 
            style="color(237)",
            complete_style="#00FF00",
            finished_style="#FF00FF"
        ),
        "[progress.percentage][bold #00FFFF]{task.percentage:>3.1f}%[/]",
        "\u2022",
        DownloadColumn(),
        "\u2022",
        TransferSpeedColumn(),
        "\u2022",
        TimeRemainingColumn(),
        console=console
    ) as progress:
        
        overall_task_id = progress.add_task("overall", filename="[bold #39FF14]\U0001f680 OVERALL PROGRESS", total=len(pending_tasks_data))

        download_tasks = [
            asyncio.create_task(
                download_worker(client, message, target_file, clean_filename, sec, msg_id, progress, overall_task_id)
            )
            for message, target_file, clean_filename, sec, msg_id in pending_tasks_data
        ]

        results = await asyncio.gather(*download_tasks)

    total_downloaded = sum(1 for r in results if r is True)
    
    # \U0001f389 Funky Completion Summary
    summary_text = (
        f"[bold #00FFFF]Total Files Found:[/bold #00FFFF]       {total_found}\n"
        f"[bold #FFA500]Skipped (Already on D:):[/bold #FFA500] {total_skipped}\n"
        f"[bold #00FF00]Successfully Downloaded: {total_downloaded}[/bold #00FF00]\n"
        f"[bold #FF00FF]All Files Saved In:[/bold #FF00FF]      [italic]{DOWNLOAD_ROOT}[/]"
    )
    console.print("\n")
    console.print(Panel(
        summary_text, 
        title="[bold #39FF14]\U0001f389 BATCH DOWNLOAD COMPLETE! \U0001f389[/]", 
        border_style="#00FFFF",
        expand=False
    ))
