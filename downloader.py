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

async def save_manifest_async(progress_context=None):
    def _write():
        try:
            with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
        except Exception as e:
            # Safely print above the progress bar if context exists
            if progress_context:
                progress_context.console.print(f"[red][Error saving manifest]: {e}[/red]")
    await asyncio.to_thread(_write)

async def download_worker(client, message, target_file, clean_filename, current_section, msg_id, progress, overall_task_id):
    """Parallel download worker using Rich progress bars."""
    async with semaphore:
        slot_id = await worker_slots.get()

        # Display: show msg ID + truncated name for unique identification
        short_name = clean_filename.split("_", 1)[-1] if "_" in clean_filename else clean_filename
        disp_tag = f"#{msg_id}"
        max_name_len = 20 - len(disp_tag) - 1  # leave room for ID + space
        if len(short_name) > max_name_len:
            disp_name = f"{disp_tag} {short_name[:max_name_len - 3]}..."
        else:
            disp_name = f"{disp_tag} {short_name:<{max_name_len}}"

        file_size = getattr(message, "file", None).size if getattr(message, "file", None) else 0
        if not file_size and hasattr(message.media, "document"):
            file_size = getattr(message.media.document, "size", 0)

        task_id = progress.add_task("download", filename=disp_name, total=file_size if file_size > 0 else None, custom_stats="")

        def progress_callback(current, total):
            if total > 0:
                progress.update(task_id, completed=current, total=total)
            else:
                progress.update(task_id, completed=current)

        success = False
        large_file_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                if file_size >= large_file_bytes and message.media:
                    success = await download_file_parallel(
                        client, message.media, target_file, file_size, progress_callback=progress_callback
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
                    await save_manifest_async(progress_context=progress)
                    break

            except errors.FloodWaitError as e:
                progress.update(task_id, filename=f"[yellow]{'Paused (RateLimit)':<20}[/yellow]")
                await asyncio.sleep(e.seconds + 2)
                progress.update(task_id, filename=disp_name)
            except Exception as e:
                # SAFE PRINT: Does not corrupt the progress buffer
                progress.console.print(f"[red]Worker {slot_id} Error on {disp_name.strip()}: {e}[/red]")
                progress.update(task_id, filename=f"[red]{f'Retry {attempt}/{RETRY_ATTEMPTS}':<20}[/red]")
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

            # Sanitize then prepend message ID to guarantee unique filenames across the channel
            clean_base = "".join([c if c.isalnum() or c in " ._-" else "_" for c in filename])
            clean_filename = f"{message.id}_{clean_base}"
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

    # \U0001f308 Glitch-Proof Progress Bar Layout
    with Progress(
        TextColumn("[bold #FFD700]{task.fields[filename]}", justify="left"),
        BarColumn(
            bar_width=30,  # Hardcoded width prevents terminal-resizing from breaking layout
            style="color(237)",
            complete_style="#00FF00",
            finished_style="#FF00FF"
        ),
        "[progress.percentage][bold #00FFFF]{task.percentage:>3.1f}%[/]",
        "\u2022",
        TextColumn("{task.fields[custom_stats]}", justify="left"),
        console=console,
        expand=False  # Prevents stretching and wrapping
    ) as progress:

        # Overall task uses custom_stats to format items as Files, not bytes
        overall_task_id = progress.add_task(
            "overall",
            filename="[bold #39FF14]\U0001f680 OVERALL PROGRESS  ",
            total=len(pending_tasks_data),
            custom_stats=f"[#39FF14]0/{len(pending_tasks_data)} Files[/]"
        )

        # Wrapped worker updates overall counter with correct "X/Y Files" text
        async def wrapped_worker(msg, t_file, c_file, sec, m_id):
            res = await download_worker(client, msg, t_file, c_file, sec, m_id, progress, None)
            if res:
                progress.update(overall_task_id, advance=1)
                completed = progress.tasks[overall_task_id].completed
                progress.update(overall_task_id, custom_stats=f"[#39FF14]{int(completed)}/{len(pending_tasks_data)} Files[/]")
            return res

        download_tasks = [
            asyncio.create_task(
                wrapped_worker(message, target_file, clean_filename, sec, msg_id)
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
