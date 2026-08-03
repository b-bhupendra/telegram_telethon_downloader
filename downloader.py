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
    from rich.text import Text
    from rich.table import Table
except ImportError:
    print("[Dependency] Installing rich for advanced UI...")
    os.system("pip install rich")
    from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text
    from rich.table import Table

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
active_manifest_path = MANIFEST_FILE

def load_manifest(path: Path):
    global manifest, active_manifest_path
    active_manifest_path = path
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except Exception:
            manifest = {}
    else:
        manifest = {}

async def save_manifest_async(progress_context=None):
    def _write():
        try:
            with open(active_manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)
        except Exception as e:
            # Safely print above the progress bar if context exists
            if progress_context:
                progress_context.console.print(f"[red][Error saving manifest]: {e}[/red]")
    await asyncio.to_thread(_write)


# --- SMART UI COLUMNS ---
# These columns reveal speed/MBs for file tasks, but hide for the Overall Bar
class SmartDownloadColumn(DownloadColumn):
    def render(self, task):
        if task.fields.get("is_overall"):
            return Text("")
        return Text(" \u2022 ") + super().render(task)

class SmartTransferSpeedColumn(TransferSpeedColumn):
    def render(self, task):
        if task.fields.get("is_overall"):
            return Text("")
        return Text(" \u2022 ") + super().render(task)

class SmartTimeRemainingColumn(TimeRemainingColumn):
    def render(self, task):
        if task.fields.get("is_overall"):
            return Text("")
        return Text(" \u2022 ") + super().render(task)
# ------------------------


async def download_worker(client, message, target_file, clean_filename, current_section, msg_id, progress, overall_task_id):
    """Parallel download worker using Rich progress bars."""
    async with semaphore:
        slot_id = await worker_slots.get()

        # Format display name: 25 chars max, padded to prevent jitter
        if len(clean_filename) > 25:
            disp_name = clean_filename[:22] + "..."
        else:
            disp_name = f"{clean_filename:<25}"

        file_size = getattr(message, "file", None).size if getattr(message, "file", None) else 0
        if not file_size and hasattr(message.media, "document"):
            file_size = getattr(message.media.document, "size", 0)

        # is_overall=False so Smart Columns render speed data for this task
        task_id = progress.add_task("download", filename=disp_name, total=file_size if file_size > 0 else None, custom_stats="", is_overall=False)

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
                progress.update(task_id, filename=f"[yellow]{'Paused (RateLimit)':<25}[/yellow]")
                await asyncio.sleep(e.seconds + 2)
                progress.update(task_id, filename=disp_name)
            except Exception as e:
                # SAFE PRINT: Does not corrupt the progress buffer
                progress.console.print(f"[red]Worker {slot_id} Error on {disp_name.strip()}: {e}[/red]")
                progress.update(task_id, filename=f"[red]{f'Retry {attempt}/{RETRY_ATTEMPTS}':<25}[/red]")
                await asyncio.sleep(2)
                progress.update(task_id, filename=disp_name)

        # Hide the bar when finished to keep the terminal clean
        progress.update(task_id, visible=False)
        worker_slots.put_nowait(slot_id)

        if overall_task_id is not None:
            progress.update(overall_task_id, advance=1)

        return success

async def run_telethon_downloader():
    # Funky Neon Startup Banner
    banner_text = (
        f"[bold #00FFFF]\U0001f4c2 Default Output Directory:[/bold #00FFFF] {DOWNLOAD_ROOT}\n"
        f"[bold #FF00FF]\u26a1 Max Concurrent Files:[/bold #FF00FF]     [#00FF00]{MAX_CONCURRENT_DOWNLOADS}[/]\n"
        f"[bold #FFA500]\U0001f525 Large File Threshold:[/bold #FFA500]     [#00FF00]{LARGE_FILE_THRESHOLD_MB} MB[/] [italic](Parallel Chunking Active)[/]"
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

    # Fetch all dialogs (channels, supergroups, groups)
    console.print("[bold #00FFFF]📡 Fetching your Telegram channels and groups...[/]")
    dialogs = await client.get_dialogs()
    
    # Filter channels & groups primarily
    relevant_dialogs = [d for d in dialogs if d.is_channel or d.is_group]
    if not relevant_dialogs:
        relevant_dialogs = dialogs

    table = Table(title="[bold green]📱 Select a Channel / Group to Download From[/bold green]", border_style="#00FFFF")
    table.add_column("#", justify="right", style="cyan", no_wrap=True)
    table.add_column("Title / Name", style="bold white")
    table.add_column("Type", style="yellow")
    table.add_column("ID", style="dim")
    table.add_column("Status", justify="center")

    default_idx = None
    for idx, d in enumerate(relevant_dialogs, start=1):
        is_target = (d.id == TARGET_CHANNEL_ID) or (str(TARGET_CHANNEL_ID) in str(d.id))
        if is_target:
            default_idx = idx
            status_str = "[bold green]⭐ DEFAULT[/bold green]"
        else:
            status_str = ""

        if d.is_channel and d.is_group:
            dtype = "Supergroup"
        elif d.is_channel:
            dtype = "Channel"
        elif d.is_group:
            dtype = "Group"
        else:
            dtype = "Chat"

        table.add_row(str(idx), d.name or "Untitled", dtype, str(d.id), status_str)

    console.print(table)

    prompt_str = f"\n[bold #00FFFF]👉 Select channel (1-{len(relevant_dialogs)})"
    if default_idx:
        prompt_str += f" [Press Enter for #{default_idx} {relevant_dialogs[default_idx-1].name}]"
    prompt_str += ": [/bold #00FFFF]"
    
    console.print(prompt_str, end="")
    user_choice = (await asyncio.to_thread(input)).strip()

    selected_dialog = None
    if not user_choice:
        if default_idx:
            selected_dialog = relevant_dialogs[default_idx - 1]
        elif relevant_dialogs:
            selected_dialog = relevant_dialogs[0]
    elif user_choice.isdigit():
        choice_num = int(user_choice)
        if 1 <= choice_num <= len(relevant_dialogs):
            selected_dialog = relevant_dialogs[choice_num - 1]
    else:
        # Search by ID or title substring
        for d in relevant_dialogs:
            if str(d.id) == user_choice or user_choice.lower() in (d.name or "").lower():
                selected_dialog = d
                break

    if not selected_dialog:
        console.print("[bold red]⚠️ Selection not found. Falling back to default channel.[/bold red]")
        if default_idx:
            selected_dialog = relevant_dialogs[default_idx - 1]
        elif relevant_dialogs:
            selected_dialog = relevant_dialogs[0]

    entity = selected_dialog.entity
    channel_name = selected_dialog.name or f"Channel_{selected_dialog.id}"
    console.print(f"\n[bold #00FF00]🎯 Selected Channel: [bold #FFD700]{channel_name}[/bold #FFD700] (ID: {selected_dialog.id})[/bold #00FF00]\n")

    # Set channel-specific output path and manifest file
    clean_channel_folder = "".join([c if c.isalnum() or c in " _-" else "_" for c in channel_name]).strip() or f"channel_{selected_dialog.id}"
    target_download_root = Path(f"D:/Telegram_Downloads/{clean_channel_folder}")
    target_download_root.mkdir(parents=True, exist_ok=True)
    target_manifest_file = target_download_root / "download_manifest.json"

    load_manifest(target_manifest_file)

    current_section = "00_General_Files"
    total_found = 0
    total_skipped = 0
    pending_tasks_data = []

    console.print(f"[bold #00FFFF]🌀 Scanning message history for [bold #FFD700]{channel_name}[/bold #FFD700] and cataloging media items...[/]")
    async for message in client.iter_messages(entity, reverse=True):
        text = message.text or ""
        if text and len(text) < 120 and any(kw in text.lower() for kw in ["module", "day", "chapter", "section", "#", "part", "lecture", "assignment"]):
            clean_heading = "".join([c if c.isalnum() or c in " _-" else "_" for c in text.strip()])
            current_section = clean_heading[:60].strip()

        if message.media:
            total_found += 1
            msg_id = str(message.id)
            target_folder = target_download_root / current_section
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

            clean_base = "".join([c if c.isalnum() or c in " ._-" else "_" for c in filename])

            # UNIQUE ID INJECTION: name_{msgid}.ext prevents concurrent .tmp file crashes
            name_part, ext_part = os.path.splitext(clean_base)
            clean_filename = f"{name_part}_{msg_id}{ext_part}"

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
        console.print(f"[bold green]All media files for '{channel_name}' are already downloaded and synchronized on Drive D:![/bold green]")
        return

    # Smart Glitch-Proof UI Layout
    with Progress(
        TextColumn("[bold #FFD700]{task.fields[filename]}", justify="left"),
        BarColumn(
            bar_width=30,
            style="color(237)",
            complete_style="#00FF00",
            finished_style="#FF00FF"
        ),
        "[progress.percentage][bold #00FFFF]{task.percentage:>3.1f}%[/]",
        TextColumn("{task.fields[custom_stats]}", justify="left"),
        SmartDownloadColumn(),       # Only renders for file tasks
        SmartTransferSpeedColumn(),  # Only renders for file tasks
        SmartTimeRemainingColumn(),  # Only renders for file tasks
        console=console,
        expand=False
    ) as progress:

        # is_overall=True hides Download, Speed, Time columns for this bar
        overall_task_id = progress.add_task(
            "overall",
            filename="[bold #39FF14]🚀 OVERALL PROGRESS    ",
            total=len(pending_tasks_data),
            custom_stats=f" • [#39FF14]0/{len(pending_tasks_data)} Files[/]",
            is_overall=True
        )

        async def wrapped_worker(msg, t_file, c_file, sec, m_id):
            res = await download_worker(client, msg, t_file, c_file, sec, m_id, progress, overall_task_id)
            if res:
                completed = progress.tasks[overall_task_id].completed
                progress.update(overall_task_id, custom_stats=f" • [#39FF14]{int(completed)}/{len(pending_tasks_data)} Files[/]")
            return res

        download_tasks = [
            asyncio.create_task(
                wrapped_worker(message, target_file, clean_filename, sec, msg_id)
            )
            for message, target_file, clean_filename, sec, msg_id in pending_tasks_data
        ]

        results = await asyncio.gather(*download_tasks)

    total_downloaded = sum(1 for r in results if r is True)

    # Funky Completion Summary
    summary_text = (
        f"[bold #00FFFF]Total Files Found:[/bold #00FFFF]       {total_found}\n"
        f"[bold #FFA500]Skipped (Already on D:):[/bold #FFA500] {total_skipped}\n"
        f"[bold #00FF00]Successfully Downloaded: {total_downloaded}[/bold #00FF00]\n"
        f"[bold #FF00FF]All Files Saved In:[/bold #FF00FF]      [italic]{target_download_root}[/]"
    )
    console.print("\n")
    console.print(Panel(
        summary_text,
        title="[bold #39FF14]🎉 BATCH DOWNLOAD COMPLETE! 🎉[/]",
        border_style="#00FFFF",
        expand=False
    ))
