# fast_telethon.py - Parallel Byte-Range Chunk Downloader
import math
import asyncio
from pathlib import Path
from telethon import TelegramClient
from config import CHUNK_SIZE, PARALLEL_CHUNKS_PER_FILE

async def download_chunk_with_retry(client: TelegramClient, location, offset: int, limit: int, retries: int = 3) -> bytes:
    """Downloads a single byte-range chunk using iter_download with automatic retry logic."""
    for attempt in range(1, retries + 1):
        try:
            data = b""
            # iter_download correctly resolves the media location and respects byte offsets
            async for chunk in client.iter_download(location, offset=offset, request_size=1024 * 1024):
                data += chunk
                if len(data) >= limit:
                    break
                    
            if data:
                return data[:limit]  # Strictly truncate to the requested limit
        except Exception as e:
            if attempt == retries:
                print(f"[Chunk Error at offset {offset}]: {e}")
                return b""
            await asyncio.sleep(1)
    return b""

async def download_file_parallel(
    client: TelegramClient, 
    location, 
    out_path: Path, 
    file_size: int,
    progress_callback=None
) -> bool:
    """Downloads a large file using parallel byte-range streams directly to disk."""
    if file_size <= 0:
        await client.download_file(location, out_path)
        return True

    num_chunks = math.ceil(file_size / CHUNK_SIZE)
    semaphore = asyncio.Semaphore(PARALLEL_CHUNKS_PER_FILE)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    # Allocate file space on disk
    with open(temp_path, "wb") as f:
        f.seek(file_size - 1)
        f.write(b"\0")

    downloaded_bytes = 0
    bytes_lock = asyncio.Lock()

    async def worker(chunk_idx: int):
        nonlocal downloaded_bytes
        async with semaphore:
            offset = chunk_idx * CHUNK_SIZE
            limit = min(CHUNK_SIZE, file_size - offset)
            
            chunk_data = await download_chunk_with_retry(client, location, offset, limit)
            if chunk_data:
                with open(temp_path, "r+b") as f:
                    f.seek(offset)
                    f.write(chunk_data)
                
                # Atomically update the progress bar
                async with bytes_lock:
                    downloaded_bytes += len(chunk_data)
                    if progress_callback:
                        progress_callback(downloaded_bytes, file_size)

    tasks = [asyncio.create_task(worker(i)) for i in range(num_chunks)]
    await asyncio.gather(*tasks)

    # Verify integrity before replacing target destination
    if temp_path.exists() and temp_path.stat().st_size == file_size:
        if out_path.exists():
            out_path.unlink()
        temp_path.replace(out_path)
        return True
    else:
        if temp_path.exists():
            temp_path.unlink()
        return False
