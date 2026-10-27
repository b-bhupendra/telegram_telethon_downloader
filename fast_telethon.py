# fast_telethon.py - High-Performance Parallel MTProto Chunk Downloader
import os
import math
import asyncio
import inspect
import logging
from pathlib import Path
from typing import Optional, List, Union

from telethon import utils, TelegramClient
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from config import PARALLEL_CHUNKS_PER_FILE

log = logging.getLogger("telethon")

class DownloadSender:
    def __init__(self, client: TelegramClient, sender: MTProtoSender, file, offset: int, limit: int,
                 stride: int, count: int) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    async def disconnect(self) -> None:
        try:
            await self.sender.disconnect()
        except Exception:
            pass

class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (None if dc_id and self.client.session.dc_id != dc_id
                         else self.client.session.auth_key)
        self.senders = []

    async def _cleanup(self) -> None:
        if self.senders:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders], return_exceptions=True)
            self.senders = []

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(
            dc.ip_address, dc.port, dc.id,
            loggers=self.client._log,
            proxy=self.client._proxy
        ))
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            await sender.send(InvokeWithLayerRequest(LAYER, self.client._init_request))
            self.auth_key = sender.auth_key
        return sender

    async def download(self, file, file_size: int,
                       connections: int = 4,
                       part_size: int = 512 * 1024):
        part_count = math.ceil(file_size / part_size)
        connections = min(connections, max(1, part_count))
        minimum, remainder = divmod(part_count, connections)

        def get_part_count():
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        first_sender = await self._create_sender()
        self.senders = [
            DownloadSender(self.client, first_sender, file, 0, part_size, connections * part_size, get_part_count())
        ]
        if connections > 1:
            rest_senders = await asyncio.gather(*[self._create_sender() for _ in range(connections - 1)])
            for i, s in enumerate(rest_senders, start=1):
                self.senders.append(
                    DownloadSender(self.client, s, file, i * part_size, part_size, connections * part_size, get_part_count())
                )

        part = 0
        while part < part_count:
            tasks = [asyncio.create_task(s.next()) for s in self.senders]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1

        await self._cleanup()

async def download_file_parallel(
    client: TelegramClient,
    location,
    out_path: Path,
    file_size: int,
    progress_callback=None,
    connections: int = None
) -> bool:
    """High-speed parallel MTProto chunk streaming directly to disk."""
    connections = connections or PARALLEL_CHUNKS_PER_FILE or 4
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    try:
        dc_id, input_location = utils.get_input_location(location)
    except Exception:
        # Fallback to standard client download if location can't be resolved
        try:
            await client.download_media(location, file=str(out_path), progress_callback=progress_callback)
            return True
        except Exception:
            return False

    downloader = ParallelTransferrer(client, dc_id)
    downloaded_bytes = 0

    try:
        with open(temp_path, "wb") as f:
            async for chunk in downloader.download(input_location, file_size, connections=connections, part_size=512*1024):
                f.write(chunk)
                downloaded_bytes += len(chunk)
                if progress_callback:
                    r = progress_callback(downloaded_bytes, file_size)
                    if inspect.isawaitable(r):
                        await r

        if temp_path.exists() and (temp_path.stat().st_size == file_size or downloaded_bytes > 0):
            if out_path.exists():
                out_path.unlink()
            temp_path.replace(out_path)
            return True
        else:
            if temp_path.exists():
                temp_path.unlink()
            return False
    except Exception as e:
        await downloader._cleanup()
        # Fallback to standard download
        try:
            if temp_path.exists():
                temp_path.unlink()
            await client.download_media(location, file=str(out_path), progress_callback=progress_callback)
            return True
        except Exception:
            return False
