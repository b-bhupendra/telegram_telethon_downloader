# main.py - CLI Entry Point for Telethon MTProto Downloader

import sys
import asyncio
from downloader import run_telethon_downloader

def main():
    try:
        asyncio.run(run_telethon_downloader())
    except KeyboardInterrupt:
        print("\n[User Cancelled] Telethon session stopped.")
    except Exception as e:
        print(f"\n[Fatal Error]: {e}")

if __name__ == "__main__":
    main()
