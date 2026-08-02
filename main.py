# main.py - CLI Entry Point for Telethon MTProto Downloader

import sys
import shutil
import asyncio
from pathlib import Path
from config import DOWNLOAD_ROOT, MANIFEST_FILE, SESSION_FILE
from downloader import run_telethon_downloader

def clean_old_downloads():
    """Safely removes downloaded files and manifest while protecting telegram_user.session."""
    print("==================================================================")
    print("CLEANING OLD DOWNLOADS (PROTECTING USER LOGIN SESSION)...")
    print("==================================================================")
    
    # Remove manifest
    if MANIFEST_FILE.exists():
        try:
            MANIFEST_FILE.unlink()
            print("Deleted download_manifest.json")
        except Exception as e:
            print(f"Warning deleting manifest: {e}")

    # Remove subfolders/files inside DOWNLOAD_ROOT, EXCEPT telegram_user.session
    for item in DOWNLOAD_ROOT.iterdir():
        if item.resolve() == SESSION_FILE.resolve():
            continue  # NEVER delete user login session!
        
        try:
            if item.is_dir():
                shutil.rmtree(item)
                print(f"Removed directory: {item.name}")
            else:
                item.unlink()
                print(f"Removed file: {item.name}")
        except Exception as e:
            print(f"Warning removing {item.name}: {e}")

    print("==================================================================")
    print("Cleanup complete! Ready to fresh-download all channel files.")
    print("==================================================================\n")

def main():
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["--clean", "--clean-all", "--force", "-c"]:
        clean_old_downloads()

    try:
        asyncio.run(run_telethon_downloader())
    except KeyboardInterrupt:
        print("\n[User Cancelled] Telethon session stopped.")
    except Exception as e:
        print(f"\n[Fatal Error]: {e}")

if __name__ == "__main__":
    main()
