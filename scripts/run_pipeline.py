"""
run_pipeline.py
Full automation flow for SAR Tren Maya project.
This script orchestrates:
  1) Earth Engine exports (01_export_s1_gee.py)
  2) Waits until Drive exports are ready
  3) Downloads them locally
  4) Runs processing (02_process_s1_local.py)
  5) Saves all outputs in organized folders
  6) Prepares summary for teammates

Call this script from Power Automate or Task Scheduler.
"""

import os
import time
import subprocess
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# ---------------- CONFIG ----------------
EXPORT_SCRIPT = "scripts/01_export_s1_gee.py"
PROCESS_SCRIPT = "scripts/02_process_s1_local.py"
EXPORT_FOLDER = "SpaceApps_SAR"   # Google Drive folder where GEE sends files
LOCAL_EXPORTS = "outputs/gee/"
LOCAL_PROCESSED = "outputs/processed/"
WAIT_TIME = 300   # seconds between checks for new exports
MAX_WAIT = 7200   # max wait (2h) for Drive exports

# ---------------- STEP 1. Run GEE export script ----------------
print("[INFO] Starting Earth Engine export script...")
subprocess.run(["python", EXPORT_SCRIPT], check=True)
print("[INFO] Export tasks submitted to GEE.")

# ---------------- STEP 2. Wait for exports to appear in Drive ----------------
print("[INFO] Waiting for Google Drive exports...")

# Authenticate Google Drive
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

def list_drive_files(folder_name):
    folder_list = drive.ListFile(
        {'q': f"title = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed=false"}
    ).GetList()
    if not folder_list:
        raise Exception(f"Drive folder {folder_name} not found")
    folder_id = folder_list[0]['id']
    return drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

start_time = time.time()
files_ready = False

while time.time() - start_time < MAX_WAIT:
    files = list_drive_files(EXPORT_FOLDER)
    if files:
        print(f"[INFO] Found {len(files)} files in {EXPORT_FOLDER}.")
        files_ready = True
        break
    print("[INFO] No files yet. Sleeping...")
    time.sleep(WAIT_TIME)

if not files_ready:
    raise TimeoutError("Drive exports not ready in time.")

# ---------------- STEP 3. Download files ----------------
os.makedirs(LOCAL_EXPORTS, exist_ok=True)
for f in files:
    file_name = f['title']
    local_path = os.path.join(LOCAL_EXPORTS, file_name)
    if not os.path.exists(local_path):
        print(f"[INFO] Downloading {file_name}...")
        f.GetContentFile(local_path)
print("[INFO] All files downloaded.")

# ---------------- STEP 4. Run processing script ----------------
print("[INFO] Running local processing...")
subprocess.run(["python", PROCESS_SCRIPT], check=True)
print("[INFO] Local processing completed.")

# ---------------- STEP 5. Organize outputs ----------------
os.makedirs(LOCAL_PROCESSED, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
final_dir = os.path.join(LOCAL_PROCESSED, f"run_{timestamp}")
os.makedirs(final_dir, exist_ok=True)

# Move files
for folder in [LOCAL_EXPORTS, "outputs/plots/"]:
    if os.path.exists(folder):
        for f in os.listdir(folder):
            src = os.path.join(folder, f)
            dst = os.path.join(final_dir, f)
            os.rename(src, dst)
print(f"[INFO] Outputs stored in {final_dir}")

# ---------------- STEP 6. Generate summary log ----------------
summary_file = os.path.join(final_dir, "SUMMARY.txt")
with open(summary_file, "w") as f:
    f.write("SAR Tren Maya Analysis - Pipeline Run\n")
    f.write(f"Timestamp: {timestamp}\n")
    f.write(f"Files processed: {len(files)}\n")
    f.write("Steps:\n")
    f.write(" 1) Exported from GEE\n")
    f.write(" 2) Downloaded from Drive\n")
    f.write(" 3) Processed locally (filters, time-series, change maps)\n")
    f.write(" 4) Stored in outputs folder\n")
print(f"[INFO] Summary written to {summary_file}")

print("[SUCCESS] Full pipeline complete. Share results with team.")
