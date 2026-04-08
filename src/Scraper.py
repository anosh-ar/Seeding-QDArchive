import os
import time
import json
import csv
import re
import requests

BASE_URL = "https://dataverse.harvard.edu"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"
DATASET_ENDPOINT = f"{BASE_URL}/api/datasets/:persistentId"

# Optional: Dataverse API token (set in your environment if you have one)
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

# Put downloads in Seeding-QDArchive/files
FILES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "files"))


def process_item(session, item, allowed_exts):
    """Handle a single search result; return True if a file was saved."""
    name = item.get("name")
    dataset_name = item.get("dataset_name")
    dataset_persistent_id = item.get("dataset_persistent_id")
    file_id = item.get("file_id")
    download_url = item.get("url")
    restricted = item.get("restricted", False)
    ext = os.path.splitext(name or "")[1].lower().lstrip(".")

    print("\nFound file:")
    print(f"  File name: {name}")
    print(f"  Dataset:   {dataset_name}")
    print(f"  DOI:       {dataset_persistent_id}")
    print(f"  File ID:   {file_id}")
    print(f"  extension: {ext or '[none]'}")

    if restricted:
        print("  -> Skipping download (restricted=True).")
        return False

    if ext not in allowed_exts:
        print(f"  -> Skipping: extension .{ext} not in allowed set {allowed_exts}.")
        return False

    # Fetch dataset metadata (optional, no processing kept for brevity)
    if dataset_persistent_id:
        try:
            session.get(
                DATASET_ENDPOINT,
                params={"persistentId": dataset_persistent_id},
                timeout=30,
            ).raise_for_status()
        except requests.HTTPError as e:
            print(f"  -> Error fetching dataset metadata: {e}")
    else:
        print("  -> No dataset_persistent_id found, skipping metadata.")

    if not download_url:
        if not file_id:
            print("  -> Skipping: no download URL or file_id for item")
            return False
        download_url = f"{BASE_URL}/api/access/datafile/{file_id}"

    # Save each file in its own dataset folder: files/<dataset_name>/<original_filename>
    dataset_id = item.get("dataset_id") or item.get("datasetId") or dataset_persistent_id
    dataset_folder = str(dataset_id) if dataset_id else "unknown_dataset"
    dataset_folder = re.sub(r'[<>:"/\\\\|?*]', "_", dataset_folder)
    dataset_dir = os.path.join(FILES_DIR, dataset_folder)
    os.makedirs(dataset_dir, exist_ok=True)

    if not name:
        print("  -> Skipping: missing file name")
        return False

    dest_path = os.path.join(dataset_dir, name)

    if os.path.exists(dest_path):
        print(f"  -> Skipping: {name} already exists at {dest_path}")
        return False

    print(f"  -> Downloading {name} from {download_url} ...")

    try:
        with session.get(download_url, stream=False, timeout=120) as r:
            if not r.ok:
                # Show server-provided error message if JSON
                err_payload = r.json()
                print(f"     Download error {r.status_code}: {err_payload}")
                return False
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                f.write(r.content)
    except Exception as e:
        print(f"     Download failed: {e}")
        return False

    print(f"     Saved to {dest_path}")
    return True


def main():
    os.makedirs(FILES_DIR, exist_ok=True)

    # Load global list of allowed extensions from CSV (ignore project names)
    allowed_exts = set()
    csv_path = os.path.join(os.path.dirname(__file__), "QDA_files_extensions.csv")
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                for cell in row[1:]:  # skip project name column
                    cell = cell.strip()
                    if cell:
                        allowed_exts.add(cell.lstrip(".").lower())
    except FileNotFoundError:
        print("Extension config file QDA_files_extensions.csv not found; nothing will download.")
    except OSError as e:
        print(f"Error reading QDA_files_extensions.csv: {e}")

    if not allowed_exts:
        print("No extensions loaded from CSV; nothing will download.")
        return

    # Build a session (with API token if available)
    session = requests.Session()
    if API_TOKEN:
        session.headers.update({"X-Dataverse-key": API_TOKEN})

    # Build a targeted fileType query: fileType:.ext1 OR fileType:.ext2 ...
    query_terms = [f"fileType:.{ext}" for ext in sorted(allowed_exts)]
    search_query = " OR ".join(query_terms)

    print(f"Searching for files with extensions {sorted(allowed_exts)} on Harvard Dataverse...")

    start = 0
    per_page = 50
    total_processed = 0

    while True:
        params = {
            "q": search_query,
            "type": "file",
            "per_page": per_page,
            "start": start,
        }

        resp = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        print(payload)
        if payload.get("status") != "OK":
            print("Search API returned status:", payload.get("status"))
            break

        data = payload.get("data", {})
        items = data.get("items", [])
        total_count = data.get("total_count", 0)

        if not items:
            break

        for item in items:
            if process_item(session, item, allowed_exts):
                total_processed += 1

        start += per_page
        if start >= total_count:
            break

        time.sleep(0.5)  # be polite – small pause

    print(f"\nDone. Processed {total_processed} QDA file(s).")


if __name__ == "__main__":
    main()
