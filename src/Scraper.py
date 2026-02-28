import os
import time
import json
import requests

BASE_URL = "https://dataverse.harvard.edu"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"
DATASET_ENDPOINT = f"{BASE_URL}/api/datasets/:persistentId"

# Optional: Dataverse API token (set in your environment if you have one)
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

FILES_DIR = "../files"


def process_item(session, item):
    """Handle a single search result; return True if a file was saved."""
    name = item.get("name")
    dataset_name = item.get("dataset_name")
    dataset_persistent_id = item.get("dataset_persistent_id")
    file_id = item.get("file_id")
    download_url = item.get("url")
    can_download = item.get("canDownloadFile", False)

    print("\nFound .qdpx file:")
    print(f"  File name: {name}")
    print(f"  Dataset:   {dataset_name}")
    print(f"  DOI:       {dataset_persistent_id}")
    print(f"  File ID:   {file_id}")
    print(f"  canDownloadFile: {can_download}")

    if not can_download:
        print("  -> Skipping download (canDownloadFile=False).")
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

    safe_name = f"{file_id}_{name}" if file_id else name or "unknown.qdpx"
    dest_path = os.path.join(FILES_DIR, safe_name)

    print(f"  -> Downloading {safe_name} from {download_url} ...")

    with session.get(download_url, stream=False, timeout=120) as r:
        if r.status_code == 403:
            print("     Forbidden (maybe needs login / extra rights).")
            return False
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(r.content)

    print(f"     Saved to {dest_path}")
    return True


def main():
    os.makedirs(FILES_DIR, exist_ok=True)

    # Build a session (with API token if available)
    session = requests.Session()
    if API_TOKEN:
        session.headers.update({"X-Dataverse-key": API_TOKEN})

    print("Searching for .qdpx files on Harvard Dataverse...")

    start = 0
    per_page = 50
    total_processed = 0

    while True:
        params = {
            "q": "fileType:.qdpx",
            "type": "file",
            "per_page": per_page,
            "start": start,
        }

        resp = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("status") != "OK":
            print("Search API returned status:", payload.get("status"))
            break

        data = payload.get("data", {})
        items = data.get("items", [])
        total_count = data.get("total_count", 0)

        if not items:
            break

        for item in items:
            if process_item(session, item):
                total_processed += 1

        start += per_page
        if start >= total_count:
            break

        time.sleep(0.5)  # be polite – small pause

    print(f"\nDone. Processed {total_processed} .qdpx file(s).")


if __name__ == "__main__":
    main()
