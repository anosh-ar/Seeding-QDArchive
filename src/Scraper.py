import os
import time
import json
import csv
import re
import sqlite3
from datetime import datetime
import requests

BASE_URL = "https://dataverse.harvard.edu"
FALLBACK_BASE_URL = "https://borealisdata.ca"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"
DATASET_ENDPOINT = f"{BASE_URL}/api/datasets/:persistentId"

# Optional: Dataverse API token (set in your environment if you have one)
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

# Put downloads in Seeding-QDArchive/files
FILES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "files"))
DB_PATH = os.path.join(os.path.dirname(__file__), "metadata.sqlite")
REPOSITORY_ID = 18
REPOSITORY_URL = "https://dataverse.harvard.edu/"
DOWNLOAD_REPOSITORY_FOLDER = "harvard-dataverse"
DOWNLOAD_METHOD = "API-CALL"
SKIP_EXTS = {
    "mp4",
    "m4v",
    "mov",
    "avi",
    "mkv",
    "webm",
    "wmv",
    "flv",
    "mpeg",
    "mpg",
    "m2v",
    "3gp",
    "3g2",
    "ts",
    "m2ts",
    "mts",
    "vob",
    "ogv",
    "rm",
    "rmvb",
    "mp3",
    "wav",
    "acc",
    "flac",
    "ogg",
    "wma",
    "m4a",
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PROJECTS (
            id INTEGER PRIMARY KEY,
            query_string TEXT,
            repository_id INTEGER,
            repository_url TEXT,
            project_url TEXT,
            version TEXT,
            title TEXT,
            description TEXT,
            language TEXT,
            doi TEXT,
            upload_date TEXT,
            download_date TEXT,
            download_repository_folder TEXT,
            download_project_folder TEXT UNIQUE,
            download_method TEXT
        )
        """
    )
    conn.commit()
    return conn


def _citation_field_value(metadata_blocks, type_name):
    fields = ((metadata_blocks or {}).get("citation") or {}).get("fields") or []
    for field in fields:
        if field.get("typeName") == type_name:
            return field.get("value")
    return None


def insert_project(conn, row):
    conn.execute(
        """
        INSERT OR IGNORE INTO PROJECTS (
            query_string,
            repository_id,
            repository_url,
            project_url,
            version,
            title,
            description,
            language,
            doi,
            upload_date,
            download_date,
            download_repository_folder,
            download_project_folder,
            download_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row.get("query_string"),
            REPOSITORY_ID,
            REPOSITORY_URL,
            row.get("project_url"),
            row.get("version"),
            row.get("title"),
            row.get("description"),
            "n/a",
            row.get("doi"),
            row.get("upload_date"),
            row.get("download_date"),
            DOWNLOAD_REPOSITORY_FOLDER,
            row.get("download_project_folder"),
            DOWNLOAD_METHOD,
        ),
    )
    conn.commit()


def process_item(session, conn, item, allowed_exts, seen_datasets, failed_projects):
    """Handle a single search result; download the whole dataset if an allowed file is found."""
    name = item.get("name")
    dataset_name = item.get("dataset_name")
    dataset_persistent_id = item.get("dataset_persistent_id")
    restricted = item.get("restricted", False)
    ext = os.path.splitext(name or "")[1].lower().lstrip(".")
    query_string = ext

    print("\nFound file:")
    print(f"  File name: {name}")
    print(f"  Dataset:   {dataset_name}")
    print(f"  DOI:       {dataset_persistent_id}")
    print(f"  extension: {ext or '[none]'}")

    if restricted:
        print("  -> Skipping download (restricted=True).")
        return 0

    if ext not in allowed_exts:
        print(f"  -> Skipping: extension .{ext} not in allowed set {allowed_exts}.")
        return 0

    if not dataset_persistent_id:
        print("  -> Skipping: no dataset_persistent_id; can't expand dataset.")
        return 0

    if dataset_persistent_id in seen_datasets:
        return 0

    payload = None
    dataset_base_url = None
    for base_url in (BASE_URL, FALLBACK_BASE_URL):
        try:
            resp = session.get(
                f"{base_url}/api/datasets/:persistentId",
                params={"persistentId": dataset_persistent_id},
                timeout=30,
            )
            resp.raise_for_status()
            candidate = resp.json()
            if candidate.get("status") == "OK" and candidate.get("data"):
                payload = candidate
                dataset_base_url = base_url
                break
        except Exception:
            pass

    if not payload:
        failed_projects.append(dataset_persistent_id)
        return 0

    data = payload.get("data", {})
    dataset_folder = str(data.get("id") or "unknown_dataset")
    dataset_dir = os.path.join(FILES_DIR, dataset_folder)

    if os.path.exists(dataset_dir):
        seen_datasets.add(dataset_persistent_id)
        return 0

    os.makedirs(dataset_dir, exist_ok=True)

    files = (data.get("latestVersion") or {}).get("files") or []
    downloaded = 0
    for entry in files:
        if entry.get("restricted"):
            continue
        data_file = entry.get("dataFile") or {}
        f_id = data_file.get("id")
        f_name = data_file.get("filename") or entry.get("label")
        if not f_id or not f_name:
            continue

        f_ext = os.path.splitext(f_name)[1].lower().lstrip(".")
        if f_ext in SKIP_EXTS:
            continue

        dest_path = os.path.join(dataset_dir, f_name)
        if os.path.exists(dest_path):
            continue

        download_url = f"{dataset_base_url}/api/access/datafile/{f_id}"
        print(f"  -> Downloading {f_name} ...")
        try:
            with session.get(download_url, stream=False, timeout=120) as r:
                if not r.ok:
                    try:
                        err_payload = r.json()
                    except Exception:
                        err_payload = r.text
                    print(f"     Download error {r.status_code}: {err_payload}")
                    continue
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    f.write(r.content)
            downloaded += 1
        except OSError as e:
            print(f"     Save failed for {f_name}: {e}")
        except Exception as e:
            print(f"     Download failed for {f_name}: {e}")

    meta = data.get("latestVersion") or {}
    dataset_persistent = meta.get("datasetPersistentId") or dataset_persistent_id
    metadata_blocks = meta.get("metadataBlocks") or {}
    title = _citation_field_value(metadata_blocks, "title")
    date_of_deposit = _citation_field_value(metadata_blocks, "dateOfDeposit")
    ds_desc = _citation_field_value(metadata_blocks, "dsDescription")
    description = None
    if isinstance(ds_desc, list) and ds_desc:
        first = ds_desc[0] or {}
        description = ((first.get("dsDescriptionValue") or {}).get("value")) or None

    insert_project(
        conn,
        {
            "query_string": query_string,
            "project_url": f"{dataset_base_url}/dataset.xhtml?persistentId={dataset_persistent}",
            "version": str(meta.get("versionNumber")) if meta.get("versionNumber") is not None else None,
            "title": title,
            "description": description,
            "doi": data.get("persistentUrl"),
            "upload_date": date_of_deposit,
            "download_date": datetime.now().isoformat(timespec="seconds"),
            "download_project_folder": dataset_folder,
        },
    )

    seen_datasets.add(dataset_persistent_id)

    if downloaded:
        print(f"     Saved {downloaded} file(s) to {dataset_dir}")
    return downloaded


def main():
    os.makedirs(FILES_DIR, exist_ok=True)
    conn = init_db()

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
    seen_datasets = set()
    failed_projects = []

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
        #print(payload)
        if payload.get("status") != "OK":
            print("Search API returned status:", payload.get("status"))
            break

        data = payload.get("data", {})
        items = data.get("items", [])
        total_count = data.get("total_count", 0)

        if not items:
            break

        for item in items:
            total_processed += process_item(session, conn, item, allowed_exts, seen_datasets, failed_projects)

        start += per_page
        if start >= total_count:
            break

        time.sleep(0.5)  # be polite – small pause

    print(f"\nDone. Downloaded {total_processed} file(s).")
    if failed_projects:
        print("\nFailed projects (DOI persistentId):")
        for doi in failed_projects:
            print(f"  {doi}")
    conn.close()


if __name__ == "__main__":
    main()
