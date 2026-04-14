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
KEYWORDS_CSV = os.path.join(os.path.dirname(__file__), "search_keywords.csv")
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
    conn.execute("PRAGMA foreign_keys = ON")
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS PERSON_ROLE (
            id INTEGER PRIMARY KEY,
            project_id INTEGER UNIQUE,
            name TEXT,
            role TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS LICENSES (
            id INTEGER PRIMARY KEY,
            project_id INTEGER UNIQUE,
            license TEXT,
            FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS FILES (
            id INTEGER PRIMARY KEY,
            project_id INTEGER,
            file_name TEXT,
            file_type TEXT,
            status TEXT CHECK(status IN ('SUCCEEDED','FAILED','SKIPPED')),
            UNIQUE(project_id, file_name),
            FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
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
    cur = conn.execute(
        "SELECT id FROM PROJECTS WHERE download_project_folder = ?",
        (row.get("download_project_folder"),),
    )
    res = cur.fetchone()
    return res[0] if res else None


def insert_person_role(conn, project_id, name, role):
    conn.execute(
        "INSERT OR IGNORE INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)",
        (project_id, name, role),
    )
    conn.commit()


def insert_license(conn, project_id, license_name):
    conn.execute(
        "INSERT OR IGNORE INTO LICENSES (project_id, license) VALUES (?, ?)",
        (project_id, license_name),
    )
    conn.commit()


def insert_files(conn, project_id, file_rows):
    if not project_id or not file_rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO FILES (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
        [(project_id, r["file_name"], r["file_type"], r["status"]) for r in file_rows],
    )
    conn.commit()


def download_dataset(session, conn, dataset_persistent_id, query_string, seen_datasets, failed_projects):
    if not dataset_persistent_id:
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
    file_rows = []
    for entry in files:
        status = "SKIPPED"
        data_file = entry.get("dataFile") or {}
        f_id = data_file.get("id")
        f_name = data_file.get("filename") or entry.get("label")
        if not f_name:
            continue

        f_ext = os.path.splitext(f_name)[1].lower().lstrip(".")

        if entry.get("restricted"):
            status = "SKIPPED"
        elif f_ext in SKIP_EXTS:
            status = "SKIPPED"
        elif not f_id:
            status = "FAILED"
        else:
            dest_path = os.path.join(dataset_dir, f_name)
            if os.path.exists(dest_path):
                status = "SKIPPED"
            else:
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
                            status = "FAILED"
                        else:
                            r.raise_for_status()
                            with open(dest_path, "wb") as f:
                                f.write(r.content)
                            status = "SUCCEEDED"
                except OSError as e:
                    print(f"     Save failed for {f_name}: {e}")
                    status = "FAILED"
                except Exception as e:
                    print(f"     Download failed for {f_name}: {e}")
                    status = "FAILED"

        if status == "SUCCEEDED":
            downloaded += 1

        file_rows.append({"file_name": f_name, "file_type": f_ext, "status": status})

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

    project_id = insert_project(
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

    author = _citation_field_value(metadata_blocks, "author")
    author_name = None
    if isinstance(author, list) and author:
        author_name = ((author[0] or {}).get("authorName") or {}).get("value")

    if project_id:
        insert_person_role(conn, project_id, author_name, "Author" if author_name else "UNKNOWN")
        insert_license(conn, project_id, (meta.get("license") or {}).get("name"))
        insert_files(conn, project_id, file_rows)

    seen_datasets.add(dataset_persistent_id)

    if downloaded:
        print(f"     Saved {downloaded} file(s) to {dataset_dir}")
    return downloaded


def process_item(session, conn, item, allowed_exts, seen_datasets, failed_projects):
    """Handle a single file search result; download the whole dataset."""
    name = item.get("name")
    dataset_name = item.get("dataset_name")
    dataset_persistent_id = item.get("dataset_persistent_id")
    restricted = item.get("restricted", False)
    query_string = item.get("_query_string")

    print("\nFound file:")
    print(f"  File name: {name}")
    print(f"  Dataset:   {dataset_name}")
    print(f"  DOI:       {dataset_persistent_id}")

    if restricted:
        print("  -> Skipping download (restricted=True).")
        return 0

    if not dataset_persistent_id:
        print("  -> Skipping: no dataset_persistent_id; can't expand dataset.")
        return 0

    return download_dataset(session, conn, dataset_persistent_id, query_string, seen_datasets, failed_projects)


def load_keyword_phrases():
    phrases = []
    try:
        with open(KEYWORDS_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                q = (row.get("query") or "").strip()
                if q:
                    phrases.append(q)
    except FileNotFoundError:
        return []
    except OSError:
        return []
    return phrases


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
    total_processed = 0
    seen_datasets = set()
    failed_projects = []
    per_page = 50

    print(f"Searching for files with extensions {sorted(allowed_exts)} on Harvard Dataverse...")
    for ext in sorted(allowed_exts):
        search_query = f"fileType:.{ext}"
        print(f"\nExtension query: {search_query}")
        start = 0
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
            if payload.get("status") != "OK":
                print("Search API returned status:", payload.get("status"))
                break

            data = payload.get("data", {})
            items = data.get("items", [])
            total_count = data.get("total_count", 0)

            if not items:
                break

            for item in items:
                item["_query_string"] = search_query
                total_processed += process_item(session, conn, item, allowed_exts, seen_datasets, failed_projects)

            start += per_page
            if start >= total_count:
                break

            time.sleep(0.5)  # be polite – small pause

    print(f"\nExtension phase done. Downloaded {total_processed} file(s).")

    # Keyword phase: search datasets by exact phrases and download whole datasets.
    phrases = load_keyword_phrases()
    if phrases:
        print("\nKeyword phase: searching datasets by phrases from search_keywords.csv ...")
        total_keyword_upper_bound = 0
        for phrase in phrases:
            q = '"' + phrase.replace('"', '\\"') + '"'
            try:
                resp = session.get(
                    SEARCH_ENDPOINT,
                    params={"q": q, "type": "dataset", "per_page": 1, "start": 0},
                    timeout=30,
                )
                resp.raise_for_status()
                payload = resp.json()
                total_keyword_upper_bound += (
                    (payload.get("data") or {}).get("total_count", 0) if payload.get("status") == "OK" else 0
                )
            except Exception:
                pass
        print(f"Keyword phase: will process up to {total_keyword_upper_bound} dataset(s).")

        for phrase in phrases:
            keyword_query = '"' + phrase.replace('"', '\\"') + '"'
            print(f"\nKeyword query: {keyword_query}")
            start = 0
            while True:
                params = {
                    "q": keyword_query,
                    "type": "dataset",
                    "per_page": per_page,
                    "start": start,
                }
                resp = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("status") != "OK":
                    break

                data = payload.get("data", {})
                items = data.get("items", [])
                total_count = data.get("total_count", 0)
                if not items:
                    break

                for item in items:
                    dataset_persistent_id = (
                        item.get("dataset_persistent_id")
                        or item.get("global_id")
                        or item.get("globalId")
                        or item.get("persistentId")
                    )
                    if dataset_persistent_id:
                        total_processed += download_dataset(
                            session,
                            conn,
                            dataset_persistent_id,
                            keyword_query,
                            seen_datasets,
                            failed_projects,
                        )

                start += per_page
                if start >= total_count:
                    break
                time.sleep(0.5)

    if failed_projects:
        print("\nFailed projects (DOI persistentId):")
        for doi in failed_projects:
            print(f"  {doi}")

    print(f"\nAll phases done. Downloaded {total_processed} file(s).")
    conn.close()


if __name__ == "__main__":
    main()
