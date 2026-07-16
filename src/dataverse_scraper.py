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
FALLBACK_BASE_URL_2 = "https://phys-techsciences.datastations.nl"
FALLBACK_BASE_URL_4 = "https://dataverse.nl"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"
DATASET_ENDPOINT = f"{BASE_URL}/api/datasets/:persistentId"

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
HARVARD_API_TOKEN = None

# Put downloads in Downloaded_Files/harvard-murray-archive
FILES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "Downloaded_Files", "harvard-murray-archive")
)
DB_PATH = os.path.join(os.path.dirname(__file__), "metadata.sqlite")
REPOSITORY_ID = 18
REPOSITORY_URL = "https://www.murray.harvard.edu/dataverse"
DOWNLOAD_REPOSITORY_FOLDER = "harvard-murray-archive"
DOWNLOAD_METHOD = "API-CALL"
KEYWORDS_CSV = os.path.join(os.path.dirname(__file__), "search_keywords.csv")
MAX_FILE_SIZE_BYTES = 500 * 1024 * 1024
RATE_LIMIT_SLEEP_SECONDS = 10 * 60
MAX_403_RETRIES = 3
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


def load_env_file(path):
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        pass


class RateLimit403Error(RuntimeError):
    pass


def session_get_with_403_retry(session, url, fatal_on_403=False, **kwargs):
    last = None
    for attempt in range(MAX_403_RETRIES):
        if last is not None:
            try:
                last.close()
            except Exception:
                pass
        last = session.get(url, **kwargs)
        if last.status_code != 403:
            return last
        minutes = RATE_LIMIT_SLEEP_SECONDS // 60
        if attempt < MAX_403_RETRIES - 1:
            print(f"  -> Got 403 (rate limit?). Code is paused for {minutes} minutes...")
            time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    if fatal_on_403 and last is not None and last.status_code == 403:
        raise RateLimit403Error("403 after retries")
    return last


def headers_for_base_url(base_url):
    if base_url == BASE_URL and HARVARD_API_TOKEN:
        return {"X-Dataverse-key": HARVARD_API_TOKEN}
    return None


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
        CREATE TABLE IF NOT EXISTS KEYWORDS (
            id INTEGER PRIMARY KEY,
            project_id INTEGER,
            keyword TEXT,
            UNIQUE(project_id, keyword),
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
            note TEXT,
            UNIQUE(project_id, file_name),
            FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
        )
        """
    )
    try:
        conn.execute("ALTER TABLE FILES ADD COLUMN note TEXT")
    except sqlite3.OperationalError:
        pass
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


def insert_keywords(conn, project_id, keywords):
    if not project_id or not keywords:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO KEYWORDS (project_id, keyword) VALUES (?, ?)",
        [(project_id, k) for k in keywords],
    )
    conn.commit()


def insert_files(conn, project_id, file_rows):
    if not project_id or not file_rows:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO FILES (project_id, file_name, file_type, status, note) VALUES (?, ?, ?, ?, ?)",
        [(project_id, r["file_name"], r["file_type"], r["status"], r.get("note")) for r in file_rows],
    )
    conn.commit()


def download_dataset(
    session,
    conn,
    dataset_persistent_id,
    dataset_id_hint,
    query_string,
    seen_datasets,
    failed_projects,
):
    if not dataset_persistent_id:
        return 0

    if dataset_persistent_id in seen_datasets:
        return 0

    if dataset_id_hint:
        hinted_folder = str(dataset_id_hint)
        hinted_dir = os.path.join(FILES_DIR, hinted_folder)
        if os.path.exists(hinted_dir):
            seen_datasets.add(dataset_persistent_id)
            return 0

    payload = None
    dataset_base_url = None
    attempt_errors = []
    base_urls = (BASE_URL, FALLBACK_BASE_URL, FALLBACK_BASE_URL_2, FALLBACK_BASE_URL_4)
    for base_url in base_urls:
        api_url = f"{base_url}/api/datasets/:persistentId?persistentId={dataset_persistent_id}"
        try:
            resp = session_get_with_403_retry(
                session,
                f"{base_url}/api/datasets/:persistentId",
                params={"persistentId": dataset_persistent_id},
                headers=headers_for_base_url(base_url),
                timeout=30,
            )
            if not resp.ok:
                try:
                    err_payload = resp.json()
                except Exception:
                    err_payload = resp.text
                err_text = err_payload
                if isinstance(err_payload, dict):
                    err_text = err_payload.get("message") or err_payload.get("error") or str(err_payload)
                if not isinstance(err_text, str):
                    err_text = str(err_text)
                err_text = " ".join(err_text.split())
                if len(err_text) > 500:
                    err_text = err_text[:500] + "..."
                attempt_errors.append(
                    (
                        base_url,
                        api_url,
                        f"HTTP {resp.status_code}: {err_text}",
                        f"{base_url}/dataset.xhtml?persistentId={dataset_persistent_id}",
                    )
                )
                continue

            candidate = resp.json()
            if candidate.get("status") == "OK" and candidate.get("data"):
                payload = candidate
                dataset_base_url = base_url
                break

            attempt_errors.append(
                (
                    base_url,
                    api_url,
                    f"API status {candidate.get('status')}: {candidate.get('message') or ''}".strip(),
                    f"{base_url}/dataset.xhtml?persistentId={dataset_persistent_id}",
                )
            )
        except Exception as e:
            attempt_errors.append(
                (base_url, api_url, str(e), f"{base_url}/dataset.xhtml?persistentId={dataset_persistent_id}")
            )

    if not payload:
        print(f"\nFailed to fetch dataset metadata for {dataset_persistent_id} from all fallbacks.")
        for base_url, api_url, err, project_url in attempt_errors:
            print(f"  Project URL: {project_url}")
            print(f"  API URL:     {api_url}")
            print(f"  Error: {err}")
        failed_projects.append(dataset_persistent_id)
        return 0

    data = payload.get("data", {})
    dataset_folder = str(dataset_id_hint or data.get("id") or "unknown_dataset")
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
        note = None
        data_file = entry.get("dataFile") or {}
        f_id = data_file.get("id")
        f_name = data_file.get("filename") or entry.get("label")
        if not f_name:
            continue

        f_ext = os.path.splitext(f_name)[1].lower().lstrip(".")

        if entry.get("restricted"):
            status = "SKIPPED"
            note = "RESTRICTED"
        elif f_ext in SKIP_EXTS:
            status = "SKIPPED"
            note = "Video or Audio File"
        elif not f_id:
            status = "FAILED"
        else:
            f_size = data_file.get("filesize")
            if isinstance(f_size, int) and f_size > MAX_FILE_SIZE_BYTES:
                status = "FAILED"
                note = f"Too large file size: {f_size / (1024**3):.2f} GB"
            else:
                dest_path = os.path.join(dataset_dir, f_name)
                if os.path.exists(dest_path):
                    status = "SKIPPED"
                else:
                    download_url = f"{dataset_base_url}/api/access/datafile/{f_id}"
                    print(f"  -> Downloading {f_name} ...")
                    try:
                        with session_get_with_403_retry(
                            session,
                            download_url,
                            fatal_on_403=True,
                            headers=headers_for_base_url(dataset_base_url),
                            stream=False,
                            timeout=120,
                        ) as r:
                            if not r.ok:
                                try:
                                    err_payload = r.json()
                                except Exception:
                                    err_payload = r.text
                                print(f"     Download error {r.status_code}: {err_payload}")
                                status = "FAILED"
                                if r.status_code == 403 and not note:
                                    note = "403 Forbidden (rate limit?)"
                            else:
                                r.raise_for_status()
                                with open(dest_path, "wb") as f:
                                    f.write(r.content)
                                status = "SUCCEEDED"
                    except RateLimit403Error:
                        print(f"     File still fails after {MAX_403_RETRIES} retries with error 403: {f_name}")
                        raise SystemExit(1)
                    except OSError as e:
                        print(f"     Save failed for {f_name}: {e}")
                        status = "FAILED"
                    except Exception as e:
                        print(f"     Download failed for {f_name}: {e}")
                        status = "FAILED"

        if status == "SUCCEEDED":
            downloaded += 1

        file_rows.append({"file_name": f_name, "file_type": f_ext, "status": status, "note": note})

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

        keywords_field = _citation_field_value(metadata_blocks, "keyword")
        keywords = set()
        if isinstance(keywords_field, list):
            for entry in keywords_field:
                raw = ((entry or {}).get("keywordValue") or {}).get("value")
                if not raw or not isinstance(raw, str):
                    continue
                for part in raw.split(","):
                    part = part.strip()
                    if part:
                        keywords.add(part)
        insert_keywords(conn, project_id, sorted(keywords))

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
    dataset_id = item.get("dataset_id") or item.get("datasetId")
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

    if dataset_id:
        dataset_dir = os.path.join(FILES_DIR, str(dataset_id))
        if os.path.exists(dataset_dir):
            seen_datasets.add(dataset_persistent_id)
            return 0

    return download_dataset(
        session,
        conn,
        dataset_persistent_id,
        dataset_id,
        query_string,
        seen_datasets,
        failed_projects,
    )


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


def get_last_query_string(conn):
    try:
        cur = conn.execute("SELECT query_string FROM PROJECTS ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None


def main():
    os.makedirs(FILES_DIR, exist_ok=True)
    db_exists = os.path.exists(DB_PATH)
    conn = init_db()

    allowed_exts = set()
    if not db_exists:
        # Load global list of allowed extensions from CSV (ignore project names)
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
            print("Extension config file QDA_files_extensions.csv not found; extension phase will be skipped.")
        except OSError as e:
            print(f"Error reading QDA_files_extensions.csv; extension phase will be skipped: {e}")

    load_env_file(ENV_PATH)

    # Build a session (with API token if available)
    session = requests.Session()
    global HARVARD_API_TOKEN
    HARVARD_API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

    # Build a targeted fileType query: fileType:.ext1 OR fileType:.ext2 ...
    total_processed = 0
    seen_datasets = set()
    failed_projects = []
    per_page = 50

    if not db_exists and allowed_exts:
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

                resp = session_get_with_403_retry(
                    session, SEARCH_ENDPOINT, params=params, headers=headers_for_base_url(BASE_URL), timeout=30
                )
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
    elif db_exists:
        last_q = get_last_query_string(conn)
        if last_q:
            print(f"\nResuming from last query_string in DB: {last_q}")

    # Keyword phase: search datasets by exact phrases and download whole datasets.
    phrases = load_keyword_phrases()
    if phrases:
        print("\nKeyword phase: searching datasets by phrases from search_keywords.csv ...")
        keyword_queries = ['"' + p.replace('"', '\\"') + '"' for p in phrases]

        start_index = 0
        last_query = get_last_query_string(conn) if db_exists else None
        if last_query and last_query in keyword_queries:
            start_index = keyword_queries.index(last_query)

        total_keyword_upper_bound = 0
        for q in keyword_queries[start_index:]:
            try:
                resp = session_get_with_403_retry(
                    session,
                    SEARCH_ENDPOINT,
                    params={"q": q, "type": "dataset", "per_page": 1, "start": 0},
                    headers=headers_for_base_url(BASE_URL),
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

        for keyword_query in keyword_queries[start_index:]:
            print(f"\nKeyword query: {keyword_query}")
            start = 0
            while True:
                params = {
                    "q": keyword_query,
                    "type": "dataset",
                    "per_page": per_page,
                    "start": start,
                }
                resp = session_get_with_403_retry(
                    session, SEARCH_ENDPOINT, params=params, headers=headers_for_base_url(BASE_URL), timeout=30
                )
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
                    dataset_id = item.get("dataset_id") or item.get("datasetId")
                    if dataset_persistent_id:
                        total_processed += download_dataset(
                            session,
                            conn,
                            dataset_persistent_id,
                            dataset_id,
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
