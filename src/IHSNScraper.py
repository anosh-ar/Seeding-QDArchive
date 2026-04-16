import csv
import os
import re
import sqlite3
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote

import requests
from requests import exceptions as req_exc

BASE_URL = "https://catalog.ihsn.org"
SEARCH_ENDPOINTS = (
    f"{BASE_URL}/index.php/api/catalog/search",
    f"{BASE_URL}/api/catalog/search",
)
DETAIL_ENDPOINTS = (
    f"{BASE_URL}/index.php/api/catalog",
    f"{BASE_URL}/api/catalog",
)

FILES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "files"))
IHSN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Downloaded_Files", "IHSN"))
DB_PATH = os.path.join(os.path.dirname(__file__), "metadata.sqlite")

REPOSITORY_ID = 9
REPOSITORY_URL = f"{BASE_URL}/"
DOWNLOAD_REPOSITORY_FOLDER = "IHSN"
DOWNLOAD_METHOD = "SCRAPING"

KEYWORDS_CSV = os.path.join(os.path.dirname(__file__), "search_keywords.csv")
THROTTLE_SLEEP_SECONDS = int(os.getenv("IHSN_THROTTLE_SLEEP_SECONDS") or "600")
THROTTLE_MAX_RETRIES = int(os.getenv("IHSN_THROTTLE_MAX_RETRIES") or "3")


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
            project_id INTEGER,
            name TEXT,
            role TEXT,
            UNIQUE(project_id, name, role),
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
            UNIQUE(project_id, file_name),
            FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
        )
        """
    )
    conn.commit()
    _ensure_person_role_schema(conn)
    return conn


def _ensure_person_role_schema(conn):
    """
    Migrate legacy PERSON_ROLE schema that used `project_id INTEGER UNIQUE`,
    which prevents storing multiple producers per project.
    """
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='PERSON_ROLE'",
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return

    sql = row[0].lower()
    if "project_id integer unique" not in sql:
        return

    conn.execute("BEGIN")
    try:
        conn.execute(
            """
            CREATE TABLE PERSON_ROLE_NEW (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                name TEXT,
                role TEXT,
                UNIQUE(project_id, name, role),
                FOREIGN KEY (project_id) REFERENCES PROJECTS(id)
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO PERSON_ROLE_NEW (id, project_id, name, role)
            SELECT id, project_id, name, role FROM PERSON_ROLE
            """
        )
        conn.execute("DROP TABLE PERSON_ROLE")
        conn.execute("ALTER TABLE PERSON_ROLE_NEW RENAME TO PERSON_ROLE")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


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
            row.get("language") or "n/a",
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


def project_exists(conn, download_project_folder):
    try:
        cur = conn.execute(
            "SELECT id FROM PROJECTS WHERE download_project_folder = ? AND download_repository_folder = ? LIMIT 1",
            (download_project_folder, DOWNLOAD_REPOSITORY_FOLDER),
        )
        return cur.fetchone() is not None
    except Exception:
        return False


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
        "INSERT OR IGNORE INTO FILES (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)",
        [(project_id, r["file_name"], r["file_type"], r["status"]) for r in file_rows],
    )
    conn.commit()


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


def _safe_folder_part(value):
    value = str(value or "").strip()
    if not value:
        return ""
    value = re.sub(r"[^\w.-]+", "_", value, flags=re.UNICODE)
    value = value.strip("._")
    return value[:120]


def _guess_filename_from_url(url, fallback):
    try:
        path = urlparse(url).path or ""
        name = os.path.basename(path.rstrip("/"))
        name = unquote(name)
        name = name.strip()
        if name:
            return name
    except Exception:
        pass
    return fallback


def _file_ext(name):
    return os.path.splitext(name)[1].lower().lstrip(".")


def _looks_like_downloadable_file(url):
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        # Many NADA downloads are routed through "/catalog/<id>/download/..."
        # and may not expose a filename in the URL.
        if "/download" in path:
            return True
        base = os.path.basename(path.rstrip("/"))
        if not base or "." not in base:
            return False
        ext = _file_ext(base)
        if not ext or len(ext) > 10:
            return False
        return True
    except Exception:
        return False


def _json_get(session, url, params=None, timeout=30):
    # `requests` timeout is (connect, read). Keeping connect short avoids "hangs"
    # on network/proxy issues where the OS can take a long time to fail.
    if not isinstance(timeout, tuple):
        timeout = (10, timeout)

    def is_throttled_status(code):
        return code in (403, 429, 500, 502, 503, 504)

    last_exc = None
    for attempt in range(1, THROTTLE_MAX_RETRIES + 2):  # initial try + retries
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if is_throttled_status(resp.status_code):
                msg = f"HTTP {resp.status_code}"
                if attempt <= THROTTLE_MAX_RETRIES:
                    print(
                        f"  -> Throttled ({msg}) on {url}. Sleeping {THROTTLE_SLEEP_SECONDS}s then retrying (attempt {attempt}/{THROTTLE_MAX_RETRIES})..."
                    )
                    time.sleep(THROTTLE_SLEEP_SECONDS)
                    continue
            resp.raise_for_status()
            return resp.json()
        except (req_exc.ConnectTimeout, req_exc.ReadTimeout, req_exc.ConnectionError) as e:
            last_exc = e
            if attempt <= THROTTLE_MAX_RETRIES:
                print(
                    f"  -> Network error ({type(e).__name__}) on {url}. Sleeping {THROTTLE_SLEEP_SECONDS}s then retrying (attempt {attempt}/{THROTTLE_MAX_RETRIES})..."
                )
                time.sleep(THROTTLE_SLEEP_SECONDS)
                continue
            raise
        except Exception as e:
            last_exc = e
            raise
    raise last_exc  # pragma: no cover


def _nada_search(session, params):
    last_err = None
    for endpoint in SEARCH_ENDPOINTS:
        try:
            payload = _json_get(session, endpoint, params=params, timeout=30)
            if isinstance(payload, dict) and isinstance(payload.get("result"), dict) and isinstance(payload["result"].get("rows"), list):
                return payload
        except Exception as e:
            last_err = e
    raise RuntimeError(f"NADA search failed for all endpoints: {last_err}")


def _nada_detail(session, study_id_or_idno):
    last_err = None
    for base in DETAIL_ENDPOINTS:
        for suffix in (str(study_id_or_idno),):
            try:
                url = f"{base}/{suffix}"
                payload = _json_get(session, url, timeout=30)
                if isinstance(payload, dict):
                    return payload
            except Exception as e:
                last_err = e
    return {"_detail_error": str(last_err) if last_err else "unknown"}


def _collect_download_urls(obj):
    urls = set()
    if isinstance(obj, dict):
        for v in obj.values():
            urls.update(_collect_download_urls(v))
    elif isinstance(obj, list):
        for v in obj:
            urls.update(_collect_download_urls(v))
    elif isinstance(obj, str):
        if obj.startswith("http://") or obj.startswith("https://"):
            urls.add(obj)
    return urls


_HREF_RE = re.compile(r"""href\s*=\s*(?P<q>["'])(?P<u>.*?)(?P=q)""", re.IGNORECASE | re.DOTALL)


def _collect_download_urls_from_study_page(html, base_url):
    urls = set()
    for match in _HREF_RE.finditer(html or ""):
        href = (match.group("u") or "").strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        if not abs_url.startswith(BASE_URL):
            continue
        if "/download" in urlparse(abs_url).path:
            urls.add(abs_url)
    return urls


_CD_FILENAME_RE = re.compile(r"""filename\*?=(?:UTF-8''|")?(?P<name>[^\";\r\n]+)""", re.IGNORECASE)


def _guess_filename_from_headers(url, headers, fallback):
    try:
        cd = headers.get("content-disposition") or headers.get("Content-Disposition") or ""
        m = _CD_FILENAME_RE.search(cd)
        if m:
            name = unquote((m.group("name") or "").strip().strip('"'))
            if name:
                return name
    except Exception:
        pass
    return _guess_filename_from_url(url, fallback)


def _sanitize_filename(name):
    name = os.path.basename(str(name or "").strip())
    name = name.replace("\x00", "")
    name = re.sub(r"[<>:\"/\\\\|?*]+", "_", name)
    name = name.strip().strip(".")
    return name[:200] or "downloaded_file"


def _deep_get(obj, *path, default=None):
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


_DOI_RE = re.compile(r"\bdoi\s*:\s*([^\s,;]+)", re.IGNORECASE)


def _extract_doi(series_info):
    if not series_info or not isinstance(series_info, str):
        return None
    m = _DOI_RE.search(series_info)
    if not m:
        return None
    doi = (m.group(1) or "").strip()
    return doi or None


def _download_one(session, url, dest_dir, fallback_name):
    """
    Download a NADA /download/... URL and return a FILES row dict.
    Uses Content-Disposition to determine the real filename when available.
    """
    fallback_name = _sanitize_filename(fallback_name)
    def is_throttled_status(code):
        return code in (403, 429, 500, 502, 503, 504)

    last_err = None
    for attempt in range(1, THROTTLE_MAX_RETRIES + 2):
        try:
            with session.get(url, stream=True, timeout=(10, 120)) as r:
                if is_throttled_status(r.status_code):
                    last_err = f"http {r.status_code}"
                    if attempt <= THROTTLE_MAX_RETRIES:
                        print(
                            f"    -> Throttled ({last_err}) downloading {url}. Sleeping {THROTTLE_SLEEP_SECONDS}s then retrying (attempt {attempt}/{THROTTLE_MAX_RETRIES})..."
                        )
                        time.sleep(THROTTLE_SLEEP_SECONDS)
                        continue

                if not r.ok:
                    return (
                        {"file_name": fallback_name, "file_type": _file_ext(fallback_name), "status": "FAILED"},
                        f"http {r.status_code}",
                    )

                content_type = (r.headers.get("content-type") or "").lower()
                if "text/html" in content_type:
                    return (
                        {"file_name": fallback_name, "file_type": _file_ext(fallback_name), "status": "FAILED"},
                        "html response",
                    )

                suggested = _guess_filename_from_headers(url, r.headers, fallback_name)
                suggested = _sanitize_filename(suggested)
                dest_path = os.path.join(dest_dir, suggested)

                if os.path.exists(dest_path):
                    # User requirement: if the file is already on disk, assume the DB already has
                    # a SUCCEEDED row; don't write a new FILES row that would be SKIPPED.
                    return {"file_name": suggested, "file_type": _file_ext(suggested), "status": "EXISTS"}, None

                os.makedirs(dest_dir, exist_ok=True)
                r.raise_for_status()
                try:
                    with open(dest_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 128):
                            if chunk:
                                f.write(chunk)
                except OSError as e:
                    return {"file_name": suggested, "file_type": _file_ext(suggested), "status": "FAILED"}, str(e)

                return {"file_name": suggested, "file_type": _file_ext(suggested), "status": "SUCCEEDED"}, None
        except (req_exc.ConnectTimeout, req_exc.ReadTimeout, req_exc.ConnectionError) as e:
            last_err = f"{type(e).__name__}"
            if attempt <= THROTTLE_MAX_RETRIES:
                print(
                    f"    -> Network error ({last_err}) downloading {url}. Sleeping {THROTTLE_SLEEP_SECONDS}s then retrying (attempt {attempt}/{THROTTLE_MAX_RETRIES})..."
                )
                time.sleep(THROTTLE_SLEEP_SECONDS)
                continue
            return {"file_name": fallback_name, "file_type": _file_ext(fallback_name), "status": "FAILED"}, last_err

    return {"file_name": fallback_name, "file_type": _file_ext(fallback_name), "status": "FAILED"}, last_err or "unknown"


def _download_known_exports(session, study_id):
    base = f"{BASE_URL}/index.php/catalog/{study_id}/download"
    candidates = [
        (f"{base}/json", f"metadata_{study_id}.json"),
        (f"{base}/ddi", f"metadata_{study_id}.xml"),
        (f"{base}/pdf", f"metadata_{study_id}.pdf"),
    ]
    return candidates


def process_study(session, conn, row, query_string, allowed_exts, seen_folders):
    study_id = row.get("id")
    idno = row.get("idno")
    title = row.get("title")

    if not study_id and not idno:
        return 0

    # Always resolve details via idno; on catalog.ihsn.org the public study API
    # works reliably with idno (and not always with numeric ids).
    detail = _nada_detail(session, idno or study_id)
    if isinstance(detail, dict) and isinstance(detail.get("dataset"), dict) and detail["dataset"].get("id"):
        study_id = detail["dataset"]["id"]

    study_id_str = _safe_folder_part(study_id) or "unknown"
    print(f"  Study: {study_id_str}" + (f" (idno={idno})" if idno else ""))

    # User requirement: use dataset.id as the project folder (both on disk and in metadata).
    download_project_folder = study_id_str
    if download_project_folder in seen_folders:
        print("    Skipping: already processed in this run (duplicate from another query).")
        return 0
    if project_exists(conn, download_project_folder):
        print("    Skipping: already in metadata.sqlite (previously explored).")
        seen_folders.add(download_project_folder)
        return 0

    seen_folders.add(download_project_folder)

    dataset_dir = os.path.join(IHSN_DIR, download_project_folder)

    project_url = row.get("url") or f"{BASE_URL}/catalog/{study_id}"

    detail_obj = (
        (detail.get("dataset") if isinstance(detail, dict) else None)
        or (detail.get("result") if isinstance(detail, dict) and isinstance(detail.get("result"), dict) else None)
        or detail
    )

    description = None
    for k in ("abstract", "study_desc", "description"):
        v = detail_obj.get(k) if isinstance(detail_obj, dict) else None
        if isinstance(v, str) and v.strip():
            description = v.strip()
            break
    if not description:
        description = "n/a"

    upload_date = row.get("created") or (detail_obj.get("created") if isinstance(detail_obj, dict) else None)

    # version_statement.version (prefer doc_desc, fall back to study_desc, then search-row changed date)
    version = (
        _deep_get(detail_obj, "metadata", "doc_desc", "version_statement", "version")
        or _deep_get(detail_obj, "metadata", "study_desc", "version_statement", "version")
        or "n/a"
    )

    # doi is stored in series_statement.series_info (example: "DOI:10.18128/D020.V7.5")
    series_info = _deep_get(detail_obj, "metadata", "study_desc", "series_statement", "series_info")
    doi = _extract_doi(series_info) or "n/a"

    project_row = {
        "query_string": query_string,
        "project_url": project_url,
        "version": version,
        "title": title,
        "description": description,
        "doi": doi or "n/a",
        "upload_date": upload_date,
        "download_date": datetime.now().isoformat(timespec="seconds"),
        "download_project_folder": download_project_folder,
    }

    people_for_db = []
    authoring_entity = None
    producers_roles = []
    if isinstance(detail_obj, dict):
        # PERSON_ROLE: name from authoring_entity, role from producers.role (if present).
        authoring_entity = (detail_obj.get("authoring_entity") or "").strip() or None
        producers = (((detail_obj.get("metadata") or {}).get("doc_desc") or {}).get("producers")) or []
        if isinstance(producers, list):
            for p in producers:
                if not isinstance(p, dict):
                    continue
                role = (p.get("role") or "").strip()
                if role:
                    producers_roles.append(role)
                if not authoring_entity:
                    pn = (p.get("name") or "").strip()
                    if pn:
                        authoring_entity = pn

    if not authoring_entity:
        authoring_entity = (row.get("authoring_entity") or "").strip() or None

    if authoring_entity:
        if not producers_roles:
            producers_roles = ["UNKNOWN"]
        for role in producers_roles:
            people_for_db.append((authoring_entity, role))

    file_rows = []
    download_urls = set(_collect_download_urls(detail_obj))

    # catalog.ihsn.org exposes study downloadable documents via:
    #   https://catalog.ihsn.org/catalog/<id>/related-materials
    # That HTML contains "/catalog/<id>/download/<file_id>" links.
    if study_id:
        related_url = f"{BASE_URL}/catalog/{study_id}/related-materials"
        try:
            resp = session.get(related_url, timeout=(10, 30))
            if resp.ok and "text/html" in (resp.headers.get("content-type") or "").lower():
                download_urls.update(_collect_download_urls_from_study_page(resp.text, related_url))
        except Exception:
            pass

    download_links = [u for u in sorted(download_urls) if isinstance(u, str) and "/download" in urlparse(u).path]
    print(f"    Download links: {len(download_links)}")
    if not download_links:
        print("    Skipping: no downloadable files found on related-materials page.")
        return 0

    has_any_local_file = False
    for url in sorted(download_urls):
        if not isinstance(url, str):
            continue
        if "://" not in url:
            continue
        if not url.startswith(BASE_URL):
            continue
        # Only download explicit NADA download routes from the related-materials page.
        if "/download" not in urlparse(url).path:
            continue

        filename = _guess_filename_from_url(url, f"download_{os.path.basename(urlparse(url).path)}")
        try:
            row_dict, err = _download_one(session, url, dataset_dir, filename)
            if row_dict.get("status") != "EXISTS":
                file_rows.append(row_dict)
            if row_dict["status"] in ("SUCCEEDED", "EXISTS"):
                has_any_local_file = True
        except Exception:
            file_rows.append({"file_name": filename, "file_type": _file_ext(filename), "status": "FAILED"})

        time.sleep(0.2)

    if not has_any_local_file:
        # User requirement: don't write metadata unless at least one file exists locally.
        try:
            if os.path.isdir(dataset_dir) and not os.listdir(dataset_dir):
                os.rmdir(dataset_dir)
        except OSError:
            pass
        print("    Skipping: no file saved (all downloads failed and no existing local files).")
        return 0

    project_id = insert_project(conn, project_row)
    if project_id:
        if file_rows:
            insert_files(conn, project_id, file_rows)
        insert_license(conn, project_id, "ACADEMIC_USE_ONLY,RESTRICTED_RESEARCH_LICENSE")
        for name, role in people_for_db:
            insert_person_role(conn, project_id, name, role)

    succeeded = sum(1 for r in file_rows if r["status"] == "SUCCEEDED")
    if succeeded:
        print(f"    Downloaded: {succeeded}")
    return succeeded


def main():
    os.makedirs(IHSN_DIR, exist_ok=True)
    conn = init_db()

    session = requests.Session()
    session.headers.update({"User-Agent": "Seeding-QDArchive/1.0 (+https://github.com/ihsn/nada)"})

    phrases = load_keyword_phrases()
    if not phrases:
        print("No search phrases found in search_keywords.csv; nothing will download.")
        return

    seen_folders = set()
    ps = int(os.getenv("IHSN_PAGE_SIZE") or "50")
    max_studies = int(os.getenv("IHSN_MAX_STUDIES_PER_KEYWORD") or "0")
    total_downloaded = 0

    for phrase in phrases:
        query_string = phrase
        print(f"\nQuery: {query_string}")
        page = 1
        processed_for_keyword = 0
        while True:
            # catalog.ihsn.org paginates with `page` (1-based). `offset` is ignored.
            params = {"ps": ps, "page": page}
            params["study_keywords"] = phrase

            payload = _nada_search(session, params=params)
            result = payload.get("result") or {}
            rows = result.get("rows") or []
            found = int(result.get("found") or 0)
            returned_offset = int(result.get("offset") or 0)
            limit = int(result.get("limit") or len(rows) or ps)

            if not rows:
                break

            print(f"  Page: page={page} offset={returned_offset} rows={len(rows)} found={found}")
            for r in rows:
                try:
                    total_downloaded += process_study(session, conn, r, query_string, None, seen_folders)
                except Exception as e:
                    print(f"  -> Failed processing study id={r.get('id')}: {e}")
                time.sleep(0.2)
                processed_for_keyword += 1
                if max_studies and processed_for_keyword >= max_studies:
                    print(f"  Reached IHSN_MAX_STUDIES_PER_KEYWORD={max_studies}; stopping this keyword.")
                    break

            if max_studies and processed_for_keyword >= max_studies:
                break

            if found and (returned_offset + limit) >= found:
                break
            page += 1

    print(f"\nDone. Downloaded {total_downloaded} file(s).")


if __name__ == "__main__":
    main()
