import os
import time
import json
import requests

BASE_URL = "https://dataverse.harvard.edu"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"
DATASET_ENDPOINT = f"{BASE_URL}/api/datasets/:persistentId"

# Optional: Dataverse API token (set in your environment if you have one)
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

FILES_DIR = "harvard_qdpx_files"
META_DIR = "harvard_qdpx_authors"


# ---------------------------
# HTTP / API helpers
# ---------------------------

def get_session():
    """Create a requests Session with optional API token."""
    session = requests.Session()
    if API_TOKEN:
        session.headers.update({"X-Dataverse-key": API_TOKEN})
    return session


def search_qdpx_files(session, per_page=50):
    """
    Generator. Yields search result items for .qdpx files.

    Uses the Harvard Dataverse Search API with:
        q = "fileType:.qdpx"
        type = "file"
    and walks through all pages.
    """
    start = 0

    while True:
        params = {
            "q": "fileType:.qdpx",
            "type": "file",
            "per_page": per_page,
            "start": start,
            # Optional: restrict to main Harvard tree
            # "subtree": "harvard",
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
            name = item.get("name", "")
            # Should already be .qdpx due to fileType filter, but small check is ok
            if name.lower().endswith(".qdpx"):
                yield item

        start += per_page
        if start >= total_count:
            break

        time.sleep(0.5)  # be polite – small pause


def fetch_dataset_metadata(session, dataset_persistent_id):
    """
    Fetch full dataset metadata using the persistentId (DOI).
    """
    params = {"persistentId": dataset_persistent_id}
    resp = session.get(DATASET_ENDPOINT, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------
# Metadata extraction
# ---------------------------

def extract_authors_affiliations(dataset_json):
    """
    From the dataset metadata JSON, extract a list of authors.

    Returns a list of dicts:
        [
          {"name": "...", "affiliation": "..."},
          ...
        ]
    """
    authors = []

    data = dataset_json.get("data", {})
    latest_version = data.get("latestVersion", {})
    metadata_blocks = latest_version.get("metadataBlocks", {})
    citation_block = metadata_blocks.get("citation", {})
    fields = citation_block.get("fields", [])

    for field in fields:
        if field.get("typeName") != "author":
            continue

        # value is usually a list of author objects
        for author_obj in field.get("value", []):
            # compound structure: authorName, authorAffiliation
            name_field = author_obj.get("authorName", {})
            aff_field = author_obj.get("authorAffiliation", {})

            name = name_field.get("value")
            affiliation = aff_field.get("value")

            authors.append(
                {
                    "name": name,
                    "affiliation": affiliation,
                }
            )

    return authors


def save_authors_metadata(dataset_id, dataset_persistent_id, dataset_name, authors, out_dir):
    """
    Save a small JSON file that only contains:
        - dataset_id
        - dataset_persistent_id (DOI)
        - dataset_name
        - authors (name + affiliation)
    """
    os.makedirs(out_dir, exist_ok=True)

    data_to_save = {
        "dataset_id": dataset_id,
        "dataset_persistent_id": dataset_persistent_id,
        "dataset_name": dataset_name,
        "authors": authors,
    }

    file_path = os.path.join(out_dir, f"{dataset_id}_authors.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, indent=2, ensure_ascii=False)

    print(f"  -> Saved authors metadata to {file_path}")


# ---------------------------
# File download
# ---------------------------

def download_qdpx_file(session, item, output_dir):
    """
    Download the .qdpx file if canDownloadFile == True.
    """
    file_name = item.get("name", "unknown.qdpx")
    file_id = item.get("file_id")
    download_url = item.get("url")
    can_download = item.get("canDownloadFile", False)

    if not can_download:
        print(f"  -> Skipping download (canDownloadFile=False): {file_name}")
        return

    if not download_url:
        if not file_id:
            print("  -> Skipping: no download URL or file_id for item")
            return
        download_url = f"{BASE_URL}/api/access/datafile/{file_id}"

    if file_id:
        safe_name = f"{file_id}_{file_name}"
    else:
        safe_name = file_name

    os.makedirs(output_dir, exist_ok=True)
    dest_path = os.path.join(output_dir, safe_name)

    print(f"  -> Downloading {safe_name} from {download_url} ...")

    with session.get(download_url, stream=True, timeout=120) as r:
        if r.status_code == 403:
            print("     Forbidden (maybe needs login / extra rights).")
            return
        r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"     Saved to {dest_path}")


# ---------------------------
# High-level processing
# ---------------------------

def process_qdpx_item(session, item):
    """
    Handle one search result item:
      - print basic info
      - fetch dataset metadata
      - extract and save authors + affiliations
      - download the file (if allowed)
    """
    name = item.get("name")
    dataset_name = item.get("dataset_name")
    dataset_persistent_id = item.get("dataset_persistent_id")
    dataset_id = item.get("dataset_id")
    can_download = item.get("canDownloadFile")

    print("\nFound .qdpx file:")
    print(f"  File name: {name}")
    print(f"  Dataset:   {dataset_name}")
    print(f"  DOI:       {dataset_persistent_id}")
    print(f"  File ID:   {item.get('file_id')}")
    print(f"  canDownloadFile: {can_download}")

    # 1) fetch dataset metadata and extract authors
    if dataset_persistent_id:
        try:
            dataset_json = fetch_dataset_metadata(session, dataset_persistent_id)
            authors = extract_authors_affiliations(dataset_json)
            save_authors_metadata(dataset_id, dataset_persistent_id, dataset_name, authors, META_DIR)
        except requests.HTTPError as e:
            print(f"  -> Error fetching dataset metadata: {e}")
    else:
        print("  -> No dataset_persistent_id found, skipping metadata.")

    # 2) try to download file
    download_qdpx_file(session, item, FILES_DIR)


# ---------------------------
# Main
# ---------------------------

def main():
    os.makedirs(FILES_DIR, exist_ok=True)
    os.makedirs(META_DIR, exist_ok=True)

    session = get_session()

    print("Searching for .qdpx files on Harvard Dataverse...")

    count = 0
    for item in search_qdpx_files(session, per_page=50):
        process_qdpx_item(session, item)
        count += 1

    print(f"\nDone. Processed {count} .qdpx file(s).")


if __name__ == "__main__":
    main()