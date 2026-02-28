import os
import time
import requests

BASE_URL = "https://dataverse.harvard.edu"
SEARCH_ENDPOINT = f"{BASE_URL}/api/search"

# Optional: Dataverse API token (for restricted files, if you have access)
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")

# Where to save the downloaded .qdpx files
OUTPUT_DIR = "harvard_qdpx_files"


def iter_qdpx_files(per_page=100):
    """
    Iterate over all Harvard Dataverse search results
    where fileType is .qdpx.

    Uses: q="fileType:.qdpx" and type=file
    """
    start = 0

    while True:
        params = {
            "q": "fileType:.qdpx",  # this is the key change
            "type": "file",
            "per_page": per_page,
            "start": start,
        }

        if API_TOKEN:
            params["key"] = API_TOKEN

        resp = requests.get(SEARCH_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "OK":
            print("Search API returned non-OK status:", data.get("status"))
            break

        info = data.get("data", {})
        items = info.get("items", [])
        total_count = info.get("total_count", 0)

        if not items:
            break

        for item in items:
            # Dataverse query already filters by fileType:.qdpx; no extra extension check
            yield item

        start += per_page
        if start >= total_count:
            break

        # small pause to be polite
        time.sleep(0.5)


def download_qdpx_file(item, output_dir):
    """
    Download a single .qdpx file from a search result item.
    """
    file_name = item.get("name", "unknown.qdpx")
    file_id = item.get("file_id")
    download_url = item.get("url")

    if not download_url:
        if not file_id:
            print("Skipping: no download URL or file_id for item", item)
            return
        download_url = f"{BASE_URL}/api/access/datafile/{file_id}"

    # avoid name collisions
    if file_id:
        safe_name = f"{file_id}_{file_name}"
    else:
        safe_name = file_name

    dest_path = os.path.join(output_dir, safe_name)

    headers = {}
    if API_TOKEN:
        headers["X-Dataverse-key"] = API_TOKEN

    print(f"Downloading {safe_name} from {download_url} ...")

    with requests.get(download_url, headers=headers, stream=True, timeout=120) as r:
        if r.status_code == 403:
            print(f"  -> Forbidden (maybe restricted): {safe_name}")
            return
        r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"  -> Saved to {dest_path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Searching for .qdpx files on Harvard Dataverse...")
    count = 0
    for item in iter_qdpx_files(per_page=100):
        download_qdpx_file(item, OUTPUT_DIR)
        count += 1

    print(f"Done. Downloaded {count} .qdpx files.")


if __name__ == "__main__":
    main()
