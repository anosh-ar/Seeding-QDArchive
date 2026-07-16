"""
Adds a 'type' column to the PROJECTS table in 23220843-seeding.db and labels each project:
  - repository_id=18: QDA_PROJECT if the download folder contains a QDA file extension,
                      QD_PROJECT otherwise.
  - repository_id=9:  OTHER_PROJECT unconditionally.

QDA extensions are read from src/QDA_files_extensions.csv.
Project folders are resolved as Downloaded_Files/<download_repository_folder>/<download_project_folder>.
"""

import sqlite3
import csv
import os

DB_PATH = "23220843-seeding.db"
EXTENSIONS_CSV = os.path.join("src", "QDA_files_extensions.csv")
DOWNLOADED_FILES_BASE = "Downloaded_Files"


def load_qda_extensions(csv_path: str) -> set[str]:
    extensions = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            for cell in row:
                cell = cell.strip()
                if cell.startswith("."):
                    extensions.add(cell.lower())
    return extensions


def folder_has_qda_extension(folder_path: str, qda_extensions: set[str]) -> bool:
    for root, _dirs, files in os.walk(folder_path):
        for filename in files:
            _, ext = os.path.splitext(filename)
            if ext.lower() in qda_extensions:
                return True
    return False


def main():
    qda_extensions = load_qda_extensions(EXTENSIONS_CSV)
    print(f"Loaded {len(qda_extensions)} QDA extensions: {sorted(qda_extensions)}")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    # Add 'type' column if it doesn't exist yet
    existing_columns = {row[1] for row in cur.execute("PRAGMA table_info(PROJECTS)")}
    if "type" not in existing_columns:
        cur.execute("ALTER TABLE PROJECTS ADD COLUMN type TEXT")
        print("Added 'type' column to PROJECTS.")
    else:
        print("'type' column already exists, updating values.")

    # Label repository_id=9 projects
    cur.execute("UPDATE PROJECTS SET type = 'OTHER_PROJECT' WHERE repository_id = 9")
    print(f"Labelled {cur.rowcount} repository_id=9 projects as OTHER_PROJECT.")

    # Label repository_id=18 projects
    cur.execute(
        "SELECT id, download_repository_folder, download_project_folder "
        "FROM PROJECTS WHERE repository_id = 18"
    )
    rows = cur.fetchall()

    qda_count = 0
    qd_count = 0
    missing_count = 0

    for project_id, repo_folder, project_folder in rows:
        folder_path = os.path.join(DOWNLOADED_FILES_BASE, repo_folder, project_folder)

        if not os.path.isdir(folder_path):
            label = "QD_PROJECT"
            missing_count += 1
        elif folder_has_qda_extension(folder_path, qda_extensions):
            label = "QDA_PROJECT"
            qda_count += 1
        else:
            label = "QD_PROJECT"
            qd_count += 1

        cur.execute("UPDATE PROJECTS SET type = ? WHERE id = ?", (label, project_id))

    conn.commit()
    conn.close()

    print(f"\nrepository_id=18 results:")
    print(f"  QDA_PROJECT : {qda_count}")
    print(f"  QD_PROJECT  : {qd_count} (including {missing_count} with missing folders)")
    print("Done.")


if __name__ == "__main__":
    main()
