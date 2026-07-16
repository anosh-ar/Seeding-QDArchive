#!/usr/bin/env python3
"""
make_project_table.py
─────────────────────
Extracts the Part-2 Results Step 4c table (project-description slide 28) from
23220843-seeding.db and writes it as an XLSX spreadsheet.

Columns:
    repository_id
    project_type      PROJECTS.type  (QDA_PROJECT / QD_PROJECT / OTHER_PROJECT / NOT_A_PROJECT)
    project_title     PROJECTS.title
    primary_class     PROJECTS.class — the 2-digit ISIC Rev. 5 division code
                      (blank for OTHER_PROJECT / NOT_A_PROJECT, which carry no class)
    no_project_files  total number of files in the project (ALL statuses, i.e. the
                      project's full file listing, including failed downloads)

Row scope:
    Every project that has a type — all remaining projects, since the unclassified
    IHSN placeholder projects have been removed from the database.

Run (from repo root):
    python src/make_project_table.py
    # → project_table.xlsx
"""

import sqlite3

import pandas as pd

DB_PATH = "23220843-seeding.db"
OUT_XLSX = "project_table.xlsx"

COLUMNS = [
    "repository_id", "project_type", "project_title",
    "primary_class", "no_project_files",
]


def build_dataframe(conn) -> pd.DataFrame:
    query = """
        SELECT
            p.repository_id                                   AS repository_id,
            p.type                                            AS project_type,
            p.title                                           AS project_title,
            p.class                                           AS primary_class,
            (SELECT COUNT(*) FROM FILES f WHERE f.project_id = p.id)
                                                              AS no_project_files
        FROM PROJECTS p
        WHERE p.type IS NOT NULL
        ORDER BY CASE WHEN p.repository_id = 18 THEN 0 ELSE 1 END,
                 p.repository_id, p.type, p.title
    """
    df = pd.read_sql_query(query, conn)
    return df[COLUMNS]


def write_xlsx(df: pd.DataFrame, path: str) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="projects")
        ws = writer.sheets["projects"]
        # Freeze the header row, add an autofilter, and set sensible widths.
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        widths = {"A": 14, "B": 16, "C": 70, "D": 13, "E": 16}
        for col, w in widths.items():
            ws.column_dimensions[col].width = w


def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    df = build_dataframe(conn)
    conn.close()

    write_xlsx(df, OUT_XLSX)

    print(f"Wrote {OUT_XLSX}  —  {len(df)} rows")
    print("Rows per repository:")
    print(df.groupby("repository_id").size().to_string())
    print("Rows per project_type:")
    print(df.groupby("project_type").size().to_string())
    print("Rows with a primary_class:", int(df["primary_class"].notna().sum()))


if __name__ == "__main__":
    main()
