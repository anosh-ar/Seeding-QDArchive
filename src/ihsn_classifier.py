#!/usr/bin/env python3
"""
ihsn_classifier.py
──────────────────
Assigns the PROJECT_TYPE of IHSN projects (repository_id = 9) in
23220843-seeding.db, using Google Gemma via the same infrastructure as
isic_classifier.py (which is imported here, never modified).

Why file-first?
    IHSN (International Household Survey Network) is a catalog of *quantitative*
    household-survey / census microdata, and every IHSN project's `description`
    is empty ("n/a"). With no project-level text, classifying a project from
    metadata alone makes no sense — the *files* are the only real signal. So we
    classify each READABLE file, then set the project's labels from its files
    (the same shape as the ISIC classifier, which propagates a file's class up
    to its project).

    Each per-file request does BOTH jobs at once:
      1. file_data_type — the nature of the file's data (Task 1, below), and
      2. an ISIC Rev. 5 division — exactly like isic_classifier.py (Task 2).
    The ISIC division is only STORED for QD projects (see below), so for IHSN
    this classifier fills FILES.class / PROJECTS.class only on QD projects, using
    the same columns and prompt the ISIC classifier uses. The ISIC classifier's
    own run (repository_id = 18) is left completely untouched — it never touches
    repository_id = 9, so there is no conflict on the shared columns.

File labels (slide-11 boundary; QDA is skipped — IHSN has no QDA files):
    PRIMARY_DATA  a readable text / paper document — per slide 11 the "input data
                  to a researcher's analysis" (interview transcripts, research
                  papers), extensions pdf/doc/docx/txt/… "anything goes". This is
                  a FILE-TYPE decision (see filetype_to_typeclass): any readable
                  document is primary data.
    OTHER_DATA    other valid data that is not a text/paper document — tabular /
                  statistical microdata (xlsx, csv, sav, …) and binaries. An
                  archive (zip/rar/7z) is opened and counts as PRIMARY_DATA if it
                  contains any readable document inside, else OTHER_DATA.
    NOT_DATA      no derivable content (a file that extracts no text is simply
                  left unlabelled, and feeds the NOT_A_PROJECT decision).

Project type = existence rule (project description slide, page 23):
    QD_PROJECT     if ANY file is PRIMARY_DATA (qualitative primary data exists)
    OTHER_PROJECT  else if ANY file is OTHER_DATA (valid data exists)
    NOT_A_PROJECT  else — only NOT_DATA labels, OR no file could be read at all
                   (all sampled files unreadable, e.g. scanned-image census PDFs)
Mapping PRIMARY_DATA→QD_PROJECT, OTHER_DATA→OTHER_PROJECT, NOT_DATA→NOT_A_PROJECT.

ISIC classification is stored ONLY for QD_PROJECTs, and only on their PRIMARY_DATA
files (page 25). Every per-file request still returns an ISIC division, but that
division is PERSISTED only when the project resolves to QD_PROJECT — on its
primary data files. It is discarded for OTHER_PROJECT / NOT_A_PROJECT and for
non-primary files. PROJECTS.class = the DOMINANT (most frequent) ISIC division
among a QD project's primary data files; ties break toward the longest-content
file (mirroring the ISIC classifier's longest-file preference).

The results land in:
    FILES.type_class          per-file label (PRIMARY_DATA / OTHER_DATA / NOT_DATA)
    FILES.class               ISIC division code — QD projects' primary files only
    FILES.content_length      chars of extracted text (used for the class tie-break)
    FILES.classified_by       model tag for FILES.class
    FILES.type_classified_by  model tag for FILES.type_class
    PROJECTS.type             resolved project type (overwrites the placeholder)
    PROJECTS.class            dominant ISIC division — QD projects only, else NULL
    PROJECTS.classified_by    model tag for PROJECTS.class
    PROJECTS.type_classified_by  model tag for PROJECTS.type (also resume marker)

Setup / run (from repo root):
    pip install -r requirements.txt          # same deps as isic_classifier
    # GOOGLE_API_KEY must be in src/.env (same file isic_classifier.py uses)
    python src/ihsn_classifier.py

Resume:
    Safe to re-run. Files with FILES.type_class already set are not re-sent, and
    projects already stamped (PROJECTS.type_classified_by NOT NULL) are skipped.
    Set REPROCESS = True to relabel everything from scratch.
"""

import json
import logging
import random
import sqlite3
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

from google.genai import types

# ── Reuse the ISIC classifier's plumbing (import, do NOT modify) ────────────────
# Importing the module builds the shared Gemma CLIENT, the per-model rate
# LIMITER, and configures logging. We only borrow the pieces we need; the ISIC
# main() is guarded by __main__ and never runs on import.
from isic_classifier import (
    CLIENT,
    LIMITER,
    STOP_EVENT,
    FILES_BASE,
    MODEL_TAGS,
    SYSTEM_INSTRUCTION as ISIC_SYSTEM_INSTRUCTION,
    migrate_db as isic_migrate_db,
    _is_daily_quota_429,
    _fmt_elapsed,
    extract_text,
    _EXTRACTORS,
)

# The ISIC division reference table (minus its trailing "Respond ONLY…" line,
# which we replace with our own combined-output instruction below).
ISIC_REFERENCE_BODY = ISIC_SYSTEM_INSTRUCTION.split("\n\nRespond ONLY")[0].rstrip()


# ── Wider text budget for IHSN (rec. #1) ──────────────────────────────────────
# IHSN survey/census PDFs open with cover pages + statistical-office / ministry
# boilerplate, which biases the first ~3K chars toward ISIC 84 (public admin) and
# hides the real subject domain (agriculture, health, education, …). We send more
# body text so the topic reaches the model. isic_classifier's extractors hard-
# truncate at its MAX_CHARS_PER_FILE via the module-level _truncate(), and there
# is no way to pass a bigger cap through extract_text — so we install a wider
# _truncate on the imported module. Python resolves the name at call time inside
# each extractor, so this takes effect for every extractor. It runs in THIS
# (IHSN) process only: it does not edit isic_classifier.py on disk and does not
# affect the separate repository_id = 18 ISIC run.
import isic_classifier as _isic  # noqa: E402  (patched below)

IHSN_MAX_CHARS = 8_000


def _wide_truncate(text: str, max_chars: int = IHSN_MAX_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[... CONTENT TRUNCATED TO FIT TOKEN BUDGET ...]"


_isic._truncate = _wide_truncate


log = logging.getLogger("ihsn_classifier")
# isic_classifier already called logging.basicConfig (console + isic log file),
# so basicConfig here is a no-op. Attach our own file handler so IHSN runs land
# in a dedicated log without polluting the ISIC one.
_fh = logging.FileHandler("ihsn_classifier.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
log.addHandler(_fh)
log.setLevel(logging.INFO)

BANNER = "═" * 60


# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH        = "23220843-seeding.db"
REPOSITORY_ID  = 9                       # IHSN catalog
MODEL          = "gemma-4-31b-it"        # same model family as the ISIC run
MODEL_TAG      = MODEL_TAGS.get(MODEL, "31b")

# Set True to relabel every IHSN project + file even if already done.
REPROCESS: bool = False

# Per-project API budget: classify at most this many READABLE files via Gemma.
# The project type follows the existence rule (page 23), so we sample a small,
# prioritized set of the files most likely to BE qualitative primary data.
MAX_FILES_PER_PROJECT = 2

# Sampling priority: prefer pdf / doc / docx (where qualitative primary data —
# transcripts, research articles — would live), then other prose, then tabular,
# then archives. Lower number = classified first. Non-extractable types sort last
# and are skipped by the extractor guard anyway.
FILE_TYPE_PRIORITY = {
    "pdf": 0, "doc": 0, "docx": 0,
    "txt": 1, "rtf": 1,
    "html": 2, "htm": 2,
    "xlsx": 3, "xls": 3, "csv": 3, "tab": 3,
    "zip": 4, "rar": 4, "7z": 4,
}

# Mapping from file label to the project-type it implies, plus the existence
# priority order: the resolved type is the HIGHEST-priority project type that has
# at least one file (any PRIMARY_DATA → QD_PROJECT, else any OTHER_DATA → OTHER…).
FILE_TO_PROJECT = {
    "PRIMARY_DATA": "QD_PROJECT",
    "OTHER_DATA":   "OTHER_PROJECT",
    "NOT_DATA":     "NOT_A_PROJECT",
}
PRIORITY_ORDER = ["QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"]
PROJECT_TYPES = tuple(PRIORITY_ORDER)

# ── Slide-11 primary-data rule ────────────────────────────────────────────────
# Per project-description slide 11, a "primary data file" is the INPUT to a
# researcher's analysis — interview transcripts or research papers — extensions
# "pdf, doc, docx, txt, jpg, etc. (anything goes)". Operationally: any readable
# text/paper document is PRIMARY_DATA; other valid data (tabular, statistical,
# binary, archives) is OTHER_DATA. This makes the data-nature label a function of
# the file type (no LLM judgement needed); the LLM is used only for ISIC.
PRIMARY_FILE_TYPES = frozenset({
    "pdf", "doc", "docx", "txt", "rtf", "odt", "html", "htm", "ppt", "pptx",
})


def filetype_to_typeclass(file_type: str) -> str:
    """Slide-11 mapping: text/paper document → PRIMARY_DATA, else OTHER_DATA."""
    return "PRIMARY_DATA" if (file_type or "").lower() in PRIMARY_FILE_TYPES else "OTHER_DATA"


# Archive containers are opened and judged by their contents: one primary-type
# document inside is enough to mark the whole archive PRIMARY_DATA.
ARCHIVE_EXTS = frozenset({"zip", "rar", "7z"})


def _archive_member_names(path: Path) -> list[str]:
    """Inner file names of a zip / 7z / rar archive (directories excluded)."""
    ext = path.suffix.lstrip(".").lower()
    if ext == "zip":
        with zipfile.ZipFile(str(path)) as zf:
            return [m.filename for m in zf.infolist() if not m.is_dir()]
    if ext == "7z":
        import py7zr  # type: ignore
        with py7zr.SevenZipFile(str(path), "r") as sz:
            return [n for n in sz.getnames() if not n.endswith("/")]
    if ext == "rar":
        import rarfile  # type: ignore
        with rarfile.RarFile(str(path)) as rf:
            return [m.filename for m in rf.infolist() if not m.is_dir()]
    return []


def archive_has_primary(path: Path) -> bool:
    """
    True if the archive contains at least one primary-type document (slide-11).
    One qualifying inner file is enough to classify the whole archive as primary.
    A broken / unreadable archive returns False (→ OTHER_DATA).
    """
    try:
        names = _archive_member_names(path)
    except Exception as exc:
        log.warning("  Could not read archive %s — %s", path.name, exc)
        return False
    return any(Path(n).suffix.lstrip(".").lower() in PRIMARY_FILE_TYPES for n in names)


def label_for_file(file_type: str, path: Path) -> str:
    """
    Slide-11 data-nature label for a file. Plain files map by extension; archive
    containers (zip/rar/7z) are opened and count as PRIMARY_DATA if they hold any
    primary-type document inside, else OTHER_DATA.
    """
    if (file_type or "").lower() in ARCHIVE_EXTS:
        return "PRIMARY_DATA" if archive_has_primary(path) else "OTHER_DATA"
    return filetype_to_typeclass(file_type)


# ── Output schemas ────────────────────────────────────────────────────────────

class IHSNFileClassification(BaseModel):
    """Combined per-file result: data-nature label + ISIC division, one request."""
    file_data_type: Literal["PRIMARY_DATA", "OTHER_DATA", "NOT_DATA"] = Field(
        description="Nature of the data in THIS file (Task 1)"
    )
    isic_division_code: str = Field(
        description="2-digit ISIC Rev. 5 division code, e.g. '01', '62', '85' (Task 2)"
    )
    division_name: str = Field(description="Official ISIC Rev. 5 division name")
    confidence_score: float = Field(
        ge=0.0, le=1.0, description="Confidence in the ISIC division"
    )
    justification: str = Field(description="One or two sentences")


# ── System instructions (strict-qualitative boundary) ─────────────────────────

FILE_SYSTEM_INSTRUCTION = """You perform TWO classifications on a SINGLE file \
from a research data project in the IHSN (International Household Survey Network) \
catalog, and return BOTH in one JSON object.

═══ TASK 1 — file_data_type: the NATURE of the data in this file ═══
Choose exactly one:
- PRIMARY_DATA  The file itself holds qualitative primary data — a verbatim
                interview or focus-group transcript, open-ended narrative survey
                responses, field notes, or a qualitative study / research article
                that presents and interprets such material.
- OTHER_DATA    The file is valid data or documentation, but NOT qualitative
                primary data — e.g. quantitative survey microdata, census tables,
                numeric datasets, questionnaires, codebooks, data dictionaries,
                sampling designs, or statistical / analytical reports.
- NOT_DATA      The file has no derivable data content — empty, unreadable
                garble, or pure boilerplate.
STRICT RULE: Questionnaires, enumeration forms, codebooks, data dictionaries,
  sampling reports and statistical / analytical reports are OTHER_DATA, NOT
  PRIMARY_DATA, even though they are readable prose. Numeric survey / census
  microdata is OTHER_DATA. Only choose PRIMARY_DATA when the file contains actual
  qualitative material (verbatim transcripts, open-ended narrative text, or an
  explicitly qualitative study). When unsure between PRIMARY_DATA and OTHER_DATA,
  choose OTHER_DATA.

═══ TASK 2 — ISIC Rev. 5 division: isic_division_code + division_name ═══
Classify the SAME file by its SUBJECT MATTER into exactly one ISIC Rev. 5
division, using the reference below.

""" + ISIC_REFERENCE_BODY + """

IHSN survey/census guidance for Task 2:
  Classify by the survey's SUBJECT DOMAIN, NOT by the agency that produced it.
  A national statistical office, ministry, census bureau, confidentiality notice,
  or enumerator instructions (common on the cover pages) do NOT by themselves
  imply division 84. Choose 84 (public administration) ONLY when the subject is
  genuinely public administration / public finance / social security, or a
  general multi-purpose population & housing census with no more specific domain.
  Otherwise use the topical division, e.g.: agriculture / crops / livestock → 01;
  health / nutrition / mortality / fertility → 86; education / schooling → 85;
  labour force / employment → 78; and similarly map income & expenditure, poverty,
  living standards, or a sector-specific survey to its most specific division.

═══ OUTPUT ═══
Return ONE JSON object with fields: file_data_type (Task 1); isic_division_code
and division_name (Task 2); confidence_score (0.0-1.0, for the ISIC division);
justification (one or two sentences). No markdown fences, no extra text."""


# ── Database helpers ──────────────────────────────────────────────────────────

def migrate_db(conn: sqlite3.Connection) -> None:
    """
    Ensure every column this classifier writes exists. Idempotent.

    The ISIC columns (FILES.class / content_length / classified_by,
    PROJECTS.class / classified_by) are created by reusing the ISIC classifier's
    own migration so the schema stays identical. Then we add the type columns:
      PROJECTS.type                 (created by label_project_types.py; created
                                     here too if that script never ran)
      PROJECTS.type_classified_by   resume marker for finalized projects
      FILES.type_class              per-file label (PRIMARY_DATA/OTHER_DATA/NOT_DATA)
      FILES.type_classified_by      model tag that produced the file label
    """
    isic_migrate_db(conn)   # FILES.class/content_length/classified_by, PROJECTS.class/classified_by

    cur = conn.cursor()
    proj_cols = {row[1] for row in cur.execute("PRAGMA table_info(PROJECTS)")}
    if "type" not in proj_cols:
        cur.execute("ALTER TABLE PROJECTS ADD COLUMN type TEXT")
        log.info("DB migration: 'type' column added to PROJECTS.")
    if "type_classified_by" not in proj_cols:
        cur.execute("ALTER TABLE PROJECTS ADD COLUMN type_classified_by TEXT")
        log.info("DB migration: 'type_classified_by' column added to PROJECTS.")

    files_cols = {row[1] for row in cur.execute("PRAGMA table_info(FILES)")}
    if "type_class" not in files_cols:
        cur.execute("ALTER TABLE FILES ADD COLUMN type_class TEXT")
        log.info("DB migration: 'type_class' column added to FILES.")
    if "type_classified_by" not in files_cols:
        cur.execute("ALTER TABLE FILES ADD COLUMN type_classified_by TEXT")
        log.info("DB migration: 'type_classified_by' column added to FILES.")

    conn.commit()


def get_ihsn_projects(conn: sqlite3.Connection) -> list[tuple]:
    """
    (id, title, download_repository_folder, download_project_folder) for IHSN
    projects still needing a finalized type. When REPROCESS is False, projects
    already stamped (type_classified_by NOT NULL) are skipped. Ordered by id.
    """
    clause = "" if REPROCESS else " AND p.type_classified_by IS NULL"
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT p.id, p.title,
               p.download_repository_folder, p.download_project_folder
        FROM   PROJECTS p
        WHERE  p.repository_id = ?{clause}
        ORDER  BY p.id
        """,
        (REPOSITORY_ID,),
    )
    return cur.fetchall()


def get_all_succeeded_files(conn: sqlite3.Connection, project_id: int) -> list[tuple]:
    """
    (id, file_name, file_type, type_class, class, content_length) for every
    SUCCEEDED file in the project, id order.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, file_name, file_type, type_class, class, content_length
        FROM   FILES
        WHERE  project_id = ? AND status = 'SUCCEEDED'
        ORDER  BY id
        """,
        (project_id,),
    )
    return cur.fetchall()


def save_file_classification(
    conn: sqlite3.Connection,
    file_id: int,
    isic_code: str,
    file_data_type: str,
    content_length: int,
    model_tag: str,
) -> None:
    """Persist both the ISIC class and the data-nature label for one file."""
    conn.execute(
        "UPDATE FILES "
        "SET class = ?, content_length = ?, classified_by = ?, "
        "    type_class = ?, type_classified_by = ? "
        "WHERE id = ?",
        (isic_code, content_length, model_tag, file_data_type, model_tag, file_id),
    )
    conn.commit()


def save_project_type(
    conn: sqlite3.Connection, project_id: int, project_type: str, model_tag: str,
) -> None:
    conn.execute(
        "UPDATE PROJECTS SET type = ?, type_classified_by = ? WHERE id = ?",
        (project_type, model_tag, project_id),
    )
    conn.commit()


def save_project_class(
    conn: sqlite3.Connection, project_id: int, isic_code: str, model_tag: str,
) -> None:
    conn.execute(
        "UPDATE PROJECTS SET class = ?, classified_by = ? WHERE id = ?",
        (isic_code, model_tag, project_id),
    )
    conn.commit()


# ── Existence-rule aggregation (page 23) ──────────────────────────────────────

def resolve_project_type(file_labels: list[str]) -> Optional[str]:
    """
    Existence rule: return the highest-priority project type that has at least
    one file. Any PRIMARY_DATA → QD_PROJECT; else any OTHER_DATA → OTHER_PROJECT;
    else NOT_A_PROJECT. Returns None if there are no labels at all.
    """
    votes = {FILE_TO_PROJECT[lbl] for lbl in file_labels if lbl in FILE_TO_PROJECT}
    if not votes:
        return None
    for ptype in PRIORITY_ORDER:
        if ptype in votes:
            return ptype
    return None


def dominant_isic_class(file_rows: list[tuple]) -> Optional[str]:
    """
    Most frequent ISIC division across the project's classified files.
    `file_rows` are (id, name, type, type_class, class, content_length) tuples.
    Ties are broken toward the class of the longest-content file (mirroring the
    ISIC classifier's longest-file preference). Returns None if no file has a class.
    """
    classified = [(cls, clen or 0) for *_, cls, clen in file_rows if cls is not None]
    if not classified:
        return None
    counts = Counter(cls for cls, _ in classified)
    top = max(counts.values())
    tied = {cls for cls, n in counts.items() if n == top}
    if len(tied) == 1:
        return next(iter(tied))
    # Tie-break: among the tied classes, take the one on the longest-content file.
    best, best_len = None, -1
    for cls, clen in classified:
        if cls in tied and clen > best_len:
            best, best_len = cls, clen
    return best


# ── Classifier calls ──────────────────────────────────────────────────────────

def _parse(response, schema) -> dict:
    parsed = getattr(response, "parsed", None)
    if parsed is not None:
        return parsed.model_dump()
    raw = (response.text or "").strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    result, _ = json.JSONDecoder().raw_decode(raw)
    schema(**result)   # raises on bad shape
    return result


def _generate(user_content: str, system_instruction: str, schema) -> Optional[dict]:
    """
    One Gemma call with the shared limiter + 3-attempt retry. Returns a validated
    dict, or None on exhaustion / daily-quota trip / interrupt.
    """
    attempts = 3
    for attempt in range(1, attempts + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            LIMITER.wait(MODEL)
            if STOP_EVENT.is_set():
                return None
            response = CLIENT.models.generate_content(
                model=MODEL,
                contents=user_content,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                    response_schema=schema,
                    temperature=0.1,
                    max_output_tokens=512,
                ),
            )
            usage = getattr(response, "usage_metadata", None)
            prompt_tokens = getattr(usage, "prompt_token_count", None) if usage else None
            LIMITER.record(MODEL, prompt_tokens)
            return _parse(response, schema)
        except Exception as exc:
            if _is_daily_quota_429(exc):
                LIMITER.trip_daily(MODEL)
                return None
            jitter = random.uniform(2.0, 5.0) * attempt
            log.warning("[%s]   ↻ retry %d/%d  %s  (sleeping %.1fs)",
                        MODEL_TAG, attempt, attempts, exc, jitter)
            if STOP_EVENT.wait(jitter):
                return None
    log.error("[%s]   ✗ exhausted %d attempts", MODEL_TAG, attempts)
    return None


def classify_file(text: str, file_name: str, title: str) -> Optional[dict]:
    user_content = (
        f"Project title: {title or '(none)'}\n"
        f"File name: {file_name}\n\n"
        f"--- File content ---\n{text}"
    )
    return _generate(user_content, FILE_SYSTEM_INSTRUCTION, IHSNFileClassification)


def clear_file_classes(
    conn: sqlite3.Connection, project_id: int, keep_primary: bool,
) -> None:
    """
    Drop the ISIC class from this project's files (it is stored only for QD
    projects' primary data files). With keep_primary=True, PRIMARY_DATA files
    keep their class and only non-primary files are cleared; otherwise every
    file's class is cleared. type_class / type_classified_by are left intact.
    """
    if keep_primary:
        conn.execute(
            "UPDATE FILES SET class = NULL, classified_by = NULL "
            "WHERE project_id = ? AND status = 'SUCCEEDED' "
            "  AND (type_class IS NULL OR type_class != 'PRIMARY_DATA')",
            (project_id,),
        )
    else:
        conn.execute(
            "UPDATE FILES SET class = NULL, classified_by = NULL "
            "WHERE project_id = ? AND status = 'SUCCEEDED'",
            (project_id,),
        )
    conn.commit()


def clear_project_class(conn: sqlite3.Connection, project_id: int) -> None:
    conn.execute(
        "UPDATE PROJECTS SET class = NULL, classified_by = NULL WHERE id = ?",
        (project_id,),
    )
    conn.commit()


# ── Per-project processing ────────────────────────────────────────────────────

def process_project(
    conn: sqlite3.Connection,
    project_id: int,
    title: str,
    repo_folder: str,
    proj_folder: str,
    stats: dict,
) -> str:
    """
    Classify a project's readable files (up to the cap, pdf/doc/docx first) — each
    file gets both a data-nature label and an ISIC division in one request — then
    finalize the project's type (existence rule) and class (dominant). Returns a
    short status string.
    """
    files = get_all_succeeded_files(conn, project_id)

    # No files at all → NOT_A_PROJECT, no API call. Leave class NULL.
    if not files:
        save_project_type(conn, project_id, "NOT_A_PROJECT", MODEL_TAG)
        return "NOT_A_PROJECT (no files)"

    # A file is "done" (resume) when its type_class is set — the combined request
    # writes class + type_class together, so type_class NOT NULL implies both.
    already_done = sum(1 for r in files if r[3] is not None)
    todo = [(r[0], r[1], r[2]) for r in files if r[3] is None]
    budget = MAX_FILES_PER_PROJECT - already_done

    # Prioritize pdf/doc/docx first (see FILE_TYPE_PRIORITY), then id order.
    todo.sort(key=lambda r: (FILE_TYPE_PRIORITY.get((r[2] or "").lower(), 99), r[0]))

    for file_id, file_name, file_type in todo:
        if STOP_EVENT.is_set() or budget <= 0:
            break
        ftype = (file_type or "").lower()
        if ftype not in _EXTRACTORS:
            continue  # not a readable type — skip (leave unlabelled)
        path = FILES_BASE / (repo_folder or "") / (proj_folder or "") / file_name
        if not path.is_file():
            continue
        text = extract_text(ftype, path)
        if not text:
            continue  # scanned / empty — if the whole project is like this it
                      # resolves to NOT_A_PROJECT below

        result = classify_file(text, file_name, title)
        if result is None:
            stats["file_fail"] += 1
            if STOP_EVENT.is_set() or LIMITER._tripped_until.get(MODEL):
                break
            continue
        # Data-nature label follows slide 11 (file-type rule; archives judged by
        # their contents), not the model's own file_data_type field, which is now
        # vestigial. ISIC still comes from the model. A file only reaches here if
        # it extracted text, so scanned / empty files never get labelled (they
        # feed the NOT_A_PROJECT path).
        label = label_for_file(file_type, path)
        isic = result["isic_division_code"]
        save_file_classification(conn, file_id, isic, label, len(text), MODEL_TAG)
        stats["file_ok"] += 1
        budget -= 1
        log.info("[%s]     · file=%-6d %-5s → %-12s ISIC=%-3s  conf=%.2f  %s",
                 MODEL_TAG, file_id, ftype, label, isic,
                 result["confidence_score"], file_name)

    if STOP_EVENT.is_set() or LIMITER._tripped_until.get(MODEL):
        return "stopped"

    # Re-read files (existing + just-saved) and resolve the project.
    rows = get_all_succeeded_files(conn, project_id)
    labels = [r[3] for r in rows if r[3] is not None]

    # No file could be read (all sampled files unreadable) → NOT_A_PROJECT.
    if not labels:
        save_project_type(conn, project_id, "NOT_A_PROJECT", MODEL_TAG)
        clear_project_class(conn, project_id)
        return "NOT_A_PROJECT  (all sampled files unreadable)"

    ptype = resolve_project_type(labels)
    save_project_type(conn, project_id, ptype, MODEL_TAG)

    if ptype == "QD_PROJECT":
        # ISIC is stored ONLY here, and ONLY on the primary data files (p25).
        clear_file_classes(conn, project_id, keep_primary=True)
        primary_rows = [r for r in rows if r[3] == "PRIMARY_DATA"]
        dom = dominant_isic_class(primary_rows)
        if dom is not None:
            save_project_class(conn, project_id, dom, MODEL_TAG)
        return f"{ptype}  ISIC={dom}  (from {len(labels)} file(s))"

    # OTHER_PROJECT / NOT_A_PROJECT: no ISIC — drop any division we computed.
    clear_file_classes(conn, project_id, keep_primary=False)
    clear_project_class(conn, project_id)
    return f"{ptype}  (no ISIC; from {len(labels)} file(s))"


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    start_ts = datetime.now(timezone.utc)
    log.info(BANNER)
    log.info("  IHSN Project-Type Classifier  (repository_id = %d)", REPOSITORY_ID)
    log.info("  Started  : %s", start_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("  Model    : %-4s →  %s", MODEL_TAG, MODEL)
    log.info("  Strategy : file-first  ·  type (existence, p23) + ISIC (dominant)")
    log.info("  Per file : one request → file_data_type + ISIC division")
    log.info("  Cap      : up to %d files/project via API  (pdf/doc/docx first)",
             MAX_FILES_PER_PROJECT)
    log.info("  Text     : up to %d chars/file  ·  subject-domain ISIC guidance",
             IHSN_MAX_CHARS)
    log.info("  Reprocess: %s", REPROCESS)
    log.info(BANNER)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    migrate_db(conn)

    projects = get_ihsn_projects(conn)
    total = len(projects)
    log.info("  Projects : %d to classify", total)
    log.info(BANNER)

    counts = Counter()
    stats = {"file_ok": 0, "file_fail": 0}

    try:
        for idx, (project_id, title, repo_folder, proj_folder) in enumerate(projects, 1):
            if STOP_EVENT.is_set() or LIMITER._tripped_until.get(MODEL):
                break

            short = (title or "").strip().replace("\n", " ")
            if len(short) > 50:
                short = short[:49] + "…"
            log.info("[%s] ▶ %4d/%d  id=%-6d  %s", MODEL_TAG, idx, total, project_id, short)

            status = process_project(conn, project_id, title, repo_folder, proj_folder, stats)
            log.info("[%s]   ✓ %s", MODEL_TAG, status)
            counts[status.split()[0]] += 1

            if status == "stopped":
                log.warning("[%s]   ⏸ stopping (interrupt / daily quota)", MODEL_TAG)
                break

    except KeyboardInterrupt:
        STOP_EVENT.set()
        log.warning("\n  Ctrl+C — stopping after current project.")

    conn.close()

    elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
    log.info(BANNER)
    log.info("  Completed")
    log.info("  Elapsed  : %s", _fmt_elapsed(elapsed))
    log.info("  Projects : QD=%d  ·  OTHER=%d  ·  NOT_A=%d",
             counts.get("QD_PROJECT", 0), counts.get("OTHER_PROJECT", 0),
             counts.get("NOT_A_PROJECT", 0))
    log.info("  Files    : %d labelled  ·  %d failed", stats["file_ok"], stats["file_fail"])
    log.info("  ISIC     : stored on QD projects only (primary data files)")
    log.info(BANNER)


if __name__ == "__main__":
    main()
