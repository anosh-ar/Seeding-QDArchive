#!/usr/bin/env python3
"""
isic_classifier.py
──────────────────
Classifies research data files stored in 23220843-seeding.db into
UN ISIC Rev. 5 Division-level categories (2-digit codes) using Google
Gemma 4 (31B) via the Google AI Studio API (google-genai SDK).

Text is extracted from each file (PDF / DOCX / XLSX / TAB / CSV / TXT),
truncated to stay within Gemma's 16K-input-tokens-per-minute paid-tier
cap, then sent for structured JSON classification. Results are written
to FILES.class and propagated up to PROJECTS.class.

Setup:
    pip install -r requirements.txt

    Put your Google AI Studio key in src/.env (same file Scraper.py uses):
        GOOGLE_API_KEY=your_key_here

    (src/.env is git-ignored, so the key never leaves your machine.)

    Alternatively, set the environment variable directly:
        Windows : set GOOGLE_API_KEY=your_key_here
        Linux   : export GOOGLE_API_KEY=your_key_here

Run (from repo root):
    python src/isic_classifier.py

Resume:
    Re-running is fully safe — rows where FILES.class IS NOT NULL are skipped.
"""

import json
import logging
import os
import queue
import random
import re
import sqlite3
import tempfile
import threading
import time
import zipfile
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pdfplumber
import pypdf
import docx as python_docx
import openpyxl
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from google import genai
from google.genai import types

# Load GOOGLE_API_KEY (and any other vars) from src/.env — the same file
# Scraper.py uses. Explicit path so it works regardless of the cwd from
# which this script is launched.
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("isic_classifier.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Fixed-width separator used in startup and summary banners.
BANNER = "═" * 60


def _fmt_elapsed(seconds: float) -> str:
    """Format seconds as 'Xd HH:MM:SS' / 'HH:MM:SS' / 'MM:SS' for readable logs."""
    s = int(seconds)
    d, s = divmod(s, 86_400)
    h, s = divmod(s, 3_600)
    m, s = divmod(s, 60)
    if d:
        return f"{d}d {h:02d}:{m:02d}:{s:02d}"
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


# ── Graceful shutdown signal ──────────────────────────────────────────────────

# Set by main() when it catches a KeyboardInterrupt. Every long sleep in the
# limiter and the retry loops uses STOP_EVENT.wait(timeout) instead of
# time.sleep() so a single Ctrl+C can unblock them within ~½ second. Workers
# also peek at this flag between projects and files so they exit cleanly.
STOP_EVENT = threading.Event()


# ── Quota / clock helpers ─────────────────────────────────────────────────────

_PT = ZoneInfo("America/Los_Angeles")


def _seconds_until_next_pt_midnight(buffer: float = 60.0) -> float:
    """
    Seconds from now until the next 00:00 America/Los_Angeles boundary.
    Google's daily quotas reset at midnight PT, so this is the proper wake-up
    time after a daily-cap exhaustion. A small buffer (default 60s) is added
    so the first call after wake-up lands strictly inside the new quota day.
    """
    now_pt = datetime.now(_PT)
    next_midnight_pt = (now_pt + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    return (next_midnight_pt - now_pt).total_seconds() + buffer


def _is_daily_quota_429(exc: BaseException) -> bool:
    """
    True if `exc` looks like a *daily*-quota exhaustion 429 from the Gemini /
    AI Studio API. Per-minute (TPM/RPM) 429s are recoverable inside one
    session and should NOT trip the daily circuit; daily-cap 429s should.

    The daily-quota error string carries either the "PerDay" marker in the
    quotaId (e.g. GenerateRequestsPerDayPerProjectPerModel-FreeTier) or the
    "free_tier_requests" metric name. Per-minute quotas use "PerMinute".
    """
    s = str(exc)
    if "RESOURCE_EXHAUSTED" not in s and "429" not in s:
        return False
    return "PerDay" in s or "free_tier_requests" in s


# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH         = "23220843-seeding.db"
FILES_BASE      = Path("Downloaded_Files")

# Temporary scope: only classify projects from this repository_id. Set to
# None to process every repository. Remove once the targeted repo is done.
REPOSITORY_ID_FILTER: Optional[int] = 18

# Earliest project_id to include in the queue. Lets the run skip the bulk
# of already-classified projects from earlier sessions while still picking
# up the boundary one (so any straggler files left behind by errors get a
# retry). Set to None to include the full id range.
START_FROM_PROJECT_ID: Optional[int] = 212

PRIMARY_MODEL   = "gemma-4-31b-it"

# Worker models — each entry spawns a worker thread that pulls projects from
# the shared queue. The architecture supports N parallel models; currently
# we run only 31B (26B produced lower-quality JSON output and was dropped).
# Add another model name back to this list to re-enable parallel workers.
WORKER_MODELS = [PRIMARY_MODEL]

# Short, stable tags persisted into FILES.classified_by / PROJECTS column for
# later auditing. Keep these in sync with WORKER_MODELS.
MODEL_TAGS = {
    PRIMARY_MODEL:  "31b",
}

# Google AI Studio paid-tier quotas for the Gemma 4 models we use:
#   TPM  = 16,000 input tokens/minute       (per-minute token cap)
#   RPM  = 15  requests/minute              (per-minute request cap)
#   RPD  = 1,500 requests/calendar-day      (the binding constraint for us)
# All three buckets are PER MODEL, so each worker has its own quota and the
# two workers do not contend with each other.
TPM_LIMIT       = 16_000
RPM_HARD_CAP    = 15
RPD_CAP         = 1_500
# Per-session safety stop — a fallback that bounds total work done in one
# process invocation. With RPD now enforced in the limiter, the script will
# already sleep through quota exhaustion automatically; this only fires if
# something runs away unexpectedly across multiple daily quota refreshes.
RPD_SAFETY_STOP = 20_000

# Per-request text budget — sized to fit comfortably under the TPM cap so
# the rolling-window limiter can sustain ~5 RPM steady-state per model and
# burst higher when actual usage runs below worst case.
#   3 K chars ≈ 750 tokens (at ~4 chars/token)
#   ~1.53K system prompt + ~70 boilerplate + ~625 description + 750 content
#   ≈ 2.97K tokens / request  →  16K / 2.97K  ≈  5 RPM steady-state.
MAX_CHARS_PER_FILE = 3_000

# Conservative pre-call estimate used by the rate limiter to decide whether
# a new request fits in the rolling TPM window before its actual token count
# is known. Updated to actuals after each successful call via usage_metadata.
TOKEN_BUDGET_ESTIMATE = 3_000

# Maximum number of files per project that may be sent to the API for
# individual classification. Once this cap is reached, any further
# extractable files in the same project are labelled with the project's
# class (inherited, no API call). Files that cannot be extracted at all
# remain untouched (FILES.class stays NULL).
MAX_API_CLASSIFICATIONS_PER_PROJECT = 10

# Base document types — extracted directly with type-specific extractors
BASE_DOC_TYPES = frozenset({
    "pdf", "docx", "doc", "xlsx", "xls", "tab", "csv", "txt", "html", "htm",
})

# Generic archives — opened by the appropriate library, then every supported
# inner document is extracted and the bundle is classified as ONE combined
# document (same pattern as QDA archives below).
GENERIC_ARCHIVE_TYPES = frozenset({"zip", "7z", "rar"})

# QDA project archives — opened as ZIPs, then each supported inner document is
# extracted and the bundle is classified as ONE combined document. Only formats
# that are genuine ZIP containers are listed here; legacy / proprietary-binary /
# SQLite-based QDA formats (MAXQDA pre-2018, NVivo, old ATLAS.ti, QDA Miner,
# f4analyse, Quirkos) are NOT extractable this way and are intentionally omitted.
QDA_ARCHIVE_TYPES = frozenset({
    "qda",            # generic alias
    "qdpx",           # REFI-QDA exchange standard
    "mqda", "mqtc",   # MAXQDA 2024 (project / teamwork)
    "mx24", "mc24",   # MAXQDA 2024
    "mx22",           # MAXQDA 2022
    "mx20",           # MAXQDA 2020
    "mx18",           # MAXQDA 2018 (first ZIP-container MAXQDA release)
    "atlproj",        # ATLAS.ti 9+
    "atlprojbundle",  # ATLAS.ti project bundle
})

# Union of every archive container — used as the recursion guard so a zip
# inside a zip (or any other nesting) is not re-entered.
ARCHIVE_TYPES = GENERIC_ARCHIVE_TYPES | QDA_ARCHIVE_TYPES

# File types we know how to extract text from
CLASSIFIABLE_TYPES = BASE_DOC_TYPES | GENERIC_ARCHIVE_TYPES | QDA_ARCHIVE_TYPES

# Maximum rows read from tabular files to protect the token budget
MAX_TABLE_ROWS = 100


# ── Pydantic Output Schema ────────────────────────────────────────────────────

class ISICClassification(BaseModel):
    isic_division_code: str = Field(
        description="2-digit ISIC Rev. 5 division code, e.g. '01', '62', '85'"
    )
    division_name: str = Field(
        description="Official name of the ISIC Rev. 5 division"
    )
    confidence_score: float = Field(
        ge=0.0,
        le=1.0,
        description="Classification confidence between 0.0 and 1.0",
    )
    justification: str = Field(
        description="One or two sentences explaining the industry mapping"
    )


# ── System Instruction ────────────────────────────────────────────────────────

# The Gemma system prompt — the full ISIC Rev. 5 division reference plus the
# classification rule — lives in a plain-text file so it can be read and edited
# without touching code. The IHSN classifier imports SYSTEM_INSTRUCTION from
# here and reuses the same reference.
_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "isic_system_instruction.txt"
SYSTEM_INSTRUCTION = _PROMPT_PATH.read_text(encoding="utf-8").strip()


# ── Google Gen AI Client ──────────────────────────────────────────────────────

def _build_client() -> genai.Client:
    api_key = os.environ.get("GOOGLE_API_KEY", "").strip()
    if not api_key or api_key == "paste_your_google_ai_studio_key_here":
        raise EnvironmentError(
            "GOOGLE_API_KEY is not set.\n"
            "  Option 1 (recommended) — put it in src/.env:\n"
            "      GOOGLE_API_KEY=your_key_here\n"
            "  Option 2 — export it in your shell:\n"
            "      Windows : set GOOGLE_API_KEY=your_key_here\n"
            "      Linux   : export GOOGLE_API_KEY=your_key_here"
        )
    # SDK-level retry with exponential backoff for transient errors.
    # Field names match google-genai's current HttpRetryOptions schema
    # (exp_base / max_delay / http_status_codes).
    retry_opts = types.HttpRetryOptions(
        attempts=6,
        initial_delay=2.0,
        exp_base=2.0,
        max_delay=64.0,
        http_status_codes=[429, 500, 502, 503, 504],
    )
    http_opts = types.HttpOptions(timeout=120_000, retry_options=retry_opts)
    return genai.Client(api_key=api_key, http_options=http_opts)


# Initialised once at module load; all calls share this client.
CLIENT: genai.Client = _build_client()


# ── Per-model Rate Limiter ────────────────────────────────────────────────────

class PerModelLimiter:
    """
    Adaptive per-model rate limiter that enforces THREE quotas:
      (a) RPM hard cap (RPM_HARD_CAP, 15)         — minute-level request gap
      (b) TPM rolling budget (TPM_LIMIT, 16,000)  — rolling 60-second tokens
      (c) RPD rolling budget (RPD_CAP, 1,500)     — rolling 24-hour requests

    TPM uses ACTUAL prompt_token_count values reported by the SDK after each
    successful call (see `record`). RPD just counts requests in a 24h window.

    The Google API does NOT expose remaining quota in response headers or
    SDK metadata for any of these — we maintain the windows locally. If the
    process is restarted mid-day, the local RPD counter resets to 0 while
    the server-side quota does not; in that case the SDK's 429 retry layer
    handles the eventual quota errors.

    All quotas are PER MODEL on Google's side; state is keyed by model so
    the two workers (pinned 1:1 to a model) operate independently.
    """

    def __init__(
        self,
        tpm_limit: int = TPM_LIMIT,
        rpm_hard_cap: int = RPM_HARD_CAP,
        rpd_cap: int = RPD_CAP,
        estimate: int = TOKEN_BUDGET_ESTIMATE,
    ) -> None:
        self._tpm = tpm_limit
        self._rpm = rpm_hard_cap
        self._rpd = rpd_cap
        self._est = estimate
        self._min_gap = 60.0 / rpm_hard_cap   # seconds between calls
        self._locks: dict[str, threading.Lock] = {}
        self._history: dict[str, deque] = defaultdict(deque)   # 60s TPM window
        self._daily:   dict[str, deque] = defaultdict(deque)   # 24h RPD window
        self._last_call: dict[str, float] = {}
        # When the SERVER returns a daily-quota 429, classify_document calls
        # trip_daily(model) which sets this monotonic deadline. wait() will
        # block until that moment (the next 00:00 PT) and then clear the
        # rolling windows so we start fresh on the server's new quota day.
        self._tripped_until: dict[str, float] = {}

    def _lock_for(self, model: str) -> threading.Lock:
        lock = self._locks.get(model)
        if lock is None:
            lock = threading.Lock()
            self._locks[model] = lock
        return lock

    def _prune_minute(self, model: str, now: float) -> None:
        history = self._history[model]
        cutoff = now - 60.0
        while history and history[0][0] < cutoff:
            history.popleft()

    def _prune_day(self, model: str, now: float) -> None:
        daily = self._daily[model]
        cutoff = now - 86_400.0   # 24 h
        while daily and daily[0] < cutoff:
            daily.popleft()

    def trip_daily(self, model: str) -> None:
        """
        Mark this model's daily quota as exhausted on the server side.
        After this call, wait(model) will block until the next 00:00 PT
        (Google's documented quota-reset boundary), at which point the
        rolling windows are cleared and the model is allowed to fire again.
        Safe to call repeatedly — the deadline is only extended, never moved
        backward.
        """
        wait_secs = _seconds_until_next_pt_midnight()
        with self._lock_for(model):
            new_deadline = time.monotonic() + wait_secs
            old_deadline = self._tripped_until.get(model)
            if old_deadline is None or new_deadline > old_deadline:
                self._tripped_until[model] = new_deadline
        log.error(
            "[%s] daily quota EXHAUSTED on server  —  circuit tripped, "
            "worker will sleep %s until 00:00 PT",
            MODEL_TAGS.get(model, model), _fmt_elapsed(wait_secs),
        )

    def wait(self, model: str) -> None:
        """
        Block until a new request is allowed under ALL constraints, OR until
        STOP_EVENT is set (in which case we return early; callers should
        check STOP_EVENT themselves and abort cleanly).

        Order of checks each iteration:
          0. STOP_EVENT  — short-circuit on graceful-shutdown signal
          1. Daily-quota TRIP  — server told us we're out, sleep until 00:00 PT
          2. RPD rolling window full  — local heuristic, sleep until 00:00 PT
          3. TPM rolling budget  — short wait until tokens age out
          4. RPM hard cap  — short wait for the next 4-second slot
        """
        tag = MODEL_TAGS.get(model, model)
        with self._lock_for(model):
            while True:
                if STOP_EVENT.is_set():
                    return

                now = time.monotonic()

                # ── (1) Server-confirmed daily-quota trip. ────────────────
                trip_until = self._tripped_until.get(model)
                if trip_until is not None and trip_until > now:
                    wait_secs = trip_until - now
                    log.warning(
                        "[%s] daily circuit tripped  —  sleeping %s until 00:00 PT",
                        tag, _fmt_elapsed(wait_secs),
                    )
                    if STOP_EVENT.wait(wait_secs):
                        return
                    # Slept past midnight PT — server has refreshed, so clear
                    # our local windows and start the new day from scratch.
                    self._tripped_until.pop(model, None)
                    self._daily[model].clear()
                    self._history[model].clear()
                    continue

                self._prune_minute(model, now)
                self._prune_day(model, now)

                # ── (2) Local RPD rolling-window heuristic. ───────────────
                if len(self._daily[model]) >= self._rpd:
                    wait_secs = _seconds_until_next_pt_midnight()
                    log.warning(
                        "[%s] local RPD %d/day reached  —  sleeping %s until 00:00 PT",
                        tag, self._rpd, _fmt_elapsed(wait_secs),
                    )
                    if STOP_EVENT.wait(wait_secs):
                        return
                    self._daily[model].clear()
                    self._history[model].clear()
                    continue

                # ── (3,4) TPM and RPM: short waits, re-check after. ───────
                tpm_used = sum(t for _, t in self._history[model])
                tpm_ok = (tpm_used + self._est) <= self._tpm

                last = self._last_call.get(model, 0.0)
                rpm_ok = (now - last) >= self._min_gap

                if tpm_ok and rpm_ok:
                    if STOP_EVENT.wait(random.uniform(0.0, 0.5)):
                        return
                    self._last_call[model] = time.monotonic()
                    return

                waits: list[float] = []
                if not rpm_ok:
                    waits.append(self._min_gap - (now - last))
                if not tpm_ok and self._history[model]:
                    oldest_ts = self._history[model][0][0]
                    waits.append((oldest_ts + 60.0) - now + 0.1)
                if STOP_EVENT.wait(max(0.1, min(waits) if waits else 0.1)):
                    return

    def record(self, model: str, prompt_tokens: Optional[int]) -> None:
        """
        Update both the TPM window (with actual or fallback token count)
        and the RPD window (one entry per call).
        """
        tokens = prompt_tokens if (prompt_tokens and prompt_tokens > 0) else self._est
        with self._lock_for(model):
            now = time.monotonic()
            self._history[model].append((now, tokens))
            self._daily[model].append(now)


LIMITER = PerModelLimiter()


# ── Database Helpers ──────────────────────────────────────────────────────────

def migrate_db(conn: sqlite3.Connection) -> None:
    """
    Ensure required columns exist on FILES and PROJECTS:
      FILES.class            TEXT     2-digit ISIC division code
      FILES.content_length   INTEGER  chars of extracted text (used to rank
                                      a project's files by 'context length')
      FILES.classified_by    TEXT     short tag of the worker model that
                                      produced this classification
                                      (e.g. '31b', '26b'). Lets us audit or
                                      re-run files labelled by the smaller
                                      model if labels diverge.
      PROJECTS.class         TEXT     mirror of the class of this project's
                                      file with the largest content_length
      PROJECTS.classified_by TEXT     model tag responsible for PROJECTS.class
                                      (either the file-derived class or the
                                      metadata-only fallback)
    Idempotent — safe to call on every run.
    """
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(FILES)")
    files_cols = {row[1] for row in cur.fetchall()}
    if "class" not in files_cols:
        cur.execute("ALTER TABLE FILES ADD COLUMN class TEXT DEFAULT NULL")
        log.info("DB migration: 'class' column added to FILES.")
    if "content_length" not in files_cols:
        cur.execute("ALTER TABLE FILES ADD COLUMN content_length INTEGER DEFAULT NULL")
        log.info("DB migration: 'content_length' column added to FILES.")
    if "classified_by" not in files_cols:
        cur.execute("ALTER TABLE FILES ADD COLUMN classified_by TEXT DEFAULT NULL")
        log.info("DB migration: 'classified_by' column added to FILES.")

    cur.execute("PRAGMA table_info(PROJECTS)")
    proj_cols = {row[1] for row in cur.fetchall()}
    if "class" not in proj_cols:
        cur.execute("ALTER TABLE PROJECTS ADD COLUMN class TEXT DEFAULT NULL")
        log.info("DB migration: 'class' column added to PROJECTS.")
    if "classified_by" not in proj_cols:
        cur.execute("ALTER TABLE PROJECTS ADD COLUMN classified_by TEXT DEFAULT NULL")
        log.info("DB migration: 'classified_by' column added to PROJECTS.")

    conn.commit()


def get_projects_to_process(conn: sqlite3.Connection) -> list[tuple]:
    """
    Returns (project_id, title, description, repo_folder, proj_folder) for
    every project still needing work in the configured scope. A project is
    included when either:
      • PROJECTS.class IS NULL  (never been classified at the project level), OR
      • at least one of its SUCCEEDED, classifiable FILES.class is NULL
        (edge case: previously failed at API time, e.g. 429s, and should be
        re-tried — files that genuinely cannot be extracted will just skip
        again at the extractor step, so this is safe).

    Filters applied on top:
      • REPOSITORY_ID_FILTER     scopes to one repository if set.
      • START_FROM_PROJECT_ID    skips earlier already-done projects if set
                                 (useful to bound the straggler sweep).

    Ordered by project id.
    """
    placeholders = ",".join(f"'{t}'" for t in sorted(CLASSIFIABLE_TYPES))

    clauses: list[str] = []
    params: list = []
    if REPOSITORY_ID_FILTER is not None:
        clauses.append("p.repository_id = ?")
        params.append(REPOSITORY_ID_FILTER)
    if START_FROM_PROJECT_ID is not None:
        clauses.append("p.id >= ?")
        params.append(START_FROM_PROJECT_ID)
    extra = ("\n          AND " + "\n          AND ".join(clauses)) if clauses else ""

    cur = conn.cursor()
    cur.execute(f"""
        SELECT  p.id, p.title, p.description,
                p.download_repository_folder,
                p.download_project_folder
        FROM    PROJECTS p
        WHERE   (p.class IS NULL
           OR   EXISTS (
                  SELECT 1 FROM FILES f
                  WHERE  f.project_id = p.id
                    AND  f.class      IS NULL
                    AND  f.status     = 'SUCCEEDED'
                    AND  f.file_type IN ({placeholders})
                )){extra}
        ORDER BY p.id
    """, tuple(params))
    return cur.fetchall()


def get_files_for_project(
    conn: sqlite3.Connection, project_id: int
) -> list[tuple]:
    """
    Unclassified SUCCEEDED classifiable files for one project.
    Returns (file_id, file_name, file_type), ordered by file id.
    """
    placeholders = ",".join(f"'{t}'" for t in sorted(CLASSIFIABLE_TYPES))
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, file_name, file_type
        FROM   FILES
        WHERE  project_id = ?
          AND  class      IS NULL
          AND  status      = 'SUCCEEDED'
          AND  file_type  IN ({placeholders})
        ORDER  BY id
        """,
        (project_id,),
    )
    return cur.fetchall()


def project_has_class(conn: sqlite3.Connection, project_id: int) -> bool:
    """True iff PROJECTS.class is set (non-NULL) for this project."""
    cur = conn.cursor()
    cur.execute("SELECT class FROM PROJECTS WHERE id = ?", (project_id,))
    row = cur.fetchone()
    return row is not None and row[0] is not None


def save_project_classification(
    conn: sqlite3.Connection, project_id: int, result: dict, model_tag: str,
) -> None:
    """
    Persist a metadata-only classification directly onto PROJECTS.class.
    Stores ONLY the 2-digit ISIC division code (e.g. "85"), not the full
    JSON object. The full result is still printed to the log. The model
    tag (e.g. '31b') is recorded in PROJECTS.classified_by for audit.
    """
    conn.execute(
        "UPDATE PROJECTS SET class = ?, classified_by = ? WHERE id = ?",
        (result["isic_division_code"], model_tag, project_id),
    )
    conn.commit()


def get_project_file_names(
    conn: sqlite3.Connection, project_id: int, limit: int = 50
) -> list[str]:
    """File names within a project — a useful weak signal for metadata-only classification."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT file_name FROM FILES
        WHERE  project_id = ? AND status = 'SUCCEEDED'
        ORDER  BY id
        LIMIT  ?
        """,
        (project_id, limit),
    )
    return [row[0] for row in cur.fetchall()]


def save_classification(
    conn: sqlite3.Connection,
    file_id: int,
    project_id: int,
    result: dict,
    content_length: int,
    model_tag: str,
) -> None:
    """
    Persist the classification for one file and refresh the parent project's
    `class` to the class of whichever file in that project currently has the
    largest content_length. PROJECTS.classified_by is updated alongside so
    it always names the model whose class is currently shown. Runs in a
    single transaction.
    """
    cur = conn.cursor()
    cur.execute(
        "UPDATE FILES SET class = ?, content_length = ?, classified_by = ? WHERE id = ?",
        (result["isic_division_code"], content_length, model_tag, file_id),
    )
    # Propagate up to PROJECTS: pick the class (and its model tag) from the
    # longest-content classified file in this project.
    cur.execute(
        """
        UPDATE PROJECTS
        SET (class, classified_by) = (
            SELECT f.class, f.classified_by
            FROM   FILES f
            WHERE  f.project_id     = ?
              AND  f.class          IS NOT NULL
              AND  f.content_length IS NOT NULL
            ORDER  BY f.content_length DESC
            LIMIT  1
        )
        WHERE id = ?
        """,
        (project_id, project_id),
    )
    conn.commit()


# ── Text Extraction ───────────────────────────────────────────────────────────

def _truncate(text: str, max_chars: int = MAX_CHARS_PER_FILE) -> str:
    """Hard-cut text at max_chars to stay within the per-request token budget."""
    if len(text) <= max_chars:
        return text
    log.debug("Text truncated %d → %d chars to fit token budget.", len(text), max_chars)
    return text[:max_chars] + "\n\n[... CONTENT TRUNCATED TO FIT TOKEN BUDGET ...]"


def extract_pdf(path: Path) -> str:
    # pdfplumber preserves layout better; pypdf is the fallback.
    try:
        with pdfplumber.open(str(path)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n\n".join(pages).strip()
        if text:
            return _truncate(text)
    except Exception:
        pass
    reader = pypdf.PdfReader(str(path))
    text = "\n\n".join(page.extract_text() or "" for page in reader.pages).strip()
    return _truncate(text)


def extract_docx(path: Path) -> str:
    doc = python_docx.Document(str(path))
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    return _truncate("\n".join(paragraphs))


def extract_xlsx(path: Path) -> str:
    """Serialise each worksheet as a Markdown table (capped at MAX_TABLE_ROWS)."""
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    sheets: list[str] = []
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows: list[list[str]] = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i >= MAX_TABLE_ROWS + 1:   # +1 accounts for the header row
                break
            rows.append([str(c) if c is not None else "" for c in row])
        if not rows:
            continue
        header = rows[0]
        sep    = ["---"] * len(header)
        md     = "| " + " | ".join(header) + " |\n"
        md    += "| " + " | ".join(sep)    + " |\n"
        md    += "\n".join(
            "| " + " | ".join(r) + " |" for r in rows[1:]
        )
        sheets.append(f"## Sheet: {sheet_name}\n{md}")
    wb.close()
    return _truncate("\n\n".join(sheets))


def extract_tab(path: Path) -> str:
    df = pd.read_csv(
        str(path),
        sep="\t",
        nrows=MAX_TABLE_ROWS,
        dtype=str,
        on_bad_lines="skip",
        encoding_errors="replace",
    )
    return _truncate(df.to_markdown(index=False))


def extract_csv(path: Path) -> str:
    df = pd.read_csv(
        str(path),
        nrows=MAX_TABLE_ROWS,
        dtype=str,
        on_bad_lines="skip",
        encoding_errors="replace",
    )
    return _truncate(df.to_markdown(index=False))


def extract_txt(path: Path) -> str:
    return _truncate(path.read_text(encoding="utf-8", errors="replace"))


def _process_archive_members(
    members: list[tuple[Path, callable]],
    archive_name: str,
    archive_kind: str,
) -> str:
    """
    Shared bundle-extraction loop for ZIP / 7z / RAR / QDA archives.

    `members` is a list of (inner_path, extract_fn) tuples where
    extract_fn(tmp_dir: Path) -> Path returns the on-disk extracted file.

    Unsupported and nested-archive members are skipped (ARCHIVE_TYPES guard
    prevents infinite recursion). All successfully extracted inner texts are
    concatenated and truncated to MAX_CHARS_PER_FILE as one bundled document.
    """
    pieces: list[str] = []
    with tempfile.TemporaryDirectory(prefix=f"{archive_kind}_") as tmp:
        tmp_dir = Path(tmp)
        for inner, extract_fn in members:
            ext = inner.suffix.lstrip(".").lower()
            if ext in ARCHIVE_TYPES or ext not in _EXTRACTORS:
                continue
            try:
                extracted_path = extract_fn(tmp_dir)
            except Exception as exc:
                log.warning("  Could not unpack %s from %s — %s",
                            inner.name, archive_name, exc)
                continue
            try:
                inner_text = _EXTRACTORS[ext](extracted_path)
            except Exception as exc:
                log.warning("  Inner extraction error [%s] %s — %s",
                            ext, inner.name, exc)
                continue
            if inner_text and inner_text.strip():
                pieces.append(f"## Embedded file: {inner.name}\n{inner_text}")
    if not pieces:
        return ""
    return _truncate("\n\n".join(pieces))


def extract_zip_archive(path: Path) -> str:
    """
    Handles plain .zip files AND modern QDA project files (REFI-QDA, MAXQDA
    2018+, ATLAS.ti 9+) — all of which are ZIP containers. The bundle is
    classified as ONE combined document. Legacy proprietary / SQLite-based
    .qda variants are not ZIPs and are logged then skipped.
    """
    try:
        zf = zipfile.ZipFile(str(path))
    except zipfile.BadZipFile:
        log.warning("  %s is not a valid ZIP archive — skipping.", path.name)
        return ""
    with zf:
        members = [
            (
                Path(m.filename),
                (lambda m=m: lambda tmp_dir: Path(zf.extract(m, tmp_dir)))(),
            )
            for m in zf.infolist()
            if not m.is_dir()
        ]
        return _process_archive_members(members, path.name, "zip")


def extract_7z(path: Path) -> str:
    """
    Open a .7z archive and extract each supported inner document. Requires
    `py7zr` (lazy import — missing dep yields a warning and a clean skip
    rather than an import-time error).
    """
    try:
        import py7zr  # type: ignore
    except ImportError:
        log.warning("  py7zr not installed — install with `pip install py7zr` "
                    "to support .7z files. Skipping %s", path.name)
        return ""
    try:
        sz = py7zr.SevenZipFile(str(path), mode="r")
    except Exception as exc:
        log.warning("  %s is not a valid .7z archive (%s) — skipping.", path.name, exc)
        return ""
    with sz:
        names = [n for n in sz.getnames() if not n.endswith("/")]
        # py7zr extracts the WHOLE archive into a directory; we cannot pull
        # one member at a time without re-opening, so do a single bulk extract
        # before iterating members.
        with tempfile.TemporaryDirectory(prefix="7z_") as tmp:
            tmp_dir = Path(tmp)
            try:
                sz.extractall(path=str(tmp_dir))
            except Exception as exc:
                log.warning("  Could not unpack %s — %s", path.name, exc)
                return ""
            pieces: list[str] = []
            for name in names:
                inner = Path(name)
                ext = inner.suffix.lstrip(".").lower()
                if ext in ARCHIVE_TYPES or ext not in _EXTRACTORS:
                    continue
                extracted = tmp_dir / name
                if not extracted.is_file():
                    continue
                try:
                    inner_text = _EXTRACTORS[ext](extracted)
                except Exception as exc:
                    log.warning("  Inner extraction error [%s] %s — %s",
                                ext, inner.name, exc)
                    continue
                if inner_text and inner_text.strip():
                    pieces.append(f"## Embedded file: {inner.name}\n{inner_text}")
            if not pieces:
                return ""
            return _truncate("\n\n".join(pieces))


def extract_rar(path: Path) -> str:
    """
    Open a .rar archive and extract each supported inner document. Requires
    `rarfile` and the external `unrar` (or `bsdtar`) binary on PATH. Missing
    library or binary yields a warning and a clean skip.
    """
    try:
        import rarfile  # type: ignore
    except ImportError:
        log.warning("  rarfile not installed — install with `pip install rarfile` "
                    "and have unrar.exe on PATH to support .rar files. Skipping %s",
                    path.name)
        return ""
    try:
        rf = rarfile.RarFile(str(path))
    except Exception as exc:
        # rarfile raises various subclasses (BadRarFile, RarCannotExec, …).
        log.warning("  %s is not a usable .rar archive (%s) — skipping.",
                    path.name, exc)
        return ""
    with rf:
        members = [
            (
                Path(m.filename),
                (lambda m=m: lambda tmp_dir: Path(rf.extract(m, tmp_dir)))(),
            )
            for m in rf.infolist()
            if not m.is_dir()
        ]
        return _process_archive_members(members, path.name, "rar")


def extract_xls(path: Path) -> str:
    """
    Legacy Excel binary (.xls). Read all sheets with pandas/xlrd and render
    each as a markdown table, capped at MAX_TABLE_ROWS. xlrd is imported
    lazily so the script still runs without the dep, just skipping .xls.
    """
    try:
        import xlrd  # noqa: F401  — pandas dispatches to xlrd via engine="xlrd"
    except ImportError:
        log.warning("  xlrd not installed — install with `pip install xlrd` "
                    "to support .xls files. Skipping %s", path.name)
        return ""
    try:
        sheets_dict = pd.read_excel(
            str(path), sheet_name=None, engine="xlrd", dtype=str,
            nrows=MAX_TABLE_ROWS,
        )
    except Exception as exc:
        log.warning("  Could not read .xls %s — %s", path.name, exc)
        return ""
    sheets: list[str] = []
    for name, df in sheets_dict.items():
        if df.empty:
            continue
        md = df.fillna("").to_markdown(index=False)
        sheets.append(f"## Sheet: {name}\n{md}")
    return _truncate("\n\n".join(sheets))


class _HTMLTextExtractor(HTMLParser):
    """Collect visible text from an HTML stream — skip script/style/head."""
    _SKIP = {"script", "style", "head", "noscript"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in self._SKIP and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0:
            text = data.strip()
            if text:
                self._parts.append(text)

    def text(self) -> str:
        return "\n".join(self._parts)


def extract_html(path: Path) -> str:
    """Strip tags and decode entities from an HTML file via stdlib parser."""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        log.warning("  Could not read .html %s — %s", path.name, exc)
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(raw)
    parser.close()
    return _truncate(parser.text())


# Printable-ASCII run + printable-UTF-16-LE run regexes, used by extract_doc.
_DOC_ASCII_RUN = re.compile(rb"[\x20-\x7e\t\n\r]{6,}")
_DOC_UTF16_RUN = re.compile(rb"(?:[\x20-\x7e\t\n\r]\x00){6,}")


def extract_doc(path: Path) -> str:
    """
    Legacy Word binary (.doc). The proper text lives in the WordDocument
    OLE stream behind a piece-table — too complex to parse without a
    dedicated library. We use a best-effort heuristic: extract runs of
    printable ASCII and UTF-16LE bytes from that stream. Quality varies,
    but it gives enough signal for division-level classification, and any
    project where this fails still gets the metadata-only fallback.
    """
    try:
        import olefile  # type: ignore
    except ImportError:
        log.warning("  olefile not installed — install with `pip install olefile` "
                    "to support .doc files. Skipping %s", path.name)
        return ""
    try:
        ole = olefile.OleFileIO(str(path))
    except Exception as exc:
        log.warning("  %s is not a valid OLE .doc (%s) — skipping.", path.name, exc)
        return ""
    try:
        if not ole.exists("WordDocument"):
            return ""
        data = ole.openstream("WordDocument").read()
    finally:
        ole.close()

    ascii_runs = _DOC_ASCII_RUN.findall(data)
    utf16_runs = _DOC_UTF16_RUN.findall(data)
    parts: list[str] = []
    for run in ascii_runs:
        try:
            parts.append(run.decode("latin-1"))
        except Exception:
            continue
    for run in utf16_runs:
        try:
            parts.append(run.decode("utf-16le", errors="replace"))
        except Exception:
            continue
    if not parts:
        return ""
    return _truncate("\n".join(parts))


_EXTRACTORS = {
    "pdf":  extract_pdf,
    "docx": extract_docx,
    "doc":  extract_doc,
    "xlsx": extract_xlsx,
    "xls":  extract_xls,
    "tab":  extract_tab,
    "csv":  extract_csv,
    "txt":  extract_txt,
    "html": extract_html,
    "htm":  extract_html,
    # ── Generic archives — extracted as one bundle, like QDA archives ─────
    "zip":  extract_zip_archive,
    "7z":   extract_7z,
    "rar":  extract_rar,
    # ── QDA project archives — all dispatched through extract_zip_archive ─
    "qda":            extract_zip_archive,
    "qdpx":           extract_zip_archive,
    "mqda":           extract_zip_archive,
    "mqtc":           extract_zip_archive,
    "mx24":           extract_zip_archive,
    "mc24":           extract_zip_archive,
    "mx22":           extract_zip_archive,
    "mx20":           extract_zip_archive,
    "mx18":           extract_zip_archive,
    "atlproj":        extract_zip_archive,
    "atlprojbundle":  extract_zip_archive,
    #
    # Intentionally NOT registered (proprietary / SQLite-based, not extractable
    # without dedicated tooling):
    #   MAXQDA legacy : mx12, mx11, mx5, mx4, mx3, mx2, m2k
    #   NVivo         : nvp, nvpx, nvivo
    #   ATLAS.ti old  : atl, hpr7
    #   QDA Miner     : ppj
    #   f4analyse     : qdc, f4project
    #   Quirkos       : qrk
}


def extract_text(file_type: str, path: Path) -> Optional[str]:
    extractor = _EXTRACTORS.get(file_type.lower())
    if extractor is None:
        return None
    try:
        result = extractor(path)
        return result if result and result.strip() else None
    except Exception as exc:
        log.warning("  Extraction error [%s] %s — %s", file_type, path.name, exc)
        return None


# ── Classifier ────────────────────────────────────────────────────────────────

def _parse_classification_response(response) -> dict:
    """
    Convert a GenerateContentResponse into a validated classification dict.

    Tries the SDK-parsed Pydantic object first (google-genai ≥ 0.8). When the
    SDK didn't / couldn't parse it — common with the 26B model, which often
    appends a second JSON object or trailing prose after the answer — fall
    back to JSONDecoder.raw_decode() to grab the FIRST valid JSON object and
    drop anything that follows. Raises on truly malformed responses so the
    caller's retry loop can take over.
    """
    parsed = getattr(response, "parsed", None)
    if parsed is not None:
        return parsed.model_dump()

    raw = (response.text or "").strip()
    if raw.startswith("```"):
        # strip ```json ... ``` or ``` ... ```
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    # raw_decode returns (obj, end_index). Anything past end_index — a second
    # JSON object, a trailing explanation, stray whitespace — is ignored.
    result, end = json.JSONDecoder().raw_decode(raw)
    if end < len(raw.rstrip()):
        log.debug("Ignored %d trailing chars after JSON object.", len(raw) - end)
    ISICClassification(**result)   # raises ValidationError on bad shape
    return result


def _call_api(
    text: str,
    file_name: str,
    project_title: str,
    project_description: Optional[str],
    model: str,
) -> dict:
    """
    Single API call to Gemma 4.  Raises on any failure so the caller can retry.
    Returns a validated dict matching ISICClassification fields.
    """
    desc_snippet = (project_description or "")[:2500]
    user_content = (
        f"Project title: {project_title}\n"
        f"Project description: {desc_snippet}\n"
        f"File name: {file_name}\n\n"
        f"--- Document Content ---\n{text}"
    )

    response = CLIENT.models.generate_content(
        model=model,
        contents=user_content,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            # Structured output: SDK passes the Pydantic schema to the model.
            # Gemma 4 on AI Studio honours response_mime_type; response_schema
            # is best-effort — the JSON fallback below handles cases where the
            # model ignores the schema hint and returns plain text JSON.
            response_mime_type="application/json",
            response_schema=ISICClassification,
            temperature=0.1,      # low temperature for deterministic classification
            max_output_tokens=512,
        ),
    )

    # Feed the actual input-token count back into the rolling TPM window so
    # the limiter can pace adaptively. SDK populates usage_metadata on every
    # successful generateContent response.
    usage = getattr(response, "usage_metadata", None)
    prompt_tokens = getattr(usage, "prompt_token_count", None) if usage else None
    LIMITER.record(model, prompt_tokens)

    return _parse_classification_response(response)


def classify_document(
    text: str,
    file_name: str,
    project_title: str,
    project_description: Optional[str],
    model: str,
) -> Optional[dict]:
    """
    Attempt classification with exponential-backoff retry — pinned to ONE
    model so every file inside a project keeps the same model provenance.

    Retry schedule (application-layer, on top of SDK-level retries):
        3 attempts on `model`, jitter scaled by attempt number.

    Pacing is handled by LIMITER.wait(model) before each call so the per-model
    1-RPM budget is respected even if two workers run in parallel.

    Returns the classification dict, or None if all attempts fail.
    """
    tag = MODEL_TAGS.get(model, model)
    attempts = 3
    for attempt in range(1, attempts + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            LIMITER.wait(model)
            if STOP_EVENT.is_set():
                return None
            return _call_api(text, file_name, project_title, project_description, model)
        except Exception as exc:
            # Daily-quota exhaustion is not retryable until 00:00 PT — trip
            # the breaker and stop retrying this file. Subsequent files will
            # park inside LIMITER.wait() until the daily quota refreshes.
            if _is_daily_quota_429(exc):
                LIMITER.trip_daily(model)
                return None
            jitter = random.uniform(2.0, 5.0) * attempt
            log.warning(
                "[%s]   ↻ retry %d/%d  %s  (sleeping %.1fs)  %s",
                tag, attempt, attempts, exc, jitter, file_name,
            )
            if STOP_EVENT.wait(jitter):
                return None

    log.error("[%s]   ✗ exhausted %d attempts  %s", tag, attempts, file_name)
    return None


def classify_project_metadata_only(
    project_title: str,
    project_description: Optional[str],
    file_names: list[str],
    model: str,
) -> Optional[dict]:
    """
    Fallback classifier for projects whose files cannot be text-extracted
    (e.g., all scanned-image PDFs, unsupported formats, missing on disk).
    Uses ONLY project title + full description + the list of file names —
    no document content. Pinned to ONE model (same retry policy as
    classify_document) so the project's provenance stays consistent.
    """
    file_list = (
        "\n".join(f"  - {n}" for n in file_names)
        if file_names else "  (no readable files in project)"
    )
    user_content = (
        "NOTE: No file in this project could be text-extracted "
        "(e.g., all files are scanned-image PDFs or unsupported formats). "
        "Classify the project based on its metadata and file names ONLY.\n\n"
        f"Project title: {project_title}\n"
        f"Project description: {project_description or '(none)'}\n\n"
        f"File names in this project:\n{file_list}"
    )

    tag = MODEL_TAGS.get(model, model)
    attempts = 3
    for attempt in range(1, attempts + 1):
        if STOP_EVENT.is_set():
            return None
        try:
            LIMITER.wait(model)
            if STOP_EVENT.is_set():
                return None
            response = CLIENT.models.generate_content(
                model=model,
                contents=user_content,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    response_mime_type="application/json",
                    response_schema=ISICClassification,
                    temperature=0.1,
                    max_output_tokens=512,
                ),
            )
            usage = getattr(response, "usage_metadata", None)
            prompt_tokens = getattr(usage, "prompt_token_count", None) if usage else None
            LIMITER.record(model, prompt_tokens)
            return _parse_classification_response(response)
        except Exception as exc:
            if _is_daily_quota_429(exc):
                LIMITER.trip_daily(model)
                return None
            jitter = random.uniform(2.0, 5.0) * attempt
            log.warning(
                "[%s]   ↻ retry %d/%d  (metadata-only)  %s  (sleeping %.1fs)",
                tag, attempt, attempts, exc, jitter,
            )
            if STOP_EVENT.wait(jitter):
                return None

    log.error("[%s]   ✗ exhausted %d attempts  (metadata-only)", tag, attempts)
    return None


# ── Worker (one per model) ────────────────────────────────────────────────────

class WorkerStats:
    """Shared counters across all worker threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.request_count   = 0
        self.file_success    = 0
        self.file_skip       = 0
        self.file_fail       = 0
        self.file_inherited  = 0   # labelled from PROJECTS.class, no API call
        self.meta_success    = 0
        self.meta_fail       = 0
        self.cap_hit         = False

    def reserve_request(self) -> bool:
        """
        Atomically check the daily safety cap and reserve one request slot.
        Returns True if the caller may proceed, False if the cap is reached.
        """
        with self.lock:
            if self.request_count >= RPD_SAFETY_STOP:
                self.cap_hit = True
                return False
            self.request_count += 1
            return True


def _process_one_project(
    project_row: tuple,
    model: str,
    model_tag: str,
    conn: sqlite3.Connection,
    db_lock: threading.Lock,
    stats: WorkerStats,
    total_projects: int,
    proj_idx: int,
) -> None:
    """
    Classify every file in one project (and run the metadata-only fallback
    if needed), pinned to a single model. Every DB access is serialised by
    db_lock since we share one sqlite3.Connection across threads.
    """
    project_id, proj_title, proj_desc, repo_folder, proj_folder = project_row

    with db_lock:
        files = get_files_for_project(conn, project_id)

    title = (proj_title or "").strip().replace("\n", " ")
    if len(title) > 55:
        title = title[:54] + "…"
    log.info(
        "[%s] ▶ project %4d/%d  id=%-6d  files=%-2d  %s",
        model_tag, proj_idx, total_projects, project_id, len(files), title,
    )

    # Per-project API budget: classify up to MAX_API_CLASSIFICATIONS_PER_PROJECT
    # files via the LLM; any additional extractable files are deferred and
    # labelled with PROJECTS.class once it's known (no API cost).
    api_attempts = 0
    overflow_files: list[tuple[int, str, str, int]] = []  # (id, type, name, len)

    for file_id, file_name, file_type in files:
        if stats.cap_hit or STOP_EVENT.is_set():
            return

        file_path = (
            FILES_BASE
            / (repo_folder or "")
            / (proj_folder or "")
            / file_name
        )

        if not file_path.is_file():
            log.debug("[%s]   · skip  file=%-6d  %-5s  (not on disk)  %s",
                      model_tag, file_id, file_type, file_name)
            with stats.lock:
                stats.file_skip += 1
            continue

        text = extract_text(file_type, file_path)
        if not text:
            log.debug("[%s]   · skip  file=%-6d  %-5s  (no text)      %s",
                      model_tag, file_id, file_type, file_name)
            with stats.lock:
                stats.file_skip += 1
            continue
        content_length = len(text)

        # API budget reached for this project — record the file and move on.
        # We'll label it with PROJECTS.class after the file loop completes.
        if api_attempts >= MAX_API_CLASSIFICATIONS_PER_PROJECT:
            overflow_files.append((file_id, file_type, file_name, content_length))
            continue

        if not stats.reserve_request():
            return

        result = classify_document(text, file_name, proj_title, proj_desc, model)
        api_attempts += 1

        if result:
            with db_lock:
                save_classification(conn, file_id, project_id, result,
                                    content_length, model_tag)
            with stats.lock:
                stats.file_success += 1
            div_name = (result["division_name"] or "")[:32]
            log.info(
                "[%s]   ✓ file  file=%-6d  %-5s  [%s] %-32s  conf=%.2f  %s",
                model_tag, file_id, file_type,
                result["isic_division_code"], div_name,
                result["confidence_score"], file_name,
            )
        else:
            with stats.lock:
                stats.file_fail += 1
            log.error(
                "[%s]   ✗ file  file=%-6d  %-5s  classification failed              %s",
                model_tag, file_id, file_type, file_name,
            )

    # ── Inline metadata-only fallback ───────────────────────────────────────
    # If nothing in this project produced a class, run one metadata-only call
    # NOW. Same model as the worker for project-level consistency. Done before
    # the overflow-labelling pass so the overflow files can inherit a class.
    if not stats.cap_hit and not STOP_EVENT.is_set():
        with db_lock:
            already_classified = project_has_class(conn, project_id)
        if not already_classified and stats.reserve_request():
            with db_lock:
                file_names = get_project_file_names(conn, project_id)

            log.info("[%s]   ⓘ metadata-only fallback  (no file produced a class)",
                     model_tag)

            result = classify_project_metadata_only(
                proj_title or "", proj_desc, file_names, model,
            )

            if result:
                with db_lock:
                    save_project_classification(conn, project_id, result, model_tag)
                with stats.lock:
                    stats.meta_success += 1
                div_name = (result["division_name"] or "")[:32]
                log.info(
                    "[%s]   ✓ meta                            [%s] %-32s  conf=%.2f",
                    model_tag, result["isic_division_code"], div_name,
                    result["confidence_score"],
                )
            else:
                with stats.lock:
                    stats.meta_fail += 1
                log.error(
                    "[%s]   ✗ meta  metadata-only classification failed  (project_id=%d)",
                    model_tag, project_id,
                )

    # ── Overflow labelling pass (no API calls) ──────────────────────────────
    # Files past the per-project cap that survived extraction get labelled
    # with the project's current class. If the project still has no class
    # (every API call failed AND metadata-only failed), the overflow files
    # are left as NULL — matching the "unclassifiable files remain untouched"
    # semantics.
    if overflow_files:
        with db_lock:
            cur = conn.cursor()
            cur.execute(
                "SELECT class, classified_by FROM PROJECTS WHERE id = ?",
                (project_id,),
            )
            row = cur.fetchone()
        project_class = row[0] if row else None
        inherited_by = (row[1] if row and row[1] else model_tag)

        if project_class is not None:
            with db_lock:
                for fid, _ftype, _fname, clen in overflow_files:
                    conn.execute(
                        "UPDATE FILES "
                        "SET class = ?, content_length = ?, classified_by = ? "
                        "WHERE id = ?",
                        (project_class, clen, inherited_by, fid),
                    )
                conn.commit()
            with stats.lock:
                stats.file_inherited += len(overflow_files)
            log.info(
                "[%s]   ↳ inherited [%s] to %d overflow file(s)  (past %d-call cap)",
                model_tag, project_class, len(overflow_files),
                MAX_API_CLASSIFICATIONS_PER_PROJECT,
            )
        else:
            log.info(
                "[%s]   ↳ %d overflow file(s) left NULL  (no project class to inherit)",
                model_tag, len(overflow_files),
            )


def _worker_loop(
    model: str,
    work_queue: "queue.Queue[tuple[int, tuple]]",
    conn: sqlite3.Connection,
    db_lock: threading.Lock,
    stats: WorkerStats,
    total_projects: int,
) -> None:
    """
    Pull (proj_idx, project_row) tuples from the shared queue until empty
    or the safety cap fires. Each worker is permanently pinned to one model.
    """
    model_tag = MODEL_TAGS[model]
    log.info("[%s] worker started  (%s)", model_tag, model)
    while True:
        if STOP_EVENT.is_set():
            log.info("[%s] worker stopping  (interrupted by user)", model_tag)
            return
        if stats.cap_hit:
            log.info("[%s] worker stopping  (safety cap reached)", model_tag)
            return
        try:
            proj_idx, project_row = work_queue.get_nowait()
        except queue.Empty:
            log.info("[%s] worker stopping  (queue drained)", model_tag)
            return
        try:
            _process_one_project(
                project_row, model, model_tag,
                conn, db_lock, stats,
                total_projects, proj_idx,
            )
        except Exception:
            # Never let one bad project kill the worker — log and continue.
            log.exception("[%s] unhandled error processing project_row=%r",
                          model_tag, project_row)
        finally:
            work_queue.task_done()


# ── Main Batch Loop ───────────────────────────────────────────────────────────

def main() -> None:
    start_ts = datetime.now(timezone.utc)
    log.info(BANNER)
    log.info("  ISIC Rev. 5 Classifier")
    log.info("  Started  : %s", start_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
    for m in WORKER_MODELS:
        log.info("  Worker   : %-4s →  %s", MODEL_TAGS[m], m)
    log.info("  Quotas   : %d TPM  ·  %d RPM  ·  %d RPD   (per model)",
             TPM_LIMIT, RPM_HARD_CAP, RPD_CAP)
    log.info("  Truncate : %d chars/file  ·  %d rows/table  ·  2500 chars/desc",
             MAX_CHARS_PER_FILE, MAX_TABLE_ROWS)
    log.info("  Per-proj : up to %d API classifications  "
             "(extra files inherit project class)",
             MAX_API_CLASSIFICATIONS_PER_PROJECT)
    if REPOSITORY_ID_FILTER is not None:
        log.info("  Scope    : repository_id = %d   (other repos skipped)",
                 REPOSITORY_ID_FILTER)
    if START_FROM_PROJECT_ID is not None:
        log.info("  Scope    : project_id >= %d   (earlier projects skipped)",
                 START_FROM_PROJECT_ID)
    log.info(BANNER)

    # check_same_thread=False so both workers can use the same connection
    # under db_lock. SQLite is fine with multi-threaded access as long as
    # writes are serialised, which db_lock guarantees.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    migrate_db(conn)

    projects = get_projects_to_process(conn)
    total_projects = len(projects)
    log.info("  Projects : %d to process", total_projects)
    log.info(BANNER)

    work_queue: "queue.Queue[tuple[int, tuple]]" = queue.Queue()
    for idx, row in enumerate(projects, start=1):
        work_queue.put((idx, row))

    db_lock = threading.Lock()
    stats   = WorkerStats()

    threads = [
        threading.Thread(
            target=_worker_loop,
            name=f"worker-{MODEL_TAGS[m]}",
            args=(m, work_queue, conn, db_lock, stats, total_projects),
            daemon=False,
        )
        for m in WORKER_MODELS
    ]
    for t in threads:
        t.start()

    # Polling join: the main thread cannot receive Ctrl+C while blocked in
    # `t.join()` without a timeout (Windows in particular). Join with a small
    # timeout in a loop so KeyboardInterrupt is delivered promptly.
    interrupted = False
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
    except KeyboardInterrupt:
        interrupted = True
        log.warning("")
        log.warning(BANNER)
        log.warning("  Ctrl+C received — asking workers to finish current call and exit.")
        log.warning("  Press Ctrl+C again to force-exit immediately.")
        log.warning(BANNER)
        STOP_EVENT.set()
        # Give workers up to 60 s to break out of their current in-flight call
        # (the SDK can still be in a 64 s back-off we can't interrupt). A
        # second Ctrl+C during this window bypasses the wait entirely.
        deadline = time.monotonic() + 60.0
        try:
            while any(t.is_alive() for t in threads) and time.monotonic() < deadline:
                for t in threads:
                    t.join(timeout=0.5)
        except KeyboardInterrupt:
            log.error("  Second Ctrl+C — forcing exit.")
            try:
                conn.close()
            except Exception:
                pass
            os._exit(130)

    conn.close()

    elapsed = (datetime.now(timezone.utc) - start_ts).total_seconds()
    if interrupted:
        reason = "interrupted by user"
    elif stats.cap_hit:
        reason = "safety cap reached"
    else:
        reason = "queue drained"
    log.info(BANNER)
    log.info("  Completed  (%s)", reason)
    log.info("  Elapsed  : %s", _fmt_elapsed(elapsed))
    log.info("  Files    : %5d succeeded  ·  %5d inherited  ·  %5d skipped  ·  %5d failed",
             stats.file_success, stats.file_inherited,
             stats.file_skip, stats.file_fail)
    log.info("  Metadata : %5d succeeded  ·                                   %5d failed",
             stats.meta_success, stats.meta_fail)
    log.info("  API calls: %5d total", stats.request_count)
    log.info(BANNER)


if __name__ == "__main__":
    main()
