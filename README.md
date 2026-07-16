# Seeding-QDArchive — Data Acquisition and Classification

Automated discovery, downloading, and classification of qualitative-data-analysis (QDA) related datasets from:

- **Harvard Dataverse / Murray Research Archive** (Dataverse-based)
- **International Household Survey Network (IHSN)** catalog

The pipeline searches for QDA-relevant projects, downloads full datasets (not only the matching file), and stores **files + metadata** locally for later analysis.

![Code structure flowchart](docs/flowchart.svg)

## Repository layout

```
.
├── run_all_scrapers.py           # runs both scrapers sequentially
├── requirements.txt
├── 23220843-seeding.db           # SQLite database (metadata + classification results)
├── class_report.pdf              # Part 2 output: class-distribution report
├── project_table.xlsx            # Part 2 output: per-project results table
└── src/
    ├── dataverse_scraper.py      # Part 1: Dataverse (Harvard/Murray) scraper
    ├── ihsn_scraper.py           # Part 1: IHSN scraper
    ├── label_project_types.py    # Part 2: initial project-type labelling
    ├── isic_classifier.py        # Part 2: ISIC Rev. 5 classifier (Dataverse)
    ├── ihsn_classifier.py        # Part 2: IHSN project-type + ISIC classifier
    ├── make_class_report.py      # Part 2: builds class_report.pdf
    ├── make_project_table.py     # Part 2: builds project_table.xlsx
    ├── prompts/
    │   └── isic_system_instruction.txt   # Gemma system prompt (ISIC reference)
    ├── QDA_files_extensions.csv  # QDA tool extensions used in Dataverse file search
    ├── search_keywords.csv       # exact-match keyword phrases
    └── .env                      # API key / tokens (ignored by git)
```

## Part 1 — Data acquisition (method)

### 1) Dataverse (Harvard/Murray) — `src/dataverse_scraper.py`

Two phases are used:

1. **Extension-based file search (first run only)**  
   Loads QDA-related file extensions from `src/QDA_files_extensions.csv` and queries the Dataverse Search API for files matching `fileType:.ext`. When a match is found, the **entire dataset** is downloaded.
2. **Exact-match keyword dataset search**  
   Loads phrases from `src/search_keywords.csv` and searches datasets using exact phrase matching (quoted queries) to reduce noise.

Some datasets are hosted on external Dataverse instances; the scraper includes fallback metadata calls to additional Dataverse base URLs when needed.

### 2) IHSN — `src/ihsn_scraper.py`

Performs keyword-based catalog searches (using the same `src/search_keywords.csv` phrases), then downloads full study datasets. Requests are throttled with retries to reduce the chance of being blocked.

## Outputs

### Download rules (high level)

- **Full-dataset download**: if a QDA-relevant signal is found, the complete dataset is downloaded.
- **Restricted content**: recorded in the DB; may be skipped depending on repository response.
- **Large / multimedia**: common video/audio formats are skipped; very large files are skipped (default cap: ~500 MB).
- **Rate limiting**: Dataverse requests may pause and retry when 403 rate limits are encountered.

### Download folders

Downloads are written under `Downloaded_Files/` (this folder is ignored by git via `.gitignore`):

- `Downloaded_Files/harvard-murray-archive/…`
- `Downloaded_Files/IHSN/…`

Each dataset is stored in its own folder (named with a stable repository-specific identifier).

### Metadata database (SQLite)

Both scrapers write to `23220843-seeding.db` with these main tables:

- `PROJECTS` — dataset-level metadata (title, description/abstract, URLs, dates, etc.)
- `FILES` — file-level entries + download status
- `PERSON_ROLE` — creators/authors and their roles
- `LICENSES` — license text/info
- `KEYWORDS` — keywords normalized into one row per keyword

File download status values:

- `SUCCEEDED` — downloaded
- `FAILED` — attempted but failed
- `SKIPPED` — intentionally not downloaded (e.g., restricted, large multimedia, over size cap)

## Setup

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Create `src/.env` (ignored by git) with the credentials you need:
   - `GOOGLE_API_KEY=...` — required for Part 2 (classification via the Gemma API)
   - `DATAVERSE_API_TOKEN=...` — optional, for Part 1 Dataverse requests

IHSN tuning via environment variables (optional):

- `IHSN_THROTTLE_SLEEP_SECONDS` (default `600`)
- `IHSN_THROTTLE_MAX_RETRIES` (default `3`)
- `IHSN_PAGE_SIZE` (default `50`)
- `IHSN_MAX_STUDIES_PER_KEYWORD` (default `0`, meaning “no limit”)

## Run (Part 1)

- Run both scrapers (recommended):
  - `python run_all_scrapers.py`
- Or run individually:
  - `python src/dataverse_scraper.py`
  - `python src/ihsn_scraper.py`

## Results (from the accompanying report)

In one run documented in the project report:

- Harvard Dataverse/Murray: **602 projects**, **9,559 files**
- IHSN: **1,447 projects**, **6,998 files**
- Total: **2,049 projects**, **16,557 files**

Counts will vary depending on keywords, extensions, and when the scrape is performed.

## Part 2 — Classification

After acquisition, every project is (1) assigned a **project type** and (2) classified into an **economic-activity class** using the UN **ISIC Rev. 5** taxonomy at the division (two-digit) level.

### Model

Classification is performed with **Gemma**, an **open-source, open-weight large language model published by Google**, accessed here through the Google AI Studio API (the `google-genai` SDK); the specific checkpoint used is `gemma-4-31b-it`. Because Gemma's weights are released openly, the same pipeline could equally be pointed at a locally hosted copy of the model.

The system prompt given to the model — the full ISIC Rev. 5 division reference together with the classification rule — is kept in [`src/prompts/isic_system_instruction.txt`](src/prompts/isic_system_instruction.txt) rather than hard-coded, so it can be edited without touching the code. Its core instruction reads:

> Determine the division by the SUBJECT MATTER of the document — not its file format.
> Examples: interview transcripts about education → 85; agricultural survey data → 01;
> epidemiology dataset → 86; software repository → 62; policy analysis → 84.

### Project types

Each project is labelled with one `PROJECT_TYPE`:

- `QDA_PROJECT` — contains a qualitative-data-analysis file (a QDA-tool project file)
- `QD_PROJECT` — contains qualitative primary data (e.g. a readable research document or transcript)
- `OTHER_PROJECT` — contains only other valid data files
- `NOT_A_PROJECT` — nothing could be derived from the files

### Pipeline

- **`src/label_project_types.py`** — assigns the initial `PROJECT_TYPE` from file extensions (a QDA-tool file makes a project a `QDA_PROJECT`, and so on).
- **`src/isic_classifier.py`** — classifies the Dataverse (Harvard/Murray) projects and their files into ISIC divisions. Text is extracted from each file (PDF / DOCX / XLSX / CSV / TXT / archives …), truncated to fit the model's token budget, and sent for structured JSON classification. An adaptive per-model rate limiter respects the API's per-minute and per-day quotas.
- **`src/ihsn_classifier.py`** — classifies the IHSN projects. For each readable file it decides, in a single request, both the project-type signal (a text/paper document counts as primary data, per the project description) and the ISIC division. A project's type follows the existence rule (any primary-data file → `QD_PROJECT`), and its ISIC class — stored only for `QD_PROJECT`s — is the dominant division across its primary files. Archives are opened and count as primary data if they hold a readable document inside.

Results are written back to the database:

- `PROJECTS.type` — the project type
- `PROJECTS.class` — the project's primary ISIC division code
- `FILES.class` — the per-file ISIC division code

### Reporting

- **`src/make_class_report.py`** → **`class_report.pdf`**: for each repository, a histogram of the primary classes (full division names as labels, counts printed on the bars, drawn as vector graphics so the reader can zoom in), the twenty most common classes as a table, and a short written discussion of the findings.
- **`src/make_project_table.py`** → **`project_table.xlsx`**: a flat table of `repository_id`, `project_type`, `project_title`, `primary_class`, and `no_project_files`.

### Run (Part 2)

Requires `GOOGLE_API_KEY` in `src/.env`.

```
python src/label_project_types.py     # initial project-type labels
python src/isic_classifier.py         # ISIC classification (Dataverse)
python src/ihsn_classifier.py         # project-type + ISIC classification (IHSN)
python src/make_class_report.py       # -> class_report.pdf
python src/make_project_table.py      # -> project_table.xlsx
```

## License

See `LICENSE`.
