#!/usr/bin/env python3
"""
make_class_report.py
────────────────────
Builds the Part-2 Results Step 4d report (project-description slide 30) as a
single vector PDF from 23220843-seeding.db. For each repository that has
classified projects it emits:

  a. A histogram of the primary ISIC classes identified — one bar per class,
     full ISIC Rev. 5 division name as the bin label, the count printed on top
     of each bar. Everything is drawn as vector graphics (matplotlib → PDF), so
     the reader can zoom in without pixelation.
  b. A rank-ordered table of the top-20 classes (most common first) with counts
     and shares.
  c. Short, data-driven comments on the findings.

"Primary class" = PROJECTS.class (each project's primary ISIC division). The
histogram/table therefore describe the per-project primary-class distribution
within each repository.

Self-contained: the ISIC division names are hard-coded (the shared-folder [1]
reference), so this script needs no API key and does not import the classifier.

Run (from repo root):
    python src/make_class_report.py
    # → class_report.pdf
"""

import sqlite3
import textwrap
from collections import Counter
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")  # headless; PDF/PNG output only
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D

DB_PATH = "23220843-seeding.db"
OUT_PDF = "class_report.pdf"
TOP_N_TABLE = 20

# Fixed display order for the project-type breakdown.
TYPE_ORDER = ["QDA_PROJECT", "QD_PROJECT", "OTHER_PROJECT", "NOT_A_PROJECT"]

# ── ISIC Rev. 5 division names (2-digit code → official full name) ────────────
# Reference [1] from the shared folder. Used verbatim as histogram bin names and
# table rows.
ISIC_DIVISIONS = {
    "01": "Crop and animal production, hunting and related service activities",
    "02": "Forestry and logging",
    "03": "Fishing and aquaculture",
    "05": "Mining of coal and lignite",
    "06": "Extraction of crude petroleum and natural gas",
    "07": "Mining of metal ores",
    "08": "Other mining and quarrying",
    "09": "Mining support service activities",
    "10": "Manufacture of food products",
    "11": "Manufacture of beverages",
    "12": "Manufacture of tobacco products",
    "13": "Manufacture of textiles",
    "14": "Manufacture of wearing apparel",
    "15": "Manufacture of leather and related products",
    "16": "Manufacture of wood and of products of wood and cork, except furniture",
    "17": "Manufacture of paper and paper products",
    "18": "Printing and reproduction of recorded media",
    "19": "Manufacture of coke and refined petroleum products",
    "20": "Manufacture of chemicals and chemical products",
    "21": "Manufacture of basic pharmaceutical products and pharmaceutical preparations",
    "22": "Manufacture of rubber and plastics products",
    "23": "Manufacture of other non-metallic mineral products",
    "24": "Manufacture of basic metals",
    "25": "Manufacture of fabricated metal products, except machinery and equipment",
    "26": "Manufacture of computer, electronic and optical products",
    "27": "Manufacture of electrical equipment",
    "28": "Manufacture of machinery and equipment n.e.c.",
    "29": "Manufacture of motor vehicles, trailers and semi-trailers",
    "30": "Manufacture of other transport equipment",
    "31": "Manufacture of furniture",
    "32": "Other manufacturing",
    "33": "Repair, maintenance and installation of machinery and equipment",
    "35": "Electricity, gas, steam and air conditioning supply",
    "36": "Water collection, treatment and supply",
    "37": "Sewerage",
    "38": "Waste collection, treatment and disposal activities; materials recovery",
    "39": "Remediation activities and other waste management services",
    "41": "Construction of buildings",
    "42": "Civil engineering",
    "43": "Specialized construction activities",
    "45": "Wholesale and retail trade and repair of motor vehicles and motorcycles",
    "46": "Wholesale trade, except of motor vehicles and motorcycles",
    "47": "Retail trade, except of motor vehicles and motorcycles",
    "49": "Land transport and transport via pipelines",
    "50": "Water transport",
    "51": "Air transport",
    "52": "Warehousing and support activities for transportation",
    "53": "Postal and courier activities",
    "55": "Accommodation",
    "56": "Food and beverage service activities",
    "58": "Publishing activities",
    "59": "Motion picture, video and television programme production, sound recording and music publishing activities",
    "60": "Programming and broadcasting activities",
    "61": "Telecommunications",
    "62": "Computer programming, consultancy and related activities",
    "63": "Information service activities",
    "64": "Financial service activities, except insurance and pension funding",
    "65": "Insurance, reinsurance and pension funding, except compulsory social security",
    "66": "Activities auxiliary to financial service and insurance activities",
    "68": "Real estate activities",
    "69": "Legal and accounting activities",
    "70": "Activities of head offices; management consultancy activities",
    "71": "Architectural and engineering activities; technical testing and analysis",
    "72": "Scientific research and development",
    "73": "Advertising and market research",
    "74": "Other professional, scientific and technical activities",
    "75": "Veterinary activities",
    "77": "Rental and leasing activities",
    "78": "Employment activities",
    "79": "Travel agency, tour operator, reservation service and related activities",
    "80": "Security and investigation activities",
    "81": "Services to buildings and landscape activities",
    "82": "Office administrative, office support and other business support activities",
    "84": "Public administration and defence; compulsory social security",
    "85": "Education",
    "86": "Human health activities",
    "87": "Residential care activities",
    "88": "Social work activities without accommodation",
    "90": "Creative, arts and entertainment activities",
    "91": "Libraries, archives, museums and other cultural activities",
    "92": "Gambling and betting activities",
    "93": "Sports activities and amusement and recreation activities",
    "94": "Activities of membership organizations",
    "95": "Repair of computers and personal and household goods",
    "96": "Other personal service activities",
    "97": "Activities of households as employers of domestic personnel",
    "98": "Undifferentiated goods- and services-producing activities of private households for own use",
    "99": "Activities of extraterritorial organizations and bodies",
}


def div_name(code: str) -> str:
    """Full ISIC division name, falling back to the raw code if unknown."""
    return ISIC_DIVISIONS.get((code or "").strip(), f"(unknown division {code})")


# ── Data access ───────────────────────────────────────────────────────────────

def get_repositories(conn) -> list[tuple]:
    """(repository_id, url, n_classified) for repos with ≥1 classified project."""
    return conn.execute(
        """
        SELECT repository_id, MIN(repository_url),
               COUNT(*) FILTER (WHERE class IS NOT NULL)
        FROM   PROJECTS
        GROUP  BY repository_id
        HAVING COUNT(*) FILTER (WHERE class IS NOT NULL) > 0
        ORDER  BY repository_id
        """
    ).fetchall()


def get_type_counts(conn, repo_id: int) -> tuple[dict, int]:
    """
    ({project_type: count}, in_progress) for a repository. All remaining projects
    are genuinely classified (unclassified IHSN placeholders were removed), so we
    simply count every typed row. in_progress is kept (always 0) for the callers.
    """
    rows = conn.execute(
        "SELECT type, COUNT(*) FROM PROJECTS WHERE repository_id = ? "
        "AND type IS NOT NULL GROUP BY type",
        (repo_id,),
    ).fetchall()
    return {t: n for t, n in rows}, 0


def format_type_counts(type_counts: dict) -> str:
    """"QDA_PROJECT: 35   ·   QD_PROJECT: 567   ·   OTHER_PROJECT: 0   ·   …"."""
    return "   ·   ".join(f"{t}: {type_counts.get(t, 0)}" for t in TYPE_ORDER)


def get_class_counts(conn, repo_id: int) -> list[tuple]:
    """[(code, count)] for a repo's primary classes, most common first."""
    rows = conn.execute(
        "SELECT class, COUNT(*) FROM PROJECTS "
        "WHERE repository_id = ? AND class IS NOT NULL GROUP BY class",
        (repo_id,),
    ).fetchall()
    # Sort by count desc, then by code for stable ties.
    return sorted(rows, key=lambda r: (-r[1], r[0]))


# ── Report presentation ───────────────────────────────────────────────────────

REPO_TITLE = {18: "Harvard/Murray Dataverse", 9: "IHSN Catalogue"}
REPO_PHRASE = {18: "the Harvard/Murray dataverse", 9: "the IHSN catalogue"}


def repo_title(repo_id: int) -> str:
    return REPO_TITLE.get(repo_id, f"Repository {repo_id}")


def repo_phrase(repo_id: int) -> str:
    return REPO_PHRASE.get(repo_id, f"repository {repo_id}")


def short_name(code: str) -> str:
    """A compact, lower-cased division label for use inside running prose."""
    seg = div_name(code).split(";")[0].split(",")[0].strip()
    return seg[:1].lower() + seg[1:] if seg else div_name(code)


def _join(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " and " + items[-1]


def type_phrase(type_counts: dict) -> str:
    """e.g. '567 as QD_PROJECT and 35 as QDA_PROJECT'."""
    parts = [f"{type_counts[t]:,} as {t}" for t in TYPE_ORDER if type_counts.get(t)]
    return _join(parts)


def prose_page(pdf, heading, paragraphs):
    """A portrait text page: a bold heading, a rule, and left-aligned paragraphs."""
    left, right, top = 0.11, 0.89, 0.885
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(left, 0.94, heading, fontsize=15, fontweight="bold", va="top")
    fig.add_artist(Line2D([left, right], [0.918, 0.918], color="#333333", lw=0.8))
    y = top
    for para in paragraphs:
        wrapped = textwrap.fill(para, 96)
        fig.text(left, y, wrapped, fontsize=10.5, va="top", linespacing=1.55)
        y -= (wrapped.count("\n") + 1) * 0.0245 + 0.026
    pdf.savefig(fig)
    plt.close(fig)


def histogram_page(pdf, repo_id, counts, fig_no):
    """One vector page: bar per class, full name label, count on top of the bar."""
    codes = [c for c, _ in counts]
    values = [n for _, n in counts]
    n = len(codes)
    total = sum(values)

    # Width scales with the number of bars so long labels stay legible; the page
    # is a single wide vector canvas the reader can zoom into.
    fig_w = max(9.0, n * 0.62)
    fig, ax = plt.subplots(figsize=(fig_w, 8.5))

    bars = ax.bar(range(n), values, color="#4C72B0", edgecolor="white", linewidth=0.4)
    ax.bar_label(bars, padding=3, fontsize=7)

    labels = [f"{code}  " + textwrap.fill(div_name(code), 22) for code in codes]
    ax.set_xticks(range(n))
    ax.set_xticklabels(labels, rotation=90, fontsize=6.5, ha="center")
    ax.set_ylabel("Number of projects (primary class)")
    ax.set_ylim(0, max(values) * 1.15)
    ax.set_title(
        f"Figure {fig_no}.  Primary ISIC Rev. 5 class distribution — "
        f"{repo_title(repo_id)} ({total} projects, {n} classes)",
        fontsize=10.5, fontweight="bold",
    )
    ax.margins(x=0.01)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def table_page(pdf, repo_id, counts, tbl_no):
    """One vector page: rank-ordered top-20 table with counts and shares."""
    total = sum(n for _, n in counts)
    top = counts[:TOP_N_TABLE]

    fig = plt.figure(figsize=(11, 8.5))  # landscape
    fig.suptitle(
        f"Table {tbl_no}.  Twenty most common primary classes — {repo_title(repo_id)}\n"
        f"(of {len(counts)} classes across {total} projects)",
        fontsize=11, fontweight="bold",
    )
    ax = fig.add_axes([0.04, 0.03, 0.92, 0.86])
    ax.axis("off")

    header = ["Rank", "ISIC", "ISIC Rev. 5 division", "Count", "Share"]
    cell_text = []
    for i, (code, cnt) in enumerate(top, 1):
        name = textwrap.fill(div_name(code), 60)
        cell_text.append([str(i), code, name, str(cnt), f"{100*cnt/total:.1f}%"])

    table = ax.table(
        cellText=cell_text, colLabels=header,
        colWidths=[0.06, 0.06, 0.68, 0.10, 0.10],
        cellLoc="left", loc="upper center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1, 1.9)
    # Bold header row.
    for col in range(len(header)):
        table[0, col].set_text_props(fontweight="bold")
        table[0, col].set_facecolor("#E6E6E6")
    pdf.savefig(fig)
    plt.close(fig)


def discussion_page(pdf, section_no, repo_id, counts, type_counts, fig_no, tbl_no):
    """One vector page: a narrative discussion of the repository's distribution."""
    total = sum(n for _, n in counts)
    n_classes = len(counts)
    (c0, n0), (c1, n1) = counts[0], counts[1]
    dom_share = 100 * n0 / total
    top3_share = 100 * sum(n for _, n in counts[:3]) / total
    heading = f"{section_no}.  {repo_title(repo_id)}"

    if repo_id == 18:
        paras = [
            f"{repo_title(repo_id)} is the larger and more varied of the two "
            f"collections. Its {total:,} projects — {type_phrase(type_counts)} — are "
            f"spread across {n_classes} of the roughly 88 ISIC Rev. 5 divisions, a "
            f"breadth that reflects the eclectic character of a university research "
            f"archive.",
            f"As Figure {fig_no} shows, the distribution is led by {short_name(c0)} "
            f"({n0} projects), {short_name(c1)} ({n1}) and {short_name(counts[2][0])} "
            f"({counts[2][1]}); together the three account for {top3_share:.0f}% of the "
            f"collection. A few applied divisions follow, and the remainder each "
            f"contribute only a handful of projects, producing the long, thin tail on "
            f"the right of the chart.",
            f"This is the profile one would expect of a general-purpose academic "
            f"repository: the material is dominated by the products of empirical "
            f"research — much of it in the health and social sciences — rather than by "
            f"any single applied sector. Table {tbl_no} lists the twenty most common "
            f"classes in full.",
        ]
    elif repo_id == 9:
        paras = [
            f"{repo_title(repo_id)} presents a very different picture. Once the entries "
            f"that could not be classified are set aside, {total:,} projects carry a "
            f"primary class ({type_phrase(type_counts)}), yet they occupy just "
            f"{n_classes} divisions and are heavily concentrated.",
            f"Figure {fig_no} makes the imbalance plain: {dom_share:.0f}% of the "
            f"classified projects fall into a single division, {short_name(c0)} "
            f"({n0} projects), with {short_name(c1)} a distant second ({n1}). The "
            f"catalogue is, after all, a register of official statistics — national "
            f"censuses and large household and agricultural surveys — and such "
            f"instruments read naturally as products of public administration or of the "
            f"sector they measure.",
            f"Two caveats temper these numbers. Only projects found to hold qualitative "
            f"primary data are classified here, so the distribution describes that "
            f"subset rather than the catalogue as a whole; and because the class was "
            f"assigned automatically from the opening pages of each project's documents "
            f"— pages typically dominated by the sponsoring agency and the mechanics of "
            f"the survey — the share attributed to public administration is very likely "
            f"overstated. The full ranking appears in Table {tbl_no}.",
        ]
    else:
        paras = [
            f"{repo_title(repo_id)} contributes {total:,} classified projects "
            f"({type_phrase(type_counts)}) across {n_classes} ISIC divisions. The most "
            f"common class is {short_name(c0)} ({n0} projects, {dom_share:.0f}%), and "
            f"the top three together account for {top3_share:.0f}% of the total. "
            f"Figure {fig_no} and Table {tbl_no} give the full distribution.",
        ]
    prose_page(pdf, heading, paras)


def cover_page(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.63, "Classification of Archived Research Projects",
             ha="center", fontsize=21, fontweight="bold")
    fig.text(0.5, 0.585, "by Economic Activity (ISIC Rev. 5)",
             ha="center", fontsize=15)
    fig.add_artist(Line2D([0.28, 0.72], [0.55, 0.55], color="black", lw=1.1))
    fig.text(0.5, 0.515, "Seeding QDArchive  ·  Part 2 — Data Classification",
             ha="center", fontsize=11, color="#333333")
    fig.text(0.5, 0.47, datetime.now().strftime("%B %Y"), ha="center", fontsize=11)
    pdf.savefig(fig)
    plt.close(fig)


def intro_page(pdf, ordered, class_data):
    grand = sum(sum(n for _, n in class_data[rid]) for rid, *_ in ordered)
    names = _join([f"{repo_phrase(rid)} (repository {rid})" for rid, *_ in ordered])
    paras = [
        f"This report describes the distribution of economic-activity classes assigned "
        f"to the research projects gathered for the QDArchive seeding effort. Two public "
        f"sources were harvested: {names}. Every project was placed into a single "
        f"primary class drawn from the United Nations International Standard Industrial "
        f"Classification of All Economic Activities, Revision 5 (ISIC Rev. 5), at the "
        f"two-digit division level.",
        f"In total {grand:,} projects across the two repositories carry a primary class. "
        f"Classes were assigned automatically from the text extracted from each "
        f"project's documents, a project's primary class being the division that best "
        f"characterises its contents. The sections that follow treat each repository in "
        f"turn — a histogram of the full class distribution, a table of the twenty most "
        f"frequent classes, and a short discussion of the patterns that emerge — and a "
        f"brief comparison closes the report.",
    ]
    prose_page(pdf, "Introduction", paras)


def conclusion_page(pdf, ordered, class_data):
    data = {rid: class_data[rid] for rid, *_ in ordered}
    if 18 in data and 9 in data:
        c18, c9 = data[18], data[9]
        max18 = 100 * c18[0][1] / sum(n for _, n in c18)
        dom9 = 100 * c9[0][1] / sum(n for _, n in c9)
        paras = [
            f"The two collections could hardly differ more in shape. The "
            f"{repo_title(18)} is broad and research-led, its projects scattered across "
            f"{len(c18)} divisions with no single class exceeding {max18:.0f}% of the "
            f"total. The {repo_title(9)}, by contrast, is narrow and administrative: "
            f"{dom9:.0f}% of its classified projects fall into one division.",
            f"For the purpose of seeding QDArchive this contrast is an asset rather than "
            f"a problem. The academic dataverse supplies breadth across the health, "
            f"social and natural sciences, while the statistical catalogue supplies "
            f"depth in the survey and census material that underpins much applied "
            f"research. Read together — and with the automated, division-level nature of "
            f"the classification kept in mind — they offer a reasonable first map of the "
            f"archive's early contents.",
        ]
    else:
        paras = [
            "Taken together, the repositories surveyed here give an initial sense of the "
            "economic-activity profile of the material collected for QDArchive. The "
            "classification is automated and works at the division level, so the figures "
            "are best read as an indicative map rather than a precise census.",
        ]
    prose_page(pdf, "Comparison and closing remarks", paras)


def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    repos = get_repositories(conn)
    if not repos:
        print("No classified projects found — nothing to report.")
        return

    # Present repository 18 first, then any others in ascending id order.
    ordered = sorted(repos, key=lambda r: (0 if r[0] == 18 else 1, r[0]))
    type_info = {rid: get_type_counts(conn, rid)[0] for rid, _, _ in ordered}
    class_data = {rid: get_class_counts(conn, rid) for rid, _, _ in ordered}

    with PdfPages(OUT_PDF) as pdf:
        cover_page(pdf)
        intro_page(pdf, ordered, class_data)
        for i, (repo_id, _url, _n) in enumerate(ordered, 1):
            counts = class_data[repo_id]
            tc = type_info[repo_id]
            print(f"Repository {repo_id}: {sum(n for _,n in counts)} projects, "
                  f"{len(counts)} classes  |  types: {format_type_counts(tc)}")
            discussion_page(pdf, i, repo_id, counts, tc, i, i)
            histogram_page(pdf, repo_id, counts, i)
            table_page(pdf, repo_id, counts, i)
        conclusion_page(pdf, ordered, class_data)

    conn.close()
    print(f"Wrote {OUT_PDF}")


if __name__ == "__main__":
    main()
