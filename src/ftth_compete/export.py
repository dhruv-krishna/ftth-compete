"""PDF tear-sheet export.

Renders a single-page Letter-portrait PDF using ReportLab Platypus. Layout:

    Title bar:   {City, State} - FTTH Market Tear-Sheet  |  date
    KPI grid:    8 cells — pop / MFI / poverty / housing /
                 MDU% / providers / fiber% / top advertised
    Providers:   table, top N by coverage (cropped if long)
    Speeds:      Ookla averages (when present)
    Narrative:   1-paragraph deterministic summary
    Footer:      data versions + Ookla attribution + non-commercial note

Designed to be a deck-ready handout. Single function entry point:
`build_tearsheet_pdf(sheet) -> bytes` for in-memory use; the Streamlit app
serves it via `st.download_button`.
"""

from __future__ import annotations

import io
from datetime import date

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from .format import fmt_currency, fmt_int, fmt_pct, fmt_speed
from .pipeline import TearSheet
from .ui.narrative import fiber_availability_share, fiber_share, market_narrative

PAGE_WIDTH = letter[0]
USABLE_WIDTH = PAGE_WIDTH - inch  # left+right margins of 0.5"

# Color palette — kept restrained on purpose. The PDF should look like a
# market-research handout, not a brochure.
HEADER_BG = colors.HexColor("#1F2A40")
HEADER_FG = colors.HexColor("#FFFFFF")
ROW_ALT_BG = colors.HexColor("#F4F6FA")
ROW_BORDER = colors.HexColor("#D9DEE8")
LABEL_COLOR = colors.HexColor("#6B7280")
ACCENT_GREEN = colors.HexColor("#1F8A4C")


def build_tearsheet_pdf(sheet: TearSheet) -> bytes:
    """Build a PDF tear-sheet for the given TearSheet, return bytes."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title=(
            f"{sheet.market['city']}, {sheet.market['state']} - "
            "FTTH Market Tear-Sheet"
        ),
        author="ftth-compete",
    )
    styles = _styles()
    story = []

    story.extend(_title_block(sheet, styles))
    story.append(Spacer(1, 10))
    story.append(_kpi_grid(sheet))
    story.append(Spacer(1, 14))
    story.append(Paragraph("Snapshot", styles["section"]))
    story.append(Paragraph(market_narrative(sheet), styles["body"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Providers", styles["section"]))
    story.append(_providers_table(sheet))
    story.append(Spacer(1, 12))
    speeds = _speeds_block(sheet, styles)
    if speeds:
        story.extend(speeds)
        story.append(Spacer(1, 12))
    story.extend(_footer(sheet, styles))

    doc.build(story)
    return buf.getvalue()


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    title = ParagraphStyle(
        "title", parent=base["Title"],
        fontSize=18, leading=22, textColor=HEADER_BG, alignment=0,
    )
    subtitle = ParagraphStyle(
        "subtitle", parent=base["Italic"],
        fontSize=10, textColor=LABEL_COLOR, leading=12, spaceAfter=4,
    )
    section = ParagraphStyle(
        "section", parent=base["Heading2"],
        fontSize=12, textColor=HEADER_BG, leading=15, spaceAfter=4, spaceBefore=4,
    )
    body = ParagraphStyle(
        "body", parent=base["BodyText"],
        fontSize=9.5, leading=13, spaceAfter=4,
    )
    footer = ParagraphStyle(
        "footer", parent=base["BodyText"],
        fontSize=7.5, textColor=LABEL_COLOR, leading=10,
    )
    return {
        "title": title,
        "subtitle": subtitle,
        "section": section,
        "body": body,
        "footer": footer,
    }


def _title_block(sheet: TearSheet, styles: dict[str, ParagraphStyle]) -> list:
    title = Paragraph(
        f"<b>{sheet.market['city']}, {sheet.market['state']}</b> "
        "&middot; FTTH Market Tear-Sheet",
        styles["title"],
    )
    n_tracts = sheet.demographics.n_tracts
    sub = Paragraph(
        f"{n_tracts} census tract{'s' if n_tracts != 1 else ''} "
        f"&middot; generated {date.today().isoformat()} &middot; ftth-compete",
        styles["subtitle"],
    )
    return [title, sub]


def _kpi_grid(sheet: TearSheet) -> Table:
    h = sheet.housing
    d = sheet.demographics
    # Distinct canonical providers (sheet.providers is per-(provider, tech))
    n_providers = (
        len({p.canonical_name for p in sheet.providers}) if sheet.providers else 0
    )

    if sheet.providers:
        max_advertised = max((p.max_advertised_down or 0) for p in sheet.providers)
    else:
        max_advertised = 0

    if sheet.tract_speeds:
        downs = [t.get("median_down_mbps") for t in sheet.tract_speeds if t.get("median_down_mbps")]
        avg_measured = (sum(downs) / len(downs)) if downs else None
    else:
        avg_measured = None

    fiber_avail = fiber_availability_share(sheet.location_availability)

    cells = [
        ("POPULATION", fmt_int(d.population)),
        ("MEDIAN HH INCOME", fmt_currency(d.median_household_income_weighted)),
        ("POVERTY RATE", fmt_pct(d.poverty_rate)),
        ("HOUSING UNITS", fmt_int(d.housing_units_total)),
        ("MDU SHARE", fmt_pct(h.mdu_share)),
        ("ACTIVE PROVIDERS", str(n_providers)),
        (
            "FIBER AVAILABLE" if fiber_avail is not None else "FIBER PROVIDERS",
            fmt_pct(fiber_avail) if fiber_avail is not None else fmt_pct(fiber_share(sheet.providers)),
        ),
        (
            "TOP MEASURED DOWN" if avg_measured is not None else "TOP ADVERTISED",
            fmt_speed(avg_measured) if avg_measured is not None else fmt_speed(max_advertised or None),
        ),
    ]

    # 2 rows of 4 cells each. Each cell is itself a 2-row sub-table:
    # (label, value) — but ReportLab Tables can do this with paragraphs.
    rows = [[], []]
    for i, (label, value) in enumerate(cells):
        target_row = 0 if i < 4 else 1
        rows[target_row].append(_kpi_cell(label, value))

    table = Table(rows, colWidths=[USABLE_WIDTH / 4] * 4, rowHeights=0.75 * inch)
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("BOX", (0, 0), (-1, -1), 0.5, ROW_BORDER),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, ROW_BORDER),
                ("BACKGROUND", (0, 0), (-1, 0), ROW_ALT_BG),
                ("BACKGROUND", (0, 1), (-1, 1), ROW_ALT_BG),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _kpi_cell(label: str, value: str) -> Paragraph:
    return Paragraph(
        f'<font size="7" color="#6B7280">{label}</font><br/>'
        f'<font size="14" color="#1F2A40"><b>{_xml_escape(value)}</b></font>',
        ParagraphStyle("kpi_cell", fontSize=14, leading=16, alignment=0),
    )


def _providers_table(sheet: TearSheet) -> Table:
    if not sheet.providers:
        return Table(
            [[Paragraph("No providers found in FCC BDC for this market.", getSampleStyleSheet()["BodyText"])]],
            colWidths=[USABLE_WIDTH],
        )

    # Sort fiber-first, then by coverage. Cap at 12 rows for one-page fit.
    ranked = sorted(
        sheet.providers,
        key=lambda p: (not p.has_fiber, -(p.coverage_pct or 0), -p.locations_served),
    )[:12]

    ratings = sheet.provider_ratings or {}
    subs_by_key = {
        (s["canonical_name"], s["technology"]): s for s in (sheet.provider_subs or [])
    }

    header = ["Provider", "Tech", "Coverage", "Max Down", "Locations", "Est. Subs", "Google"]
    data: list[list[str]] = [header]
    for p in ranked:
        rating_str = "-"
        r = ratings.get(p.canonical_name) or {}
        if r.get("rating") is not None:
            rating_str = f"{float(r['rating']):.1f}*  ({_compact_count(r.get('user_rating_count') or 0)})"
        sub_est = subs_by_key.get((p.canonical_name, p.technology)) or {}
        if sub_est.get("estimate_mid") is not None:
            subs_str = f"~{int(sub_est['estimate_mid']):,}"
        else:
            subs_str = "-"
        data.append(
            [
                p.canonical_name,
                p.technology or "-",
                fmt_pct(p.coverage_pct),
                fmt_speed(p.max_advertised_down),
                fmt_int(p.locations_served),
                subs_str,
                rating_str,
            ]
        )

    col_widths = [
        USABLE_WIDTH * 0.26,
        USABLE_WIDTH * 0.18,
        USABLE_WIDTH * 0.09,
        USABLE_WIDTH * 0.12,
        USABLE_WIDTH * 0.11,
        USABLE_WIDTH * 0.11,
        USABLE_WIDTH * 0.13,
    ]
    table = Table(data, colWidths=col_widths, repeatRows=1)
    style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
            ("TEXTCOLOR", (0, 0), (-1, 0), HEADER_FG),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("BOX", (0, 0), (-1, -1), 0.5, ROW_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.4, ROW_BORDER),
        ]
    )
    # Zebra striping
    for row_idx in range(2, len(data), 2):
        style.add("BACKGROUND", (0, row_idx), (-1, row_idx), ROW_ALT_BG)
    table.setStyle(style)
    return table


def _speeds_block(sheet: TearSheet, styles: dict[str, ParagraphStyle]) -> list:
    if not sheet.tract_speeds:
        return []
    downs = [t.get("median_down_mbps") for t in sheet.tract_speeds if t.get("median_down_mbps") is not None]
    ups = [t.get("median_up_mbps") for t in sheet.tract_speeds if t.get("median_up_mbps") is not None]
    lats = [t.get("median_lat_ms") for t in sheet.tract_speeds if t.get("median_lat_ms") is not None]
    total_tests = sum(int(t.get("n_tests") or 0) for t in sheet.tract_speeds)

    avg_d = sum(downs) / len(downs) if downs else None
    avg_u = sum(ups) / len(ups) if ups else None
    avg_l = sum(lats) / len(lats) if lats else None

    line = (
        f"<b>{fmt_speed(avg_d)}</b> median measured down &middot; "
        f"<b>{fmt_speed(avg_u)}</b> up &middot; "
        f"<b>{int(avg_l) if avg_l is not None else '-'} ms</b> latency &middot; "
        f"{total_tests:,} Ookla tests aggregated."
    )
    return [
        Paragraph("Measured network reality", styles["section"]),
        Paragraph(line, styles["body"]),
    ]


def _footer(sheet: TearSheet, styles: dict[str, ParagraphStyle]) -> list:
    v = sheet.data_versions
    versions = (
        f"Data versions: TIGER {v.get('tiger', '?')} &middot; "
        f"ACS5 {v.get('acs5', '?')} &middot; "
        f"FCC BDC {v.get('bdc', '-')} &middot; "
        f"Ookla {v.get('ookla', '-')} &middot; "
        f"Google Places {v.get('google_places', '-')}"
    )
    attribution = (
        "Speed test data (c) Ookla, 2019-present, distributed under "
        "CC BY-NC-SA 4.0. Personal/non-commercial use only."
    )
    return [
        Spacer(1, 6),
        Paragraph(versions, styles["footer"]),
        Paragraph(attribution, styles["footer"]),
    ]


def _xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _compact_count(n: int) -> str:
    """Compact numeric format for review counts (1.2k, 24, 5.6k)."""
    if n < 1000:
        return str(n)
    if n < 10_000:
        return f"{n/1000:.1f}k"
    return f"{n//1000}k"
