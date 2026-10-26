"""INSO security newsletters — one email = one compound report.

A report wraps several incidents (different places, dates, actors) in
report-level labelled fields: LOCATION, INCIDENT TYPE, DATE & TIME,
INFORMATION, ANALYSIS, ADVICE, ISSUED BY, SERIAL #. Ingestion is gx-only
(``InsoGXSource``); the transport is ``providers/newsletter_mailbox.py``,
which this module never imports.

  - ``parse`` is pure code over the labelled fields: HTML part first, plain
    text when there is none. Any missing label, malformed date or missing
    serial raises ``InsoParseError`` (→ quarantine), so a template change
    fails loudly instead of being guessed.
  - ``extract_incidents`` splits INFORMATION into incidents with the
    ``extraction`` LLM role. ANALYSIS never becomes incidents (it re-tells
    past ones). The model returns day/month only; ``incident_date`` adds the
    year from ``published_at``.

The serial is opaque: nothing is derived from its parts.
"""

import html
import re
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from html.parser import HTMLParser
from typing import Literal

from pydantic import BaseModel, Field

from clear_pipeline.providers.classify import (
    SignalClassification,
    code_to_level1_map,
    code_to_level2_map,
    code_to_level3_map,
)
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.prompts.event_classification import (
    SHARED_EVENT_CLASSIFICATION_PROMPT,
    SHARED_PROMPT_VERSION,
)
from clear_pipeline.providers.signal import enrich_with_geoparser

__all__ = [
    "Country",
    "Incident",
    "InsoParseError",
    "InsoReport",
    "build_inso_signal_inputs",
    "classify_incident",
    "extract_incidents",
    "parse",
    "published_at",
]

_LABELS = ("LOCATION", "INCIDENT TYPE", "DATE & TIME", "INFORMATION", "ANALYSIS", "ADVICE", "ISSUED BY", "SERIAL #")
# The serial label may carry the issuer's name ("INSO SERIAL #").
_LABEL_LINE = re.compile(
    r"^(" + "|".join(r"(?:[A-Z]+ )?SERIAL #" if lbl == "SERIAL #" else re.escape(lbl) for lbl in _LABELS) + r")$"
)
# Values the report can't do without; the others may be empty but must be labelled.
_REQUIRED_VALUES = ("LOCATION", "INCIDENT TYPE", "DATE & TIME", "INFORMATION", "SERIAL #")
_BULLET = re.compile(r"^[*•\-]\s+")


class InsoParseError(ValueError):
    """The email isn't a report this parser can trust. The message is the
    quarantine reason."""


@dataclass(frozen=True)
class InsoReport:
    message_id: str | None
    sender: str
    subject: str
    published_at: str  # the email's Date header, ISO-8601 UTC
    severity_raw: str | None  # the banner under "REPORT", unmapped
    location_raw: str
    country_raw: str  # first comma token of LOCATION
    incident_type: str  # verbatim, never split on commas
    report_datetime: str  # DATE & TIME, strict DD/MM/YYYY HH:MM, no timezone stated
    serial: str  # opaque
    issued_by: str
    information: str
    analysis: str
    advice: list[str]


class _HtmlText(HTMLParser):
    """HTML → lines: block elements break lines, paragraphs leave a blank line
    (as in the plain part), list items become `* ` bullets."""

    _BLOCKS = {"div", "td", "tr", "table", "tbody", "ul", "ol", "br", "h1", "h2", "h3", "h4"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "li":
            self.out.append("\n* ")
        elif tag == "p" or tag in self._BLOCKS:
            self.out.append("\n")

    def handle_endtag(self, tag):
        if tag == "p":
            self.out.append("\n\n")
        elif tag in self._BLOCKS or tag == "li":
            self.out.append("\n")

    def handle_data(self, data):
        self.out.append(data)


def _body_lines(msg) -> list[str]:
    part = msg.get_body(preferencelist=("html", "plain"))
    if part is None:
        raise InsoParseError("no text/html or text/plain part")
    try:
        text = part.get_content()
    except LookupError as e:  # unknown or non-text charset; bad bytes are already replaced
        raise InsoParseError(f"undecodable body: {e}") from e
    if part.get_content_type() == "text/html":
        parser = _HtmlText()
        parser.feed(text)
        text = "".join(parser.out)
    else:
        text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\xa0", " ")
    return [" ".join(line.split()) for line in text.split("\n")]


def _clean(lines: list[str]) -> str:
    """Lines → text: paragraphs separated by one blank line, no edge blanks."""
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _sections(lines: list[str]) -> tuple[list[str], dict[str, list[str]]]:
    preamble: list[str] = []
    sections: dict[str, list[str]] = {}
    current: list[str] = preamble
    for line in lines:
        match = _LABEL_LINE.fullmatch(line)
        if match:
            label = "SERIAL #" if match.group(1).endswith("SERIAL #") else match.group(1)
            if label in sections:
                raise InsoParseError(f"label {label} appears twice")
            current = sections[label] = []
        else:
            current.append(line)
    missing = [label for label in _LABELS if label not in sections]
    if missing:
        raise InsoParseError(f"missing label(s): {', '.join(missing)}")
    return preamble, sections


def _severity_banner(preamble: list[str]) -> str | None:
    lines = [line for line in preamble if line]
    if "REPORT" in lines:
        after = lines[lines.index("REPORT") + 1:]
        return after[0] if after else None
    return None


def _advice(lines: list[str]) -> list[str]:
    """Bullet items, continuation lines joined; the intro sentence is dropped.
    Without bullets, one item per paragraph."""
    items: list[str] = []
    in_item = False
    for line in lines:
        if _BULLET.match(line):
            items.append(_BULLET.sub("", line))
            in_item = True
        elif line and in_item:
            items[-1] = f"{items[-1]} {line}"
        elif not line:
            in_item = False
    if items:
        return [item.strip() for item in items]
    return [p for p in _clean(lines).split("\n\n") if p]


def published_at(raw: bytes) -> str:
    """The email's Date header as ISO-8601 UTC. Raises ``InsoParseError``
    when it's missing, malformed or has no timezone."""
    return _published_at(BytesParser(policy=policy.default).parsebytes(raw, headersonly=True))


def _published_at(msg) -> str:
    raw = msg.get("Date")
    if raw is None:
        raise InsoParseError("no Date header")
    try:
        sent = parsedate_to_datetime(str(raw))
    except (TypeError, ValueError) as e:
        raise InsoParseError(f"malformed Date header {str(raw)!r}") from e
    if sent.tzinfo is None:
        raise InsoParseError(f"Date header {str(raw)!r} has no timezone")
    return sent.astimezone(UTC).isoformat()


def parse(raw: bytes) -> InsoReport:
    """Parse one raw email into its report-level fields. Raises
    ``InsoParseError`` on anything it can't parse unambiguously."""
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    addresses = [addr.lower() for _name, addr in getaddresses([str(h) for h in msg.get_all("From", [])])]
    if len(addresses) != 1:
        raise InsoParseError(f"expected exactly one From address, got {addresses!r}")
    sent_at = _published_at(msg)

    preamble, sections = _sections(_body_lines(msg))
    values = {label: _clean(lines) for label, lines in sections.items()}
    # One line; anything after it is the legal footer.
    values["SERIAL #"] = values["SERIAL #"].split("\n", 1)[0].strip()
    empty = [label for label in _REQUIRED_VALUES if not values[label]]
    if empty:
        raise InsoParseError(f"empty value for {', '.join(empty)}")

    try:
        report_datetime = datetime.strptime(values["DATE & TIME"], "%d/%m/%Y %H:%M")
    except ValueError as e:
        raise InsoParseError(f"DATE & TIME {values['DATE & TIME']!r} is not DD/MM/YYYY HH:MM") from e

    message_id = msg.get("Message-ID")
    return InsoReport(
        message_id=str(message_id).strip() if message_id else None,
        sender=addresses[0],
        subject=str(msg.get("Subject", "")).strip(),
        published_at=sent_at,
        severity_raw=_severity_banner(preamble),
        location_raw=values["LOCATION"],
        country_raw=values["LOCATION"].split(",", 1)[0].strip(),
        incident_type=values["INCIDENT TYPE"],
        report_datetime=report_datetime.isoformat(),
        serial=values["SERIAL #"],
        issued_by=values["ISSUED BY"],
        information=values["INFORMATION"],
        analysis=values["ANALYSIS"],
        advice=_advice(sections["ADVICE"]),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Silver: INFORMATION → incidents (LLM, extraction role)
# ──────────────────────────────────────────────────────────────────────────────
class Incident(BaseModel):
    """One incident as the model reads it. Day and month only: the year is
    added by code (``incident_date``), never guessed by the model."""

    day: int | None = Field(None, description="Day of month the incident happened; null if not stated.")
    month: int | None = Field(None, description="Month (1-12) the incident happened; null if not stated.")
    admin1: str | None = Field(None, description="State / province, as written; null if not stated.")
    locality: str | None = Field(None, description="Locality (district / admin-2), as written; null if not stated.")
    place: str | None = Field(None, description="Most precise place (town, village, site), as written.")
    # Named as in the shared classification prompt, where the fields overlap.
    event_actors: list[str] = Field(default_factory=list, description="Perpetrators / parties, as written; UNKNOWN if unidentified.")
    killed: int | None = Field(None, ge=0, description="People killed. 0 if the text says none; null if unspecified.")
    injured: int | None = Field(None, ge=0, description="People injured. 0 if the text says none; null if unspecified.")
    source_text: str = Field(description="The exact sentence(s) of INFORMATION this incident was taken from.")


class IncidentList(BaseModel):
    incidents: list[Incident]


EXTRACTION_PROMPT_VERSION = "inso-incidents-v1"
EXTRACTION_SYSTEM_PROMPT = """You split the INFORMATION section of an NGO security report into incidents.

An incident is one event at one place and time (an attack, strike, clash, robbery, detention, ...).
Return one entry per incident described in the text, in the order they appear. Do not merge incidents
that happen at different places or dates; do not split one event into several.

For each incident:
- day, month: the date the incident happened, as stated. Never infer a date that isn't written;
  use null when the text gives none. Never return a year.
- admin1, locality, place: copied as written in the text (state, locality, most precise place); null when absent.
- event_actors: the perpetrators or parties as written (e.g. "RSF", "SAF"); ["UNKNOWN"] when the text says the
  actor is unknown or unidentified; [] when no actor is mentioned.
- killed, injured: numbers of people. 0 only when the text says there were none ("no casualties",
  "no injuries"); null when the number is not given ("an unspecified number of casualties").
- source_text: the exact sentence(s) the incident was taken from, copied verbatim.

Use only the text you are given."""

# Incidents dated more than this after the email are from the previous year
# (a report sent on 2 January about "30 December").
_YEAR_ROLLBACK_SLACK = timedelta(days=7)


def extract_incidents(report: InsoReport) -> list[Incident]:
    """Split INFORMATION into incidents (``extraction`` role). Raises on an
    LLM failure; an empty list is a parse failure (a report always has one)."""
    result = make_llm_provider("extraction").complete_structured(
        system=EXTRACTION_SYSTEM_PROMPT,
        user=f"INFORMATION:\n{report.information}",
        schema=IncidentList,
        max_tokens=4096,
        cache_key=EXTRACTION_PROMPT_VERSION,
    )
    if not result.incidents:
        raise InsoParseError("extraction found no incident in INFORMATION")
    return result.incidents


def incident_date(incident: Incident, published_at: str) -> date | None:
    """The incident's date with the year taken from ``published_at``, or the
    year before when that would put it more than a week after the email.
    None unless both day and month are stated. Raises ``InsoParseError`` on
    an impossible date (31 February)."""
    if incident.day is None or incident.month is None:
        return None
    published = datetime.fromisoformat(published_at).date()
    try:
        candidate = date(published.year, incident.month, incident.day)
        if candidate > published + _YEAR_ROLLBACK_SLACK:
            candidate = date(published.year - 1, incident.month, incident.day)
    except ValueError as e:
        raise InsoParseError(f"impossible incident date {incident.day}/{incident.month}") from e
    return candidate


def external_id(serial: str, ordinal: int) -> str:
    """``inso:<serial verbatim>:<ordinal>``, 1-based."""
    return f"inso:{serial}:{ordinal}"


@dataclass(frozen=True)
class Country:
    """The report's country, resolved to its clear-api L0 location."""

    location_id: str
    name: str
    iso2: str


def _count(value: int | None) -> str:
    return "not specified" if value is None else str(value)


def build_inso_signal_inputs(
    report: InsoReport, incidents: list[Incident], source_id: str, country: Country,
) -> list[dict]:
    """One CreateSignalInput-shaped dict per incident, no clear-api write.

    Location: the geoparser over "place, locality, admin1, country", limited to
    the report's country, without L4 promotion. Resolved → its coordinates go
    as lat/lng (clear-api resolves the point); unresolved → the signal is pinned
    to the country L0, so it still shows in country views. Severity is left to
    the classify hook."""
    inputs = []
    for ordinal, incident in enumerate(incidents, start=1):
        when = incident_date(incident, report.published_at)
        where = ", ".join(p for p in (incident.place, incident.locality, incident.admin1, country.name) if p)
        description = "\n".join([
            incident.source_text,
            f"Date: {when.isoformat() if when else 'not stated'}",
            f"Place: {where}",
            f"Actors: {', '.join(incident.event_actors) or 'not stated'}",
            f"Killed: {_count(incident.killed)}",
            f"Injured: {_count(incident.injured)}",
            f"Report: {report.incident_type} ({report.serial})",
        ])
        input_data: dict = {
            "sourceId": source_id,
            "externalId": external_id(report.serial, ordinal),
            "publishedAt": report.published_at,
            "title": _title(incident.source_text),
            "description": description,
            "severity": None,
            "rawData": {
                "ordinal": ordinal,
                "incident": {**incident.model_dump(), "date": when.isoformat() if when else None},
                "report": asdict(report),
            },
        }
        if incident.killed is not None:
            input_data["casualties"] = incident.killed
        geo = enrich_with_geoparser(
            input_data, title=where, description=None, promote=False, log_tag="inso",
            scope_country=country.iso2,
        )
        if geo is not None:
            input_data["lat"], input_data["lng"] = geo.lat, geo.lng
            input_data["pointName"] = geo.candidate
        else:
            input_data.pop("pointName", None)
            input_data["locationId"] = country.location_id
        inputs.append(input_data)
    return inputs


def _title(source_text: str, limit: int = 200) -> str:
    """First sentence of the incident's source text, capped."""
    first = re.split(r"(?<=[.!?])\s", source_text.strip(), maxsplit=1)[0]
    return first if len(first) <= limit else first[: limit - 1].rstrip() + "…"


# ──────────────────────────────────────────────────────────────────────────────
# Classify: the shared event-classification prompt, in place of Jev at gx
# classify (shadow mode: the shared drain still groups with Jev)
# ──────────────────────────────────────────────────────────────────────────────
class EventClassification(BaseModel):
    """The shared prompt's fields the mapping needs; its other fields are
    re-derived from the incident here, so they're not asked for."""

    event_type: str = Field(description="Exactly one type_level_3 label from the taxonomy.")
    relevance: Literal["RELEVANT", "NOT_RELEVANT"]
    severity: int | None = Field(None, ge=1, le=5)
    rationale: str = ""


class EventClassificationAnswer(BaseModel):
    """The prompt answers a JSON array; tool-use and JSON modes need an object
    root, so the array sits under `events`. Kept a list (not one object) so a
    split or an empty answer is visible and fails, instead of being forced into
    one object."""

    events: list[EventClassification]


CLASSIFY_PROMPT_VERSION = f"inso-classify-v1+{SHARED_PROMPT_VERSION}"
# Added before the shared prompt. INSO's core content is crime and harm to aid
# workers, which a generic prompt may discard as "ordinary crime".
INSO_CONTEXT_BLOCK = """SOURCE CONTEXT — INSO security newsletter.
The text is one incident from an NGO security report written for humanitarian organisations.
- An incident that affects humanitarian staff, assets, premises or access is RELEVANT, crime included
  (robbery of an NGO vehicle, a looted warehouse, a detained aid worker).
- Severity: null when the text gives no basis for one. Crime against humanitarian staff or assets is at least 2.
"""


# "other" is the level-3 label of both `ot` (catch-all) and `po` (political
# violence, other). The prompt routes all unclassifiable noise to it: `ot`.
_AMBIGUOUS_LEVEL3 = {"other": "ot"}


def _level3_codes() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for code, level3 in code_to_level3_map().items():
        out.setdefault(level3.strip().casefold(), []).append(code)
    return {**out, **{label: [code] for label, code in _AMBIGUOUS_LEVEL3.items()}}


def classify_incident(signal_input: dict) -> SignalClassification:
    """Classify one incident (``signal`` role). The answer must hold exactly
    one event; its level-3 type must map to exactly one taxonomy code. Either
    failing raises: the classify stage fails loudly rather than falling back
    to "other" or fanning an incident out."""
    answer = make_llm_provider("signal").complete_structured(
        system=f"{INSO_CONTEXT_BLOCK}\n{SHARED_EVENT_CLASSIFICATION_PROMPT}",
        # The "alert below" the prompt: the incident's text and its fields.
        user=signal_input["description"],
        schema=EventClassificationAnswer,
        max_tokens=1024,
        cache_key=CLASSIFY_PROMPT_VERSION,
    )
    if len(answer.events) != 1:
        raise ValueError(
            f"{signal_input['externalId']}: classification returned {len(answer.events)} events, expected 1"
        )
    event = answer.events[0]
    codes = _level3_codes().get(event.event_type.strip().casefold(), [])
    if len(codes) != 1:
        raise ValueError(
            f"{signal_input['externalId']}: level-3 type {event.event_type!r} maps to {codes or 'no'} taxonomy code(s)"
        )
    code = codes[0]
    return SignalClassification(
        disaster_types=[code],
        relevance=1.0 if event.relevance == "RELEVANT" else 0.0,
        severity=event.severity,
        summary=event.rationale or signal_input["title"],
        type_level_1=code_to_level1_map().get(code),
        type_level_2=code_to_level2_map()[code],
        type_level_3=event.event_type,
    )
