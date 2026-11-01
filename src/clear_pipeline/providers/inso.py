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
    past ones). The model returns day/month only; ``incident_dates`` adds the
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
    "check_extraction",
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
    added by code (``incident_dates``), never guessed by the model."""

    start_day: int | None = Field(None, description="Day of month the incident happened or began; null if not stated.")
    start_month: int | None = Field(None, description="Month (1-12) the incident happened or began; null if not stated.")
    end_day: int | None = Field(None, description="Last day of an incident stated to span several days; else null.")
    end_month: int | None = Field(None, description="Month (1-12) of end_day, or of a stated end month; else null.")
    admin1: str | None = Field(None, description="State / province, bare name (no \"State\"); null if not stated.")
    locality: str | None = Field(None, description="Locality (district / admin-2), bare name (no \"locality\"); null if not stated.")
    place: str | None = Field(None, description="Most precise named place (town, village, camp), bare name; null if none is named.")
    event_actors: list[str] = Field(default_factory=list, description="Perpetrators / parties, as written; UNKNOWN if unidentified.")
    killed: int | None = Field(None, ge=0, description="People killed. 0 if the text says none; null if unspecified.")
    injured: int | None = Field(None, ge=0, description="People injured. 0 if the text says none; null if unspecified.")
    source_text: str = Field(description="The exact sentence(s) of INFORMATION this incident was taken from.")
    title: str = Field(description="Headline: what happened, by whom, where. No date, no connective.")
    description: str = Field(description="The incident restated so it reads on its own, from INFORMATION only.")


class IncidentList(BaseModel):
    incidents: list[Incident]


EXTRACTION_PROMPT_VERSION = "inso-incidents-v4"
EXTRACTION_SYSTEM_PROMPT = """You split the INFORMATION section of an NGO security report into incidents.
Each incident becomes its own record, read on its own, without the report around it.

<splitting>
An incident is one event at one place, on one day or over one stated period (an attack, strike, clash,
robbery, detention, a siege "between 12 and 15 October", ...). Return one entry per incident described
in the text, in the order they appear. Do not merge incidents that happen at different places or dates;
do not split one event into several.
</splitting>

<fields>
- start_day, start_month: the date the incident happened, or began, as stated. When the text names
  only a month ("in early October"), give start_month and leave start_day null.
- end_day, end_month: only for an incident the text says ran over several days, with its last day or month
  ("between 12 and 15 October" -> start 12/10, end 15/10). Always give end_month with end_day. null for a
  one-day incident and when no end is stated ("since 12 October" has no end).
- Dates: never infer one that isn't written; use null when the text gives none. Never return a year.
- admin1, locality, place: the state, the locality and the most precise named place (town, village, camp),
  each as a bare proper name: drop "State", "locality", "town", "market" and any other descriptor
  ("Khartoum", not "Khartoum State"; "Nyala", not "Nyala town"). An unnamed site ("IDP gathering point",
  "city market") is not a place: leave it to the title and description, and put the named town it is
  in or near in place. null when absent.
- event_actors: the perpetrators or parties as written (e.g. "RSF", "SAF"); ["UNKNOWN"] when the text says the
  actor is unknown or unidentified; [] when no actor is mentioned.
- killed, injured: numbers of people. 0 only when the text says there were none ("no casualties",
  "no injuries"); null when the number is not given ("an unspecified number of casualties").
- source_text: the exact sentence(s) the incident was taken from, copied verbatim.
- title, description: see <title> and <description>.
</fields>

<title>
A headline an analyst scans in a list: actor (when named) + what happened + target + most precise place.
- At most 90 characters, sentence case, no final period.
- No date, no connective ("Meanwhile", "Similarly", "On 18 October, ..."), no admin hierarchy beyond
  what locates the place ("Nyala", not "Nyala town, Nyala locality, South Darfur").
- Omit an unknown actor rather than writing "unknown actor".
Examples:
- "RSF shelling of El Fasher city market"
- "Drone strike near IDP gathering point in Tawila"
- "Armed men rob NGO vehicle at checkpoint in Omdurman"
</title>

<description>
One to three sentences that state the incident completely, so a reader who never saw the report
understands it.
- Include, when stated: the date as written, the place with its state, the actor, the target,
  the people killed/injured and the damage.
- Resolve references to the rest of INFORMATION: drop "Similarly", "Meanwhile", "the same group",
  "the attack" and name what they point to.
- Say "unspecified" for a number the text doesn't give; never write one it doesn't state.
- No analysis, assessment, advice or context that isn't about this incident.
</description>

<rules>
Use only the text inside <information>. title and description restate it; they never add a fact,
a number or a cause it doesn't state.
</rules>"""

# Incidents dated more than this after the email are from the previous year
# (a report sent on 2 January about "30 December").
_YEAR_ROLLBACK_SLACK = timedelta(days=7)


def extract_incidents(report: InsoReport) -> list[Incident]:
    """Split INFORMATION into incidents (``extraction`` role). Raises on an
    LLM failure; an empty list is a parse failure (a report always has one)."""
    result = make_llm_provider("extraction").complete_structured(
        system=EXTRACTION_SYSTEM_PROMPT,
        user=f"<information>\n{report.information}\n</information>",
        schema=IncidentList,
        max_tokens=4096,
        cache_key=EXTRACTION_PROMPT_VERSION,
    )
    if not result.incidents:
        raise InsoParseError("extraction found no incident in INFORMATION")
    return result.incidents


def _squash(text: str) -> str:
    return " ".join(text.split())


def check_extraction(report: InsoReport, incidents: list[Incident]) -> str:
    """Raise ``InsoParseError`` when an incident's ``source_text`` isn't a
    verbatim span of INFORMATION (whitespace aside): the model invented or
    rewrote it; or when its date is impossible. Runs before the answer is
    stored, so a bad one is never replayed. Returns what no incident covers,
    for review only: INFORMATION may hold context sentences that aren't
    incidents, so it doesn't fail."""
    uncovered = _squash(report.information)
    for ordinal, incident in enumerate(incidents, start=1):
        span = _squash(incident.source_text)
        if not span or span not in _squash(report.information):
            raise InsoParseError(f"incident {ordinal}: source_text is not verbatim from INFORMATION")
        incident_dates(incident, report.published_at)
        uncovered = uncovered.replace(span, " ")
    return _squash(uncovered)


DateAccuracy = Literal["DAY", "MONTH"]


@dataclass(frozen=True)
class IncidentDates:
    """Start and end with the year added. A month-only date is the 1st of
    that month at ``MONTH`` accuracy (clear-api's ``DateAccuracy``)."""

    start: date | None
    start_accuracy: DateAccuracy | None
    end: date | None
    end_accuracy: DateAccuracy | None


def _partial(day: int | None, month: int | None, year: int, which: str) -> tuple[date, DateAccuracy] | None:
    if month is None:
        if day is not None:
            raise InsoParseError(f"incident {which} day {day} has no month")
        return None
    try:
        return date(year, month, day or 1), "DAY" if day is not None else "MONTH"
    except ValueError as e:
        raise InsoParseError(f"impossible incident date {day}/{month}") from e


def _year_of(day: int | None, month: int | None, published: date, which: str) -> int:
    """The email's year, or the year before when that would put the date more
    than a week after the email."""
    dated = _partial(day, month, published.year, which)
    if dated is not None and dated[0] > published + _YEAR_ROLLBACK_SLACK:
        return published.year - 1
    return published.year


def incident_dates(incident: Incident, published_at: str) -> IncidentDates:
    """The incident's start and end, years taken from ``published_at``. The
    end's year follows the start (a 30 December - 2 January incident ends the
    year after). Raises ``InsoParseError`` on an impossible date (31
    February), a day without a month, or an end before the start."""
    published = datetime.fromisoformat(published_at).date()
    start = _partial(
        incident.start_day, incident.start_month,
        _year_of(incident.start_day, incident.start_month, published, "start"), "start",
    )
    if start is None:
        end_year = _year_of(incident.end_day, incident.end_month, published, "end")
    else:
        crosses_new_year = incident.end_month is not None and incident.end_month < start[0].month
        end_year = start[0].year + 1 if crosses_new_year else start[0].year
    end = _partial(incident.end_day, incident.end_month, end_year, "end")
    if start and end and end[0] < start[0]:
        raise InsoParseError(f"incident ends ({end[0]}) before it starts ({start[0]})")
    return IncidentDates(
        start=start[0] if start else None, start_accuracy=start[1] if start else None,
        end=end[0] if end else None, end_accuracy=end[1] if end else None,
    )


def external_id(serial: str, ordinal: int) -> str:
    """``inso:<serial verbatim>:<ordinal>``, 1-based."""
    return f"inso:{serial}:{ordinal}"


@dataclass(frozen=True)
class Country:
    """The report's country, resolved to its clear-api L0 location."""

    location_id: str
    name: str
    iso2: str


def build_inso_signal_inputs(
    report: InsoReport, incidents: list[Incident], source_id: str, country: Country,
) -> list[dict]:
    """One CreateSignalInput-shaped dict per incident, no clear-api write.

    Location: the geoparser over "place, locality, admin1, country", limited to
    the report's country, without L4 promotion. Resolved → its coordinates go
    as lat/lng (clear-api resolves the point); unresolved → the signal is pinned
    to the country L0, so it still shows in country views. Severity is left to
    the classify hook. Dates, actors and countries are only sent when known,
    as in ``event_extraction.extracted_signal_fields``."""
    inputs = []
    for ordinal, incident in enumerate(incidents, start=1):
        when = incident_dates(incident, report.published_at)
        where = ", ".join(p for p in (incident.place, incident.locality, incident.admin1, country.name) if p)
        input_data: dict = {
            "sourceId": source_id,
            "externalId": external_id(report.serial, ordinal),
            "publishedAt": report.published_at,
            # Written by the extraction model; the structured fields stay in
            # rawData and the columns, not repeated in the text.
            "title": incident.title,
            "description": incident.description,
            "severity": None,
            "rawData": {
                "ordinal": ordinal,
                "incident": {
                    **incident.model_dump(),
                    "start_date": when.start.isoformat() if when.start else None,
                    "start_date_accuracy": when.start_accuracy,
                    "end_date": when.end.isoformat() if when.end else None,
                    "end_date_accuracy": when.end_accuracy,
                },
                "report": asdict(report),
            },
            "countryIds": [country.location_id],
        }
        if when.start:
            input_data["startDate"], input_data["startDateAccuracy"] = when.start.isoformat(), when.start_accuracy
        if when.end:
            input_data["endDate"], input_data["endDateAccuracy"] = when.end.isoformat(), when.end_accuracy
        # "UNKNOWN" is the extraction's marker for an unidentified actor, not a name.
        actors = [a for a in incident.event_actors if a != "UNKNOWN"]
        if actors:
            input_data["actors"] = actors
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


# ──────────────────────────────────────────────────────────────────────────────
# Classify: the shared event-classification prompt, in place of Jev at gx
# classify (shadow mode: the shared drain still groups with Jev)
# ──────────────────────────────────────────────────────────────────────────────
class EventClassification(BaseModel):
    """The shared prompt's output: classification only. Place, date, actors
    and casualties come from extraction, before the Signal is built."""

    event_type: str = Field(description="Exactly one type_level_3 label from the taxonomy.")
    relevance: Literal["RELEVANT", "NOT_RELEVANT"]
    severity: int | None = Field(None, ge=1, le=5)
    rationale: str = ""


class EventClassificationAnswer(BaseModel):
    """Tool-use and JSON modes need an object root, so the classification
    sits under `events`. Kept a list (not one object) so a
    split or an empty answer is visible and fails, instead of being forced into
    one object."""

    events: list[EventClassification]


CLASSIFY_PROMPT_VERSION = f"inso-classify-v3+{SHARED_PROMPT_VERSION}"
# Added before the shared prompt. INSO's core content is crime and harm to aid
# workers, which a generic prompt may discard as "ordinary crime".
INSO_CONTEXT_BLOCK = """<source_context>
INSO security newsletter. The incident comes from an NGO security report written for humanitarian organisations.
- An incident that affects humanitarian staff, assets, premises or access is RELEVANT, crime included
  (robbery of an NGO vehicle, a looted warehouse, a detained aid worker).
- Crime against humanitarian staff or assets is at least severity 2.
</source_context>
"""


def _date_text(incident: dict) -> str | None:
    def one(iso: str | None, accuracy: str | None) -> str | None:
        return iso[:7] if iso and accuracy == "MONTH" else iso

    start = one(incident["start_date"], incident["start_date_accuracy"])
    end = one(incident["end_date"], incident["end_date_accuracy"])
    if start and end:
        return f"{start} to {end}"
    return start or (end and f"until {end}")


def _classify_input(signal_input: dict) -> str:
    """The incident as the model reads it: the report's verbatim sentences and
    the extracted fields, not the generated title/description (a paraphrase
    of a paraphrase would drift)."""
    incident = signal_input["rawData"]["incident"]
    fields = {
        "source_text": incident["source_text"],
        "date": _date_text(incident),
        "place": ", ".join(p for p in (incident["place"], incident["locality"], incident["admin1"]) if p) or None,
        "actors": ", ".join(incident["event_actors"]) or None,
        "killed": incident["killed"],
        "injured": incident["injured"],
    }
    lines = [f"<{k}>{'not stated' if v is None else v}</{k}>" for k, v in fields.items()]
    return "\n".join(["<incident>", *lines, "</incident>"])


# "other" is the level-3 label of both `ot` (natural hazard > other) and `po`
# (political violence > other). The prompt names them apart: "other political
# violence" is `po`, plain "other" is `ot`.
_PROMPT_LEVEL3 = {"other political violence": "po", "other": "ot"}


def _level3_codes() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for code, level3 in code_to_level3_map().items():
        out.setdefault(level3.strip().casefold(), []).append(code)
    return {**out, **{label: [code] for label, code in _PROMPT_LEVEL3.items()}}


def classify_incident(signal_input: dict) -> SignalClassification:
    """Classify one incident (``signal`` role). The answer must hold exactly
    one event; its level-3 type must map to exactly one taxonomy code. Either
    failing raises: the classify stage fails loudly rather than falling back
    to "other" or fanning an incident out."""
    answer = make_llm_provider("signal").complete_structured(
        system=f"{INSO_CONTEXT_BLOCK}\n{SHARED_EVENT_CLASSIFICATION_PROMPT}",
        user=_classify_input(signal_input),
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
        type_level_3=code_to_level3_map()[code],
    )
