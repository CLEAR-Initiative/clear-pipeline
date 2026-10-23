"""Generate the SYNTHETIC INSO newsletter fixtures (``*.eml``).

Mirrors the one real sample's structure (multipart/alternative, base64 utf-8
parts, label/value HTML table) as rebuilt in datasource-analysis/inso. Every
incident is invented; every file says SYNTHETIC in subject, banner and footer.
The golden ``*.report.json`` / ``*.incidents.json`` are written by hand, not
here: they are the independent expectation the tests compare against.

Usage: uv run python tests/golden/inso/make_fixtures.py
"""

import html
from email.message import EmailMessage
from email.utils import format_datetime
from datetime import UTC, datetime
from pathlib import Path

OUT = Path(__file__).parent
SENDER = "SECURITY_REPORTS <reports@security-reports.example>"
RECIPIENT = "ingest@clear-pipeline.example"
AUTH_PASS = (
    "mx.mailbox.example; dkim=pass header.d=security-reports.example header.s=s1; "
    "dmarc=pass (p=reject) header.from=security-reports.example; spf=pass"
)
BANNER = "SYNTHETIC TEST FIXTURE - not a real security report"
FOOTER = f"This service provides security information to the NGO community. [{BANNER}]"

MULTI = {
    "date": datetime(2026, 10, 20, 9, 30, tzinfo=UTC),
    "subject_place": "SUDAN –DARFUR",
    "severity": "ATTENTION RECOMMENDED",
    "location": "Republic of Sudan, Darfur",
    "incident_type": "Conflict Impact Civilians, Risking Humanitarian Operations",
    "report_datetime": "20/10/2026 09:30",
    "information": [
        "On 18 October, in North Darfur, the RSF conducted artillery shelling on a market in El Fasher city, El Fasher locality. This resulted in four fatalities and nine injuries among civilians.",
        "Similarly, on 19 October in Tawila, North Darfur, an unknown actor conducted a drone strike near an IDP gathering point, resulting in an unspecified number of civilian casualties.",
        "Meanwhile, in South Darfur, on 19 October, SAF's drone strike hit a fuel depot in Nyala town, Nyala locality, damaging two trucks. No casualties were reported.",
    ],
    "analysis": [
        "Mid-October confirmed the trend of strikes on civilian infrastructure. On 02 October, an RSF drone strike on Kutum hit a water point, killing three civilians.",
        "Movements along the El Fasher - Tawila corridor are likely to remain exposed to indirect fire.",
    ],
    "advice": [
        "NGOs should review shelter-in-place procedures for staff in El Fasher and Tawila.",
        "NGOs should avoid gatherings near markets and fuel depots until the situation stabilises.",
    ],
    "issued_by": "Sudan Security Team",
    "serial": "SDN/Darfur/IR/20262010/R1",
    "serial_label": "INSO SERIAL #",
}

SINGLE = {
    **MULTI,
    "date": datetime(2026, 10, 14, 16, 5, tzinfo=UTC),
    "subject_place": "SUDAN –KHARTOUM",
    "location": "Republic of Sudan, Khartoum",
    "incident_type": "Crime",
    "report_datetime": "14/10/2026 16:05",
    "information": [
        "On 14 October, in Omdurman, Khartoum State, armed men stopped an NGO vehicle at an informal checkpoint and stole two phones and cash. No injuries were reported.",
    ],
    "analysis": ["Informal checkpoints have increased on secondary roads in Omdurman."],
    "advice": ["NGOs should use primary roads and travel in convoys of at least two vehicles."],
    "serial": "SDN/Khartoum/IR/20261410/R1",
    "serial_label": "SERIAL #",
}

NEW_YEAR = {
    **SINGLE,
    "date": datetime(2027, 1, 2, 8, 0, tzinfo=UTC),
    "location": "Republic of Sudan, Kassala",
    "subject_place": "SUDAN –KASSALA",
    "incident_type": "Crime, Risking Humanitarian Operations",
    "report_datetime": "02/01/2027 08:00",
    "information": [
        "On 30 December, in Kassala town, Kassala State, unidentified gunmen robbed an NGO guesthouse and assaulted a guard, who was injured.",
        "On 1 January, near Wad Sharifey camp, Kassala locality, a local NGO staff member was briefly detained by armed men.",
    ],
    "analysis": ["Criminality around Kassala town typically rises over the holiday period."],
    "advice": ["NGOs should reinforce guard shifts at guesthouses."],
    "serial": "SDN/Kassala/IR/20270201/R1",
}

HTML_ONLY = {**SINGLE, "html_only": True, "serial": "SDN/Khartoum/IR/20261410/R2"}
MISSING_SERIAL = {**SINGLE, "serial": None}
UNKNOWN_COUNTRY = {
    **SINGLE, "location": "Republic of Chad, Ouaddai", "subject_place": "CHAD –OUADDAI",
    "serial": "TCD/Ouaddai/IR/20261410/R1",
}
FORGED_SENDER = {**SINGLE, "auth": None, "serial": "SDN/Khartoum/IR/20261410/R9"}

FIXTURES = {
    "01_multi_incident": MULTI,
    "02_single_incident": SINGLE,
    "03_new_year": NEW_YEAR,
    "04_html_only": HTML_ONLY,
    "05_missing_serial": MISSING_SERIAL,
    "06_unknown_country": UNKNOWN_COUNTRY,
    "07_forged_sender": FORGED_SENDER,
}


def _fields(r: dict) -> list[tuple[str, str | list[str] | None]]:
    fields: list[tuple[str, str | list[str] | None]] = [
        ("LOCATION", r["location"]),
        ("INCIDENT TYPE", r["incident_type"]),
        ("DATE & TIME", r["report_datetime"]),
        ("INFORMATION", r["information"]),
        ("ANALYSIS", r["analysis"]),
        ("ADVICE", r["advice"]),
        ("ISSUED BY", r["issued_by"]),
    ]
    if r["serial"] is not None:
        fields.append((r["serial_label"], r["serial"]))
    return fields


def _plain(r: dict) -> str:
    lines = [f"{BANNER}. This report is for the exclusive use of partners and must not be shared outside your organisation",
             "[https://security-reports.example/assets/img/report.png]", "  REPORT", f"     {r['severity']}"]
    for label, value in _fields(r):
        lines.append(label)
        if label == "ADVICE":
            lines += ["Based on the above, the following is recommended:", ""]
            lines += [f"  *   {a}" for a in value]
        elif isinstance(value, list):
            lines.append("\n\n".join(value))
        else:
            lines.append(value)
    lines.append(FOOTER)
    return "\n".join(lines) + "\n"


def _html(r: dict) -> str:
    rows = []
    for label, value in _fields(r):
        if label == "ADVICE":
            items = "".join(f"<li>{html.escape(a)}&nbsp;</li>" for a in value)
            cell = f"<p><span>Based on the above, the following is recommended: &nbsp;</span></p><ul>{items}</ul>"
        elif isinstance(value, list):
            cell = "".join(f"<p><span>{html.escape(p)}&nbsp;</span></p>" for p in value)
        else:
            cell = html.escape(value)
        rows.append(f"<tr><td><div><b>{html.escape(label)}</b></div></td><td><div>{cell}</div></td></tr>")
    return (
        "<html><body><table><tbody>\n"
        f'<tr><td style="background-color: rgb(83, 118, 182); color: white;"><div>{BANNER}. This report is for the exclusive use of partners</div></td></tr>\n'
        '<tr><td><table cellspacing="0" cellpadding="0"><tbody><tr>\n'
        '<td><img src="https://security-reports.example/assets/img/report.png"></td>\n'
        f"<td><div><b>&nbsp;&nbsp;REPORT</b></div><div><b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{html.escape(r['severity'])}</b></div></td>\n"
        "</tr></tbody></table></td></tr>\n"
        f'<tr><td><table cellspacing="0" cellpadding="0"><tbody>{"".join(rows)}</tbody></table></td></tr>\n'
        f"<tr><td><div>{FOOTER}</div></td></tr>\n"
        "</tbody></table></body></html>\n"
    )


def build(name: str, r: dict) -> bytes:
    msg = EmailMessage()
    if (auth := r.get("auth", AUTH_PASS)) is not None:
        msg["Authentication-Results"] = auth
    msg["From"] = SENDER
    msg["To"] = RECIPIENT
    msg["Subject"] = f"[SYNTHETIC] SECURITY REPORT – {r['subject_place']}: {r['incident_type']}"
    msg["Date"] = format_datetime(r["date"])
    msg["Message-ID"] = f"<synthetic-{name}@security-reports.example>"
    if r.get("html_only"):
        msg.set_content(_html(r), subtype="html", cte="base64")
    else:
        msg.set_content(_plain(r), cte="base64")
        msg.add_alternative(_html(r), subtype="html", cte="base64")
    return msg.as_bytes()


if __name__ == "__main__":
    for name, report in FIXTURES.items():
        (OUT / f"{name}.eml").write_bytes(build(name, report))
        print(f"wrote {name}.eml")
