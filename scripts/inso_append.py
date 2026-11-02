"""IMAP APPEND one synthetic .eml into the INSO test folder. No SMTP: the bytes
land in the folder as-is. Credentials come from the pipeline's own settings
(same keys and defaults as the poll), and are never printed. Run from the repo root:
    uv run python scripts/inso_append.py tests/golden/inso/01_multi_incident.eml [--from ADDRESS]

--from replaces the From header and drops the fixture's Authentication-Results
(it vouches for the fixture's sender, not yours). The poll then needs ADDRESS in
INSO_ALLOWED_SENDERS and INSO_REQUIRE_DKIM=false. The body is left byte-for-byte.
"""

import argparse
import imaplib
import re
import ssl
import sys
import time
from email import policy
from email.parser import BytesHeaderParser

from clear_pipeline.signals.config import settings

FOLDER = "INSO-TEST"  # hard-coded: never the real INSO folder


def _rewrite_headers(raw: bytes, sender: str) -> bytes:
    """Replace From, drop Authentication-Results; folded lines go with their header."""
    head, sep, body = re.split(rb"(\r?\n\r?\n)", raw, maxsplit=1)
    eol = b"\r\n" if sep.startswith(b"\r\n") else b"\n"
    fields: list[list[bytes]] = []
    for line in head.split(eol):
        if line[:1] in (b" ", b"\t") and fields:
            fields[-1].append(line)
        else:
            fields.append([line])
    out = []
    for field in fields:
        name = field[0].split(b":", 1)[0].strip().lower()
        if name == b"authentication-results":
            continue
        out.append([f"From: {sender}".encode()] if name == b"from" else field)
    return eol.join(line for field in out for line in field) + sep + body


parser = argparse.ArgumentParser()
parser.add_argument("eml")
parser.add_argument("--from", dest="sender", help="replace the From header with this address")
args = parser.parse_args()

raw = open(args.eml, "rb").read()

# Only synthetic fixtures may go in (terms of use, pre-approval).
subject = str(BytesHeaderParser(policy=policy.default).parsebytes(raw)["Subject"] or "")
if "[SYNTHETIC]" not in subject:
    sys.exit(f"refusing: {args.eml} has no [SYNTHETIC] subject ({subject!r})")

if args.sender:
    raw = _rewrite_headers(raw, args.sender)

if not (settings.clear_mailbox_imap_user and settings.clear_mailbox_imap_password):
    sys.exit("mailbox credentials not configured (CLEAR_MAILBOX_IMAP_USER / _PASSWORD)")

# Without an explicit context, imaplib skips certificate verification.
m = imaplib.IMAP4_SSL(
    settings.clear_mailbox_imap_host, settings.clear_mailbox_imap_port,
    ssl_context=ssl.create_default_context(),
)
try:
    m.login(settings.clear_mailbox_imap_user, settings.clear_mailbox_imap_password)
    # Fail rather than let the server auto-create a misspelt folder.
    status, _ = m.select(f'"{FOLDER}"', readonly=True)
    if status != "OK":
        sys.exit(f"folder {FOLDER} not found")
    m.close()
    status, data = m.append(f'"{FOLDER}"', None, imaplib.Time2Internaldate(time.time()), raw)
    print(status, data)  # servers with UIDPLUS answer [APPENDUID <uidvalidity> <uid>]
finally:
    m.logout()
