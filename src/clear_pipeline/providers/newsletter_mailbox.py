"""Shared newsletter mailbox — IMAP over implicit TLS, read-only.

Newsletters that only reach us by email (INSO today) land in one shared
mailbox, one folder per source. This module is the transport only: it lists a
folder's messages after a cursor and returns their raw bytes. Parsing lives in
each source's provider and never imports this module.

Deliberately not an abstract interface: IMAP is the only transport. A move to
Microsoft Graph / an Outlook mailbox would replace this module, not add a
second implementation behind it.

Never changes mailbox state: the folder is opened with EXAMINE (read-only) and
bodies are fetched with ``BODY.PEEK[]``, so no ``\\Seen`` flag is set and a
person reading the newsletter in webmail can't make the pipeline skip it.

Sender trust (``sender_rejection``) is here rather than in a source provider:
it reads only the envelope headers, and every newsletter on the mailbox needs it.
"""

import imaplib
import re
from dataclasses import dataclass
from email import policy
from email.parser import BytesHeaderParser
from email.utils import getaddresses
from typing import cast

import redis

from clear_pipeline.signals.config import settings

__all__ = [
    "FolderBatch",
    "authentication_results",
    "commit_cursor",
    "fetch_after",
    "get_cursor",
    "sender_rejection",
    "stage_cursor",
]

_redis = redis.from_url(settings.redis_url, decode_responses=True)

_FETCHED_UID = re.compile(rb"UID (\d+)")
_COMMENT = re.compile(r"\([^)]*\)")


@dataclass(frozen=True)
class FolderBatch:
    """Messages after the cursor, oldest first, and the folder's UIDVALIDITY
    (the cursor to store is ``(uidvalidity, max uid)``)."""

    uidvalidity: int
    messages: list[tuple[int, bytes]]


def _check(status: str, data: list, what: str) -> list:
    if status != "OK":
        raise RuntimeError(f"IMAP {what} failed: {status} {data!r}")
    return data


def _quote(folder: str) -> str:
    return '"' + folder.replace("\\", "\\\\").replace('"', '\\"') + '"'


def fetch_after(folder: str, uidvalidity: int | None, last_uid: int) -> FolderBatch:
    """Every message in ``folder`` with a UID above ``last_uid``. When the
    folder's UIDVALIDITY differs from ``uidvalidity`` (folder recreated or
    migrated, or no cursor yet) its old UIDs mean nothing: the whole folder is
    returned, and the caller dedups on its own ids.

    Raises when the mailbox credentials are empty, so an environment without
    them can't poll by accident, and on any IMAP failure."""
    host = settings.newsletter_mailbox_imap_host
    user = settings.newsletter_mailbox_imap_user
    password = settings.newsletter_mailbox_imap_password
    if not (host and user and password):
        raise RuntimeError("Newsletter mailbox credentials are not configured (NEWSLETTER_MAILBOX_IMAP_*)")

    with imaplib.IMAP4_SSL(host, settings.newsletter_mailbox_imap_port) as imap:
        imap.login(user, password)
        _check(*imap.select(_quote(folder), readonly=True), what=f"EXAMINE {folder}")
        # The untagged `OK [UIDVALIDITY n]` EXAMINE answered with.
        _name, validity = imap.response("UIDVALIDITY")
        if not validity or validity[0] is None:
            raise RuntimeError(f"IMAP folder {folder} reported no UIDVALIDITY")
        current = int(validity[-1])
        after = last_uid if current == uidvalidity else 0

        (found,) = _check(*imap.uid("SEARCH", f"UID {after + 1}:*"), what="UID SEARCH")
        # `n:*` always matches the highest UID, even below n: filter again.
        uids = sorted(uid for uid in map(int, found.split()) if uid > after)

        messages: list[tuple[int, bytes]] = []
        for uid in uids:
            data = _check(*imap.uid("FETCH", str(uid), "(UID BODY.PEEK[])"), what=f"UID FETCH {uid}")
            parts = [p for p in data if isinstance(p, tuple)]
            if len(parts) != 1 or not (m := _FETCHED_UID.search(parts[0][0])) or int(m.group(1)) != uid:
                raise RuntimeError(f"IMAP UID FETCH {uid} returned an unexpected response")
            messages.append((uid, parts[0][1]))
    return FolderBatch(uidvalidity=current, messages=messages)


def get_cursor(source: str) -> tuple[int | None, int]:
    """``(uidvalidity, last uid)`` stored under ``<source>:last_synced`` as
    ``UIDVALIDITY:UID``; ``(None, 0)`` before the first clean batch."""
    value = cast("str | None", _redis.get(f"{source}:last_synced"))
    if not value:
        return None, 0
    validity, uid = value.split(":")
    return int(validity), int(uid)


def stage_cursor(source: str, cursor: tuple[int, int] | None) -> None:
    """Hold the cursor a fetched batch would advance to, without advancing it
    (``None`` clears it). Staged in Redis, not in memory: the step that commits
    it may run in another process than the one that fetched."""
    if cursor is None:
        _redis.delete(f"{source}:pending_cursor")
    else:
        _redis.set(f"{source}:pending_cursor", f"{cursor[0]}:{cursor[1]}")


def commit_cursor(source: str) -> None:
    """Advance the cursor to the staged one, once the batch went all the way
    through. Nothing staged → unchanged."""
    pending = cast("str | None", _redis.get(f"{source}:pending_cursor"))
    if pending:
        _redis.set(f"{source}:last_synced", pending)
        _redis.delete(f"{source}:pending_cursor")


# ──────────────────────────────────────────────────────────────────────────────
# Sender trust
# ──────────────────────────────────────────────────────────────────────────────
def _aligned(from_domain: str, domain: str) -> bool:
    domain = domain.lower().rstrip(".")
    return from_domain == domain or from_domain.endswith(f".{domain}")


def _auth_passes(auth_results: str, from_domain: str) -> bool:
    """DMARC pass for the From domain, or a DKIM pass signed by the From domain
    (or a parent). A bare DKIM pass is not enough: anyone can DKIM-sign their
    own domain and put ours in From."""
    _authserv_id, *results = _COMMENT.sub("", auth_results).split(";")
    for result in results:
        method = re.match(r"\s*([\w-]+)\s*=\s*([\w-]+)", result)
        if not method or method.group(2).lower() != "pass":
            continue
        props = {k.lower(): v for k, v in re.findall(r"([\w-]+\.[\w-]+)\s*=\s*([^\s;]+)", result)}
        kind = method.group(1).lower()
        if kind == "dmarc" and _aligned(from_domain, props.get("header.from", from_domain)):
            return True
        if kind == "dkim" and "header.d" in props and _aligned(from_domain, props["header.d"]):
            return True
    return False


def authentication_results(raw: bytes) -> str | None:
    """The topmost ``Authentication-Results`` header: the receiving server
    prepends its own, so any copy below it came with the message and may be
    forged."""
    results = BytesHeaderParser(policy=policy.default).parsebytes(raw).get_all("Authentication-Results", [])
    return " ".join(str(results[0]).split()) if results else None


def sender_rejection(raw: bytes, allowed_senders: set[str], *, require_auth: bool) -> str | None:
    """Why this message can't be trusted, or None when it can: exactly one From
    address, on ``allowed_senders`` (lowercased), and — unless ``require_auth``
    is off — a passing DKIM/DMARC result in ``authentication_results``."""
    headers = BytesHeaderParser(policy=policy.default).parsebytes(raw)
    addresses = [addr.lower() for _name, addr in getaddresses([str(h) for h in headers.get_all("From", [])])]
    if len(addresses) != 1 or "@" not in addresses[0]:
        return f"expected exactly one From address, got {addresses!r}"
    sender = addresses[0]
    if sender not in allowed_senders:
        return f"sender {sender} is not allowlisted"
    if not require_auth:
        return None
    auth_results = authentication_results(raw)
    if auth_results is None:
        return "no Authentication-Results header"
    if not _auth_passes(auth_results, sender.rsplit("@", 1)[1]):
        return "no aligned DKIM or DMARC pass in Authentication-Results"
    return None
