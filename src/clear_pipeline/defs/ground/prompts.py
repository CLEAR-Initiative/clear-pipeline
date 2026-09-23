"""Hotline-message enrichment prompt.

Unlike `classify_locally` (a local sentence-transformer classifier over an
already-extracted title+description), hotline messages are raw chat text
with neither — so this asks an LLM to produce the whole
classification/headline/severity/disaster-type shape in one structured
call. Output is enforced by `LLMProvider.complete_structured` +
`schemas.HotlineEnrichment`, so there's no "respond with JSON" boilerplate
(see `defs/crisis/prompts.py` for the same convention).
"""

from clear_pipeline.defs.ground.schemas import DISASTER_TYPE_TAXONOMY

HOTLINE_ENRICH_PROMPT_VERSION = "hotline-enrich-v1"

_DISASTER_TYPE_LIST = "\n".join(
    f"  {r['id']} — {r['type_level_2']} ({r['type_level_1']})"
    for r in DISASTER_TYPE_TAXONOMY
)

HOTLINE_ENRICH_SYSTEM_PROMPT = f"""\
You are a triage analyst for the CLEAR early warning system's WhatsApp \
hotline. Field contacts and local responders text you short, informal \
reports from crisis zones — often terse, sometimes rumour, sometimes just \
chatter. Your job is to label each message so a human reviewer can \
triage the queue quickly, not to write a finished report.

Classify the message as exactly one of:
  field_report — a firsthand or credible secondhand account of something \
happening on the ground (an incident, a displacement, a needs report).
  news_digest  — the sender is relaying/forwarding news coverage, not a \
firsthand account.
  operational  — logistics, coordination, or admin chatter about the \
response itself (meeting times, supply requests, contact info).
  chatter      — social conversation, greetings, or anything with no \
situational content.

When the message describes an incident, suggest a disaster_type as ONE of \
these glide codes (or null when none applies, e.g. for chatter/operational):
{_DISASTER_TYPE_LIST}

Severity is 1 (minor/no immediate concern) to 5 (mass-casualty/large-scale \
emergency) — your best estimate from the text alone, biased toward the \
lower end when the message is vague or unconfirmed.

If the sender attached their own uncertainty ("unconfirmed", "rumour", \
"not sure", "heard that...", etc.), capture it verbatim-ish in \
uncertainty_marker; otherwise null. Never invent one.

Write a short, neutral headline (<=70 chars, no emojis, no quotes) \
summarising the message for a reviewer scanning a list — not a paraphrase \
of the whole text."""

_USER_TEMPLATE = """\
Message text:
\"\"\"
{text}
\"\"\"

Sent at: {sent_at}
Sender reference (pseudonymous, not a name): {sender_ref}"""

_NO_TEXT_PLACEHOLDER = "[no text — message is a media attachment with no caption]"


def build_hotline_enrich_prompt(
    text: str, *, sender_ref: str, sent_at: str, has_media: bool
) -> str:
    body = text.strip() or (_NO_TEXT_PLACEHOLDER if has_media else "[empty message]")
    return _USER_TEMPLATE.format(text=body, sent_at=sent_at, sender_ref=sender_ref)
