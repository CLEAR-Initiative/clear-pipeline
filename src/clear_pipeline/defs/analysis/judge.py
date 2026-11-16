"""Adversarial evaluation judge — ported from the ``clear_eval`` framework.

The standalone ``clear_eval`` project grades spreadsheet-exported analyses: an
independent "expert humanitarian analyst" LLM scores each answer against its
framework question (complete / partial / not_answered) and, crucially, flags
**off-subject padding** (``out_of_subject``) — content that looks like an answer
but doesn't address the question. That adversarial on-topic check is the whole
point: a generator's own ``completeness`` self-rating can't catch its own
padding, so the deterministic self-rating filter alone can't either.

This module ports that judge into the pipeline. The input here is not a
spreadsheet row but a taxonomy node's descriptive artifact; each node already
carries its **Analytical Question** (``node.question``), so the judge grades the
node's ``findings`` against that question with the same rubric, the same
verdict schema, and the same consistency guards.

Design decisions (confirmed):
  - **Judge model**: a dedicated ``analysis_judge`` role (DeepSeek v3.2 via
    OpenRouter with reasoning ON + a raised max_tokens) — see ``assets_gaps``.
  - **Judge is primary**: for a judged node the judge verdict decides kept vs
    flagged, overriding the self-rating floor (``filter_package`` consumes these
    verdicts). Web-rescue still overrides on top.
  - **Scope**: only the flagged / near-floor band is judged (``select_nodes_to_judge``);
    clearly-strong nodes stay kept cheaply, empty nodes stay flagged cheaply.

Pure except :func:`judge_node`, which makes the one LLM call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

# Judge prompt/rubric version — bump when JUDGE_SYSTEM_PROMPT changes enough that
# verdicts are no longer comparable. Stored with each verdict (with the model) so
# a change in which sections are kept/dropped can be explained.
JUDGE_PROMPT_VERSION = "v1"

# complete/partial/not_answered → coverage weight (not_evaluable excluded).
WEIGHTS: dict[str, float] = {"complete": 1.0, "partial": 0.5, "not_answered": 0.0}

_VALID_SCORES = {"complete", "partial", "not_answered"}
_VALID_SHARES = {"none", "some", "mostly", "entirely", "no_content"}

# Shares at which the on-topic content is too diluted to trust the node.
_OFF_SUBJECT_FLAG_SHARES = {"mostly", "entirely"}

_SCORE_ALIASES = {
    "c": "complete", "p": "partial", "n": "not_answered",
    "not answered": "not_answered", "not-answered": "not_answered",
}
_SHARE_ALIASES = {
    "all": "entirely", "full": "entirely", "fully": "entirely",
    "partial": "some", "partly": "some", "most": "mostly",
    "empty": "no_content", "none_present": "no_content", "n/a": "no_content", "na": "no_content",
}

_FINDINGS_MAX_CHARS = 24_000


class JudgeLLM(BaseModel):
    """Judge output schema (mirrors ``clear_eval``'s payload). ``score`` and
    ``out_of_subject_share`` are plain strings (not Literals) so a drifting cheap
    model's near-miss is repaired by :func:`normalise_verdict`'s aliases rather
    than hard-failing schema validation."""

    score: str = Field(description="complete | partial | not_answered")
    has_content: bool = Field(default=True)
    out_of_subject: bool = Field(default=False)
    out_of_subject_share: str = Field(default="none", description="none|some|mostly|entirely|no_content")
    notes: str = Field(default="")


@dataclass(frozen=True)
class JudgeVerdict:
    """Normalised, guard-applied judge verdict for one node."""

    node_key: str
    score: str                 # complete | partial | not_answered
    has_content: bool
    out_of_subject: bool
    out_of_subject_share: str
    notes: str
    kept: bool
    reason: str                # judge_complete | judge_partial | judge_not_answered | judge_off_subject | no_evidence

    def as_metadata(self) -> dict[str, Any]:
        """Compact view attached to the node's ``NodeVerdict`` for publish."""
        return {
            "score": self.score,
            "out_of_subject": self.out_of_subject,
            "out_of_subject_share": self.out_of_subject_share,
            "notes": self.notes,
        }


JUDGE_SYSTEM_PROMPT = """You are an expert humanitarian analyst evaluating one section of a CLEAR \
situation analysis against its assigned analytical question.

Score ONLY from the provided findings. Do not invent content that is not present.

Allowed scores (exactly one):
- complete: comprehensively addresses the question's core dimensions with specific, source-grounded content
- partial: addresses the question but one or more core dimensions are missing, vague, or insufficiently disaggregated
- not_answered: no usable answer for the question's core dimensions

Also judge whether the findings are on-topic for THIS question:
- has_content: true if the findings contain any substantive text (not empty / placeholder)
- out_of_subject: true if SOME or ALL of the findings do NOT address the question \
(wrong topic, wrong pillar/sector, tangential material that fails to answer the question)
- out_of_subject_share: how much of the content is off-topic relative to the question:
  - none: all substantive content is on-topic (even if incomplete)
  - some: mix of on-topic and off-topic material
  - mostly: most content is off-topic; little answers the question
  - entirely: content exists but none of it answers the question
  - no_content: findings are empty / non-substantive

Important distinctions:
- Incomplete but on-topic -> score partial or not_answered; out_of_subject=false; out_of_subject_share=none
- Content present but wrong topic -> score not_answered; out_of_subject=true; out_of_subject_share=entirely (or mostly)
- Mix of relevant and irrelevant bullets -> out_of_subject=true; out_of_subject_share=some or mostly

Return a JSON object with keys: score, has_content, out_of_subject, out_of_subject_share, notes \
(brief justification; name the main gaps; if out_of_subject, say what is off-topic).
"""


def build_judge_user_prompt(*, node_name: str, task: str, question: str, definition: str, findings_text: str) -> str:
    content_block = findings_text.strip() or "(no substantive findings)"
    definition_block = f"\nSection definition: {definition.strip()}\n" if definition.strip() else ""
    return (
        f"Section: {node_name}\n"
        f"Task: {task}\n"
        f"{definition_block}"
        f"\nAnalytical question:\n{question}\n"
        f"\nFindings to evaluate:\n\"\"\"\n{content_block}\n\"\"\"\n"
        "\nRespond with JSON only.\n"
    )


def format_findings(findings: list[dict] | None) -> str:
    """Render a node artifact's ``descriptive.findings`` (``{description, ...}``
    bullets) as the text the judge grades, capped for context budget."""
    if not findings:
        return ""
    lines: list[str] = []
    for f in findings:
        text = (f.get("description") or f.get("text") or "").strip()
        if text:
            lines.append(f"- {text}")
    return "\n".join(lines)[:_FINDINGS_MAX_CHARS]


def _norm(value: Any, aliases: dict[str, str]) -> str:
    s = str(value).strip().lower().replace(" ", "_") if value is not None else ""
    return aliases.get(s, s)


def normalise_verdict(node_key: str, raw: JudgeLLM) -> JudgeVerdict:
    """Apply ``clear_eval``'s consistency guards and derive kept/flagged.

    Judge is primary: complete/partial on-topic -> kept; not_answered or
    mostly/entirely off-subject -> flagged. An "entirely" off-subject
    ``complete`` is downgraded to ``not_answered`` (the source's guard)."""
    score = _norm(raw.score, _SCORE_ALIASES)
    if score not in _VALID_SCORES:
        score = "not_answered"
    has_content = bool(raw.has_content)
    out_of_subject = bool(raw.out_of_subject)
    share = _norm(raw.out_of_subject_share, _SHARE_ALIASES)
    if share not in _VALID_SHARES:
        share = "no_content" if not has_content else ("some" if out_of_subject else "none")

    # Consistency guards (clear_eval.client.parse_score_payload).
    if not has_content:
        out_of_subject, share, score = False, "no_content", "not_answered"
    if share == "entirely":
        out_of_subject = True
        if score == "complete":
            score = "not_answered"
    if share == "none":
        out_of_subject = False

    notes = (raw.notes or "").strip()

    # Kept/flagged — judge is primary.
    if not has_content:
        kept, reason = False, "no_evidence"
    elif share in _OFF_SUBJECT_FLAG_SHARES:
        kept, reason = False, "judge_off_subject"
    elif score == "not_answered":
        kept, reason = False, "judge_not_answered"
    elif score == "complete":
        kept, reason = True, "judge_complete"
    else:  # partial, on-topic
        kept, reason = True, "judge_partial"

    return JudgeVerdict(
        node_key=node_key, score=score, has_content=has_content,
        out_of_subject=out_of_subject, out_of_subject_share=share, notes=notes,
        kept=kept, reason=reason,
    )


def select_nodes_to_judge(
    nodes: list,
    artifacts: dict[str, dict],
    *,
    floor: float,
    band: float,
    max_nodes: int | None = None,
) -> list[str]:
    """Nodes worth a judge pass: those with content but a *self-rated*
    completeness in the uncertain zone — below the floor (candidate rescue /
    confirm) or within ``band`` above it (candidate padding catch). Clearly-strong
    nodes (completeness >= floor + band) are trusted and skipped; empty nodes
    (no findings) are left to the deterministic filter. Worst-rated first."""
    ceiling = floor + band
    candidates: list[tuple[float, str]] = []
    for node in nodes:
        art = artifacts.get(node.key)
        desc = (art or {}).get("descriptive", {}) if art else {}
        if not desc.get("findings"):
            continue  # nothing to judge; filter marks no_evidence/not_generated
        completeness = desc.get("completeness")
        c = -1.0 if completeness is None else float(completeness)
        if completeness is None or c < ceiling:
            candidates.append((c, node.key))
    candidates.sort(key=lambda ck: ck[0])  # worst-rated (and None) first
    keys = [k for _c, k in candidates]
    return keys if max_nodes is None else keys[:max_nodes]


def judge_node(
    *,
    node,
    findings_text: str,
    provider,
    max_tokens: int,
) -> JudgeVerdict:
    """Run the adversarial judge over one node's findings. Charge the budget in
    the caller (via :func:`budget.charge`) after this returns."""
    raw: JudgeLLM = provider.complete_structured(
        system=JUDGE_SYSTEM_PROMPT,
        user=build_judge_user_prompt(
            node_name=node.name, task=node.task, question=node.question,
            definition=node.definition, findings_text=findings_text,
        ),
        schema=JudgeLLM,
        max_tokens=max_tokens,
    )
    return normalise_verdict(node.key, raw)


def coverage(verdicts: list[JudgeVerdict]) -> float | None:
    """Weighted coverage over judged nodes: (complete + 0.5*partial) / scored."""
    scored = [v for v in verdicts if v.score in WEIGHTS]
    if not scored:
        return None
    return sum(WEIGHTS[v.score] for v in scored) / len(scored)


def recommendation_for(verdict: JudgeVerdict) -> tuple[str, str]:
    """(action, rationale) to improve this node — ported from
    ``clear_eval.recommendation_for_row``."""
    if verdict.out_of_subject_share == "entirely" or (verdict.out_of_subject and verdict.score == "not_answered"):
        return "Wrong", verdict.notes or "Content is present but does not answer this question — replace with on-topic material."
    if verdict.out_of_subject_share in {"mostly", "some"} or verdict.out_of_subject:
        return "Change", verdict.notes or "Mix of on-topic and off-topic content — refocus and remove irrelevant material."
    if verdict.score == "not_answered":
        return "Add", verdict.notes or "No usable answer — add content for this indicator."
    if verdict.score == "partial":
        return "Expand", verdict.notes or "Partially answered — expand the missing dimensions named in the notes."
    return "Keep", verdict.notes or "Adequately answers the question — no change required."
