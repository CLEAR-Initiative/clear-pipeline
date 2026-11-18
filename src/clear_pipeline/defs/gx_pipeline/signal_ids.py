"""Signal ids minted by the gx pipeline: ``uuid5(NAMESPACE, externalId)``,
where externalId is the prefixed key clear-api dedupes on (``idmc:123``).
clear-api stores it as ``signals.id`` (clear-api ADR-0023), so a lake record
can name its Signal before Postgres has seen it.

The id depends on the exact bytes of the externalId: never change
``NAMESPACE``, a provider's ``ID_PREFIX``, or how a source builds its
externalId."""

import uuid

NAMESPACE = uuid.UUID("68e7d047-622c-40c8-a533-f9963e5603ce")

# externalId prefixes that clear-api mints outside the pipeline (X ingest,
# ground promotion, case acceptance). No pipeline prefix may overlap them.
NON_PIPELINE_PREFIXES = ("x:", "whatsapp:", "url:")


class InvalidExternalId(ValueError):
    """The externalId is missing or lacks its source's prefix: no id can be minted."""


def signal_id(external_id: str) -> str:
    """The lowercase UUIDv5 of this prefixed externalId."""
    return str(uuid.uuid5(NAMESPACE, external_id))


def mint(external_id: str | None, prefix: str) -> str:
    """``signal_id(external_id)``, once the externalId is checked to carry
    ``prefix`` and something after it."""
    if not external_id or not external_id.startswith(prefix) or external_id == prefix:
        raise InvalidExternalId(f"externalId {external_id!r} lacks the {prefix!r} prefix")
    return signal_id(external_id)
