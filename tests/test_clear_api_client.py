"""`providers/clear_api.py` error handling and the gx-only query strings.
httpx is patched; no network."""

from unittest.mock import MagicMock, patch

import pytest

from clear_pipeline.providers import clear_api
from clear_pipeline.providers.clear_api import ClearApiError, ClearApiNotFound, ClearApiStaleMembers


def _response(body: dict, status: int = 200):
    resp = MagicMock(status_code=status, text="")
    resp.json.return_value = body
    resp.raise_for_status.return_value = None
    return resp


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("CLEAR_API_URL", "http://clear-api.test/graphql")
    monkeypatch.setenv("CLEAR_API_KEY", "sk_test")


def test_not_found_is_raised_once_without_retry_or_sleep():
    body = {"errors": [{"message": "Signal not found", "extensions": {"code": "NOT_FOUND"}}]}
    with patch.object(clear_api.httpx, "post", return_value=_response(body)) as post, \
         patch.object(clear_api.time, "sleep") as sleep, \
         pytest.raises(ClearApiNotFound):
        clear_api.update_signal_content({"sourceId": "s", "externalId": "idmc:1", "contentHash": "h", "rawData": {}})
    assert post.call_count == 1
    sleep.assert_not_called()


def test_stale_event_members_is_raised_once_without_retry():
    body = {"errors": [{"message": "stale", "extensions": {"code": "STALE_EVENT_MEMBERS"}}]}
    with patch.object(clear_api.httpx, "post", return_value=_response(body)) as post, \
         patch.object(clear_api.time, "sleep") as sleep, \
         pytest.raises(ClearApiStaleMembers):
        clear_api.set_event_aggregates("e1", {"rank": 0.0}, [{"id": "s1", "revision": 2, "title": "x"}])
    assert post.call_count == 1
    sleep.assert_not_called()
    sent = post.call_args.kwargs["json"]["variables"]
    assert sent["members"] == [{"id": "s1", "revision": 2}]  # only the CAS fields


def test_other_graphql_errors_are_still_retried():
    body = {"errors": [{"message": "conflict", "extensions": {"code": "CONFLICT"}}]}
    with patch.object(clear_api.httpx, "post", return_value=_response(body)) as post, \
         patch.object(clear_api.time, "sleep"), \
         pytest.raises(RuntimeError) as exc:
        clear_api.update_signal_content({})
    assert not isinstance(exc.value, ClearApiNotFound)
    assert post.call_count == 3


def test_schema_mismatch_is_a_non_retried_clear_api_error():
    # gx deployed before clear-api: the sync query selects fields an older API lacks.
    body = {"errors": [{"message": 'Cannot query field "retracted" on type "Signal".'}]}
    with patch.object(clear_api.httpx, "post", return_value=_response(body)) as post, \
         pytest.raises(ClearApiError) as exc:
        clear_api.create_signal_for_sync({})
    assert not isinstance(exc.value, ClearApiNotFound)
    assert post.call_count == 1


def test_query_strings_carry_the_new_fields():
    for field in ("contentHash", "retracted", "rawS3Key"):
        assert field in clear_api.CREATE_SIGNAL_FOR_SYNC
    for field in ("retracted", "revision", "rawS3Key"):
        assert field in clear_api.UPDATE_SIGNAL_CONTENT


def test_shared_create_signal_query_is_unchanged():
    """Production ingest (Dataminr, ACLED, …) must not select fields an older
    clear-api lacks."""
    assert "retracted" not in clear_api.CREATE_SIGNAL
    assert "contentHash" not in clear_api.CREATE_SIGNAL
