"""The legacy Dataminr API puts the source link in publicPost.link.

PublicPost only knows `href`, so passing the legacy publicPost through
dropped the link: every Dataminr signal since the switch to the legacy API
had no url, and the app fell back to dataminr.com instead of the source.
"""

from clear_pipeline.providers.dataminr import _convert_legacy_alert

LINK = "https://r.dataminr.com/Hhd7BGDwKUuDm29NoPrWy3"


def _legacy_alert(public_post: dict | None) -> dict:
    # The shape the legacy /alerts/1/alerts endpoint returns (alertversion 19).
    return {
        "alertId": "201484348962513113-1793121379000-1",
        "eventTime": 1793121379000,
        "headline": "Protest planned outside the faculty",
        "firstAlertURL": "https://firstalert.dataminr.com/?view=alert&alertId=201484348962513113-1793121379000-1",
        "termsOfUse": "https://www.dataminr.com/legal/firstalert-tos",
        "publicPost": public_post,
    }


def test_legacy_post_link_becomes_href():
    signal = _convert_legacy_alert(_legacy_alert({"link": LINK, "text": "post text"}))
    assert signal is not None
    assert signal.publicPost is not None
    assert signal.publicPost.href == LINK
    assert signal.publicPost.text == "post text"


def test_existing_href_is_kept():
    other = "https://example.org/post"
    signal = _convert_legacy_alert(_legacy_alert({"href": other, "link": LINK}))
    assert signal is not None and signal.publicPost is not None
    assert signal.publicPost.href == other


def test_no_public_post_stays_none():
    signal = _convert_legacy_alert(_legacy_alert(None))
    assert signal is not None
    assert signal.publicPost is None
