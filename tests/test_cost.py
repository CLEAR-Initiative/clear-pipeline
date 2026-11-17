"""Per-stage generation-cost artifacts (ADR-0015): each spending asset records
its budget; overview sums them. Dict-backed fake S3."""

import io

from clear_pipeline.defs.analysis import paths, store


class _FakeS3:
    def __init__(self):
        self.store: dict[tuple, bytes] = {}

    def put_object(self, *, Bucket, Key, Body, **_kw):
        self.store[(Bucket, Key)] = Body

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise Exception("404")  # noqa: TRY002 — mimic a missing-object error
        return {"Body": io.BytesIO(self.store[(Bucket, Key)])}


_ARGS = ("SDN", "yearly", "v1", "v1")


def test_cost_key_layout():
    assert paths.cost_key(*_ARGS, "sub") == "analysis/cost/SDN/yearly/v1_v1/sub.json"


def test_stage_costs_sum_across_all_generation_stages():
    s3 = _FakeS3()
    spends = {"sub": 0.42, "evaluation": 0.05, "web_gapfill": 0.10,
              "revision": 0.08, "scenarios": 0.02, "changes": 0.03}
    for stage, cost in spends.items():
        store.save_stage_cost(s3, "b", *_ARGS, stage, cost)
    # default stages = GENERATION_COST_STAGES
    assert round(store.load_total_cost(s3, "b", *_ARGS), 2) == round(sum(spends.values()), 2)
    assert set(store.GENERATION_COST_STAGES) == set(spends)  # overview sums exactly these


def test_missing_stage_counts_zero():
    s3 = _FakeS3()
    store.save_stage_cost(s3, "b", *_ARGS, "sub", 1.0)
    # only `sub` written; the other stages are absent → 0
    assert store.load_total_cost(s3, "b", *_ARGS) == 1.0


def test_save_stage_cost_rounds_and_handles_none():
    s3 = _FakeS3()
    store.save_stage_cost(s3, "b", *_ARGS, "sub", None)
    store.save_stage_cost(s3, "b", *_ARGS, "revision", 0.123456)
    assert store.load_json(s3, "b", paths.cost_key(*_ARGS, "sub"))["spend_usd"] == 0.0
    assert store.load_json(s3, "b", paths.cost_key(*_ARGS, "revision"))["spend_usd"] == 0.1235
