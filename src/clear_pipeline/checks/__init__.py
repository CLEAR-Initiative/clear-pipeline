"""Standalone preflight checks — run from CI, not from Dagster.

A check here answers one question about the *deployed environment* the image
is about to run against, before the image is built and rolled. Nothing in
``defs/`` imports this package, and nothing here is a Dagster asset or asset
check: a check that fails must stop a deploy, not wedge a code location.

Each module is runnable: ``python -m clear_pipeline.checks.<name>``.
"""
