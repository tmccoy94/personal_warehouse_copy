"""Integration tests for pipelines.loading helpers against a real database.

Covers the pieces unit tests cannot: audit row writes, surrogate-key minting
with real sequences and constraints, partition DDL, and the schema-intent gate
actually blocking a write.
"""
from datetime import date

import pytest
from sqlalchemy import text

from pipelines.loading import (
    audit_load,
    ensure_snapshot_partition,
    resolve_branch_keys,
    upsert_person_identity,
)

pytestmark = pytest.mark.integration


# ── audit_load ────────────────────────────────────────────────────────────────

def _audit_row(engine, batch_id: str, asset_name: str):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT status, row_count, notes, started_at, completed_at "
                "FROM load_audit "
                "WHERE batch_id = :batch_id AND asset_name = :asset_name"
            ),
            {"batch_id": batch_id, "asset_name": asset_name},
        ).one_or_none()


def test_audit_load_records_success(engine, test_schema):
    with audit_load(engine, batch_id="it-success", asset_name="it_asset_success",
                    source_system="integration_test") as stats:
        stats["row_count"] = 42

    row = _audit_row(engine, "it-success", "it_asset_success")
    assert row is not None
    assert row.status == "success"
    assert row.row_count == 42
    assert row.completed_at is not None


def test_audit_load_records_failure_and_reraises(engine, test_schema):
    with pytest.raises(ValueError, match="boom"):
        with audit_load(engine, batch_id="it-failure", asset_name="it_asset_failure",
                        source_system="integration_test"):
            raise ValueError("boom")

    row = _audit_row(engine, "it-failure", "it_asset_failure")
    assert row is not None
    assert row.status == "failed"
    assert "boom" in row.notes


def test_audit_load_blocks_on_schema_intent_mismatch(
    engine, test_schema, aligned_schema_intent
):
    """The hard runtime gate: intent file says one schema, env says another —
    audit_load must raise before writing anything."""
    aligned_schema_intent.write_text("prod")  # env is still 'test'

    with pytest.raises(RuntimeError, match="Schema mismatch"):
        with audit_load(engine, batch_id="it-mismatch", asset_name="it_asset_mismatch",
                        source_system="integration_test"):
            pytest.fail("audit_load body must not run on schema mismatch")

    assert _audit_row(engine, "it-mismatch", "it_asset_mismatch") is None


# ── surrogate key helpers ─────────────────────────────────────────────────────

def test_upsert_person_identity_mints_then_reuses(engine, test_schema):
    first = upsert_person_identity(engine, [910001, 910002, 910003])
    assert set(first) == {910001, 910002, 910003}
    assert len(set(first.values())) == 3  # three distinct person_keys minted

    # Overlapping call: existing serials keep their keys, new serial gets a new one.
    second = upsert_person_identity(engine, [910002, 910003, 910004])
    assert second[910002] == first[910002]
    assert second[910003] == first[910003]
    assert second[910004] not in first.values()

    # Crosswalk rows exist exactly once per serial.
    with engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT COUNT(*) FROM person_source_identifier "
                "WHERE source_system = 'keystone' "
                "  AND source_identifier = ANY(:identifiers)"
            ),
            {"identifiers": ["910001", "910002", "910003", "910004"]},
        ).scalar()
    assert count == 4


def test_resolve_branch_keys_returns_only_known_serials(engine, seeded_branches):
    resolved = resolve_branch_keys(engine, [1, 2, 99])
    assert resolved[1] == seeded_branches["Main Branch"]
    assert resolved[2] == seeded_branches["North Branch"]
    assert 99 not in resolved  # unknown serials are omitted, caller decides


# ── partition management ──────────────────────────────────────────────────────

def test_ensure_snapshot_partition_creates_and_is_idempotent(engine, test_schema):
    target = date(2030, 6, 15)
    ensure_snapshot_partition(engine, "person_snapshot_daily", target)
    ensure_snapshot_partition(engine, "person_snapshot_daily", target)  # no error

    with engine.connect() as conn:
        exists = conn.execute(
            text(
                "SELECT 1 FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema "
                "  AND c.relname = 'person_snapshot_daily_2030_06' "
                "  AND c.relispartition"
            ),
            {"schema": test_schema},
        ).one_or_none()
    assert exists is not None
