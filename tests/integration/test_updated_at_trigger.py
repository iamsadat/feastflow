"""Migration 0003: the updated_at trigger keeps an explicit value."""

import os
from datetime import datetime, timezone

import psycopg
import pytest

pytestmark = pytest.mark.integration

BACKDATED = datetime(2026, 9, 20, 14, 30, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as c:
        yield c


def test_update_without_updated_at_is_stamped_now(conn):
    with conn.transaction(force_rollback=True):
        updated_at, tx_start = conn.execute(
            "UPDATE customers SET phone = phone WHERE customer_id = 7 "
            "RETURNING updated_at, now()"
        ).fetchone()
    assert updated_at == tx_start


def test_explicit_updated_at_is_kept(conn):
    with conn.transaction(force_rollback=True):
        (updated_at,) = conn.execute(
            "UPDATE customers SET updated_at = %s WHERE customer_id = 7 "
            "RETURNING updated_at",
            (BACKDATED,),
        ).fetchone()
    assert updated_at == BACKDATED