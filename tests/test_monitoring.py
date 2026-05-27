import pytest

from pothole_backend.monitoring import MonitoringState


@pytest.fixture
def state():
    return MonitoringState()


async def test_record_processed_increments(state):
    await state.record_processed()
    await state.record_processed()
    assert state._events_processed == 2


async def test_record_failed_increments(state):
    await state.record_failed()
    assert state._events_failed == 1


async def test_record_rejected_increments(state):
    await state.record_rejected()
    assert state._events_rejected == 1


async def test_snapshot_returns_correct_counts(state):
    await state.record_processed()
    await state.record_processed()
    await state.record_failed()
    await state.record_rejected()

    snap = await state.snapshot_and_reset()

    assert snap["events_processed"] == 2
    assert snap["events_failed"] == 1
    assert snap["events_rejected"] == 1


async def test_snapshot_resets_counters(state):
    await state.record_processed()
    await state.snapshot_and_reset()

    snap = await state.snapshot_and_reset()
    assert snap["events_processed"] == 0
    assert snap["events_failed"] == 0
    assert snap["events_rejected"] == 0


async def test_pings_per_second_within_bounds(state):
    for _ in range(60):
        await state.record_request()
    pps = state.pings_per_second()
    # 60 requests within a 60 s window = 1.0 pps (or just under if test is fast)
    assert 0.0 < pps <= 1.0


async def test_pings_per_second_empty_is_zero(state):
    assert state.pings_per_second() == 0.0
