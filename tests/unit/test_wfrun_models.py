from datetime import UTC, datetime

import msgspec
from ulid import ULID

from grctl.models import RunInfo, RunStatus


def test_workflow_run_serialization():
    run_id = str(ULID())
    parent_id = str(ULID())
    created = datetime.now(UTC)
    started = datetime.now(UTC)
    completed = datetime.now(UTC)

    run = RunInfo(
        id=run_id,
        wf_id="test-workflow-001",
        wf_type="TestWorkflow",
        status=RunStatus.scheduled,
        parent_run_id=parent_id,
        created_at=created,
        started_at=started,
        completed_at=completed,
    )

    run_bytes = msgspec.json.Encoder().encode(run)
    decoded_run = msgspec.json.Decoder(type=RunInfo).decode(run_bytes)

    assert decoded_run.id == run_id
    assert decoded_run.parent_run_id == parent_id
    assert decoded_run.created_at == created
    assert decoded_run.started_at == started
    assert decoded_run.completed_at == completed


def test_workflow_run_history_seq_id_serialization():
    run = RunInfo(
        id=str(ULID()),
        wf_id="test-workflow-001",
        wf_type="TestWorkflow",
        history_seq_id=123,
    )

    run_bytes = msgspec.msgpack.encode(run)
    decoded_run = msgspec.msgpack.decode(run_bytes, type=RunInfo)

    assert decoded_run.history_seq_id == 123


def test_workflow_run_history_seq_id_defaults_to_zero():
    run = RunInfo(
        id=str(ULID()),
        wf_id="test-workflow-001",
        wf_type="TestWorkflow",
    )

    run_bytes = msgspec.msgpack.encode(run)
    decoded_run = msgspec.msgpack.decode(run_bytes, type=RunInfo)

    assert decoded_run.history_seq_id == 0
