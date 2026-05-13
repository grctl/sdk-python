import pytest
from pydantic import BaseModel

from grctl.models import Directive, DirectiveKind, Start
from grctl.models.history import TaskStarted
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.task import task
from grctl.workflow import Workflow
from tests.conftest import create_directive


class UserModel(BaseModel):
    name: str
    age: int


def _find_task_started(published) -> TaskStarted:
    for _, msg in published:
        if isinstance(msg, object) and hasattr(msg, "msg") and isinstance(msg.msg, TaskStarted):
            return msg.msg
    raise AssertionError("No TaskStarted event found")


@pytest.mark.asyncio
async def test_task_args_normalized_to_primitives_pydantic(mock_connection):
    connection, published = mock_connection

    wf = Workflow(workflow_type="ArgNormPydantic")

    @task
    async def process(user: UserModel) -> str:
        return user.name

    @wf.start()
    async def start(ctx: Context, name: str, age: int) -> Directive:
        result = await process(UserModel(name=name, age=age))
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={"name": "Alice", "age": 30}),
        directive_id="dir-1",
        run_id="run-1",
        wf_id="wf-1",
        wf_type="ArgNormPydantic",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    started = _find_task_started(published)
    assert started.args == {"user": {"name": "Alice", "age": 30}}
    assert isinstance(started.args["user"], dict)
