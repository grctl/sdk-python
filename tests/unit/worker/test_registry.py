import pytest

from grctl.worker.registry import WorkflowRegistry
from grctl.workflow import Workflow


def test_registry_provides_the_worker_workflow_catalog() -> None:
    orders = Workflow("orders")
    payments = Workflow("payments")

    registry = WorkflowRegistry([orders, payments])

    assert registry.all() == [orders, payments]
    assert registry.types() == ["orders", "payments"]
    assert [definition.type for definition in registry.type_defs()] == ["orders", "payments"]
    assert registry.get("orders") is orders


def test_registry_rejects_an_unregistered_workflow_type() -> None:
    registry = WorkflowRegistry([Workflow("orders")])

    with pytest.raises(ValueError, match="No workflow registered for type 'payments'"):
        registry.get("payments")
