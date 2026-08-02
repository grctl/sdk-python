from grctl.exec.manager import ExecutionManager
from grctl.logging_config import get_logger
from grctl.models import Command
from grctl.models.command import CmdKind, WorkerTerminateRunCmd

logger = get_logger(__name__)


class CMDHandler:
    """Dispatches a decoded worker Command to the right domain action."""

    def __init__(self, execution_manager: ExecutionManager) -> None:
        self._execution_manager = execution_manager

    async def handle(self, cmd: Command) -> bool:
        match cmd.kind:
            case CmdKind.worker_terminate_run:
                if isinstance(cmd.msg, WorkerTerminateRunCmd):
                    return self._execution_manager.terminate(cmd.msg.run_id)
                logger.warning("Invalid worker terminate run command: %s", cmd.msg)
                return False
            case _:
                logger.warning("Unknown worker command kind: %s", cmd.kind)
                return False
