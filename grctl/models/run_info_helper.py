from datetime import datetime

from grctl.models.run_info import RunInfo, RunStatus


class RunInfoManager:
    @staticmethod
    def start(run_info: RunInfo, timestamp: datetime) -> RunInfo:
        run_info.status = RunStatus.running
        run_info.started_at = timestamp

        return run_info
