from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass(frozen=True)
class AirflowTaskData:
    task: str
    dag: str
    execution_dt: datetime

    def __str__(self) -> str:
        return f"\tTask: {self.task}\n\tDAG: {self.dag}\n\tExecution time: {self.execution_dt}"


class MessageType(Enum):
    failed = "failed"


@dataclass(frozen=True)
class TelegramMessage:
    task_data: AirflowTaskData
    type: MessageType

    def __str__(self) -> str:  # type: ignore
        if self.type.failed:
            return f"🤔 TASK FAILED!\n\n{self.task_data}"
