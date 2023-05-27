from __future__ import annotations

from pydantic import BaseModel, validator


class SparkConfigKeeper(BaseModel):
    """Dataclass for keeping Spark configuration properties

    ## Properties
    `executor_memory` : spark.executor.memory\n
    `executor_cores` : spark.executor.cores\n
    `max_executors_num` : spark.dynamicAllocation.maxExecutors\n

    ## Raises
    `ValueError` : Raises if parameter don't pass validation
    `UserWarning` : Raises for notify user about potenrial problems

    ## Notes
    For more information about Spark Properties see -> https://spark.apache.org/docs/latest/configuration.html#application-properties
    """

    executor_memory: str
    executor_cores: int
    max_executors_num: int

    @validator("executor_memory")
    def validate_executor_memory(cls, v) -> str:
        if not isinstance(v, str):
            raise ValueError("must be string")
        return v

    @validator("executor_cores")
    def validate_executor_cores(cls, v) -> int:
        if not isinstance(v, int):
            raise ValueError("must be integer")
        if v < 0:
            raise ValueError("must be positive")
        if v > 10:
            raise ValueError("cores per executor must be lower than 10")
        if v > 5:
            raise UserWarning(
                "are you sure that each executor should use more than 5 cores?"
            )

        return v

    @validator("max_executors_num")
    def validate_max_executors_num(cls, v) -> int:
        if not isinstance(v, int):
            raise ValueError("must be integer")
        if v < 0:
            raise ValueError("must be positive")
        if v > 60:
            raise ValueError("must be lower than 60")
        if v > 40:
            raise UserWarning(
                "are you sure that Cluster can allocate more than 40 executors?"
            )

        return v
