import coloredlogs
import logging
import sys
import re
from pathlib import Path
from pydantic import BaseModel


class SparkArgsHolder(BaseModel):
    date: str
    depth: str
    threshold: str
    tags_verified_path: str
    src_path: str
    tgt_path: str


class SparkLogger:
    def __init__(self):
        pass

    def get_logger(logger_name: str, level: str = "INFO") -> logging.Logger:
        logger = logging.getLogger(name=logger_name)

        coloredlogs.install(logger=logger, level=level)
        logger.setLevel(level=level)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger_handler = logging.StreamHandler(stream=sys.stdout)

        colored_formatter = coloredlogs.ColoredFormatter(
            fmt="[%(asctime)s UTC] {%(name)s:%(lineno)d} %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level_styles=dict(
                info=dict(color="green"),
                error=dict(color="red", bold=False, bright=True),
            ),
            field_styles=dict(
                asctime=dict(color="magenta"),
                name=dict(color="cyan"),
                levelname=dict(color="yellow", bold=False, bright=True),
                lineno=dict(color="white"),
            ),
        )

        logger_handler.setFormatter(fmt=colored_formatter)
        logger.addHandler(logger_handler)
        logger.propagate = False

        return logger


logger = SparkLogger.get_logger(logger_name=str(Path(Path(__file__).name)))


def assert_args(date: str, depth: int, threshold: int) -> None:
    logger.info("Asserting given arguments: `date`, `depth` and `threshold`.")

    if not isinstance(date, str):
        logger.exception("Wrong arg format detected. `date` arg must be string type.")
        raise AssertionError("`date` arg must be string type.")

    if not re.match(pattern="^\d*$", string=str(depth)):
        logger.exception(
            "Wrong arg format detected. `depth` must be int or str(int) type"
        )
        raise AssertionError(
            f"`depth` must be int or str(int) type. For example: 5 or '5'. Not `{depth}`"
        )

    if not re.match(pattern="^\d*$", string=str(threshold)):
        logger.exception(
            "Wrong arg format detected. `threshold` must be int or str(int) type."
        )
        raise AssertionError(
            f"`threshold` must be int or str(int) type. For example: 200 or '200'. Not `{threshold}`"
        )

    if not re.match(pattern="^\d{4}-\d{2}-\d{2}$", string=date):
        logger.exception(
            "Wrong arg format detected. `date` arg format must be like: `YYYY-MM-DD`."
        )
        raise AssertionError(
            f"`date` arg format must be like: `YYYY-MM-DD`. For example: `2022-01-01`. Not `{date}`"
        )

    if int(depth) > 150:
        logger.exception("Wrong arg format detected. `depth` must be lower that 150.")
        raise AssertionError("`depth` must be lower that 150.")

    if int(threshold) > 5_000:
        logger.exception(
            "Wrong arg format detected. `threshold` must be lower that 5.000."
        )
        raise AssertionError("`threshold` must be lower that 5.000.")

    logger.info("Assertion comleted.")
