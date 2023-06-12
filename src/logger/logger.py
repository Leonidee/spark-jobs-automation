from __future__ import annotations

import sys
from logging import Logger, StreamHandler, getLogger
from pathlib import Path

from coloredlogs import ColoredFormatter, install

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config


class SparkLogger(Logger):
    """Python Logger instance.

    ## Notes
    The Class instance configured to write logs only in `stgout` and `stgerr`.

    ## Examples
    Creating class instance example:
    >>> logger = SparkLogger(level="DEBUG").get_logger(logger_name=__name__)

    Common usage:
    >>> logger.info("This is a test!")
    [2023-05-24 17:32:16] {src.utils.environ:4} INFO: This is a test!
    """

    __slots__ = ("config", "_level")

    def __init__(self, level: str | None = None):
        if not level:
            self.config = Config(config_name="config.yaml")
            self._level = self.config.get_logging_level["python"]  # type: ignore
        else:
            self._level = level

    def get_logger(self, name: str) -> Logger:
        """Returns configured ready-for-use logger instance

        ## Parameters
        `logger_name` : Name of the logger

        ## Returns
        `logging.Logger` : Returns Logger class object
        """
        logger = getLogger(name=name)

        install(logger=logger, level=self._level)
        logger.setLevel(level=self._level)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger_handler = StreamHandler(stream=sys.stdout)

        colored_formatter = ColoredFormatter(
            fmt=r"[%(asctime)s] {%(name)s.%(funcName)s:%(lineno)d} %(levelname)s: %(message)s",
            datefmt=r"%Y-%m-%d %H:%M:%S",
            level_styles=dict(
                info=dict(color="green"),
                error=dict(color="red", bright=True),  # type: ignore
                exception=dict(color="red", bright=True),  # type: ignore
                warning=dict(color="magenta", bold=True),
                critical=dict(color="red", bold=True),
                debug=dict(color="blue", bright=True),
            ),
            field_styles=dict(
                asctime=dict(color="magenta"),
                name=dict(color="cyan"),
                funcName=dict(color="white"),
                levelname=dict(color="yellow", bold=False, bright=True),  # type: ignore
                lineno=dict(color="cyan", bright=True),
            ),
        )

        logger_handler.setFormatter(fmt=colored_formatter)
        logger.addHandler(logger_handler)
        logger.propagate = False

        return logger
