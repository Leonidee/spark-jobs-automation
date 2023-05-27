from __future__ import annotations

from logging import Logger, getLogger, StreamHandler
from coloredlogs import ColoredFormatter, install
import sys


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

    def __init__(self, level: str = "INFO"):
        self.level = level  # type: ignore

    def get_logger(self, logger_name: str) -> Logger:
        """Returns configured ready-for-use logger instance

        ## Parameters
        `logger_name` : Name of the logger

        ## Returns
        `logging.Logger` : Returns Logger class object
        """
        logger = getLogger(name=logger_name)

        install(logger=logger, level=self.level)
        logger.setLevel(level=self.level)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger_handler = StreamHandler(stream=sys.stdout)

        colored_formatter = ColoredFormatter(
            fmt="[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level_styles=dict(
                info=dict(color="green"),
                error=dict(color="red", bold=False, bright=True),  # type: ignore
            ),
            field_styles=dict(
                asctime=dict(color="magenta"),
                name=dict(color="cyan"),
                levelname=dict(color="yellow", bold=False, bright=True),  # type: ignore
                lineno=dict(color="white"),
            ),
        )

        logger_handler.setFormatter(fmt=colored_formatter)
        logger.addHandler(logger_handler)
        logger.propagate = False

        return logger
