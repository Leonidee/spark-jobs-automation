import logging
import coloredlogs
import sys


class SparkLogger(logging.Logger):
    def __init__(self, level: str = "INFO"):
        self.level = level

    def get_logger(self, logger_name: str) -> logging.Logger:
        logger = logging.getLogger(name=logger_name)

        coloredlogs.install(logger=logger, level=self.level)
        logger.setLevel(level=self.level)

        if logger.hasHandlers():
            logger.handlers.clear()

        logger_handler = logging.StreamHandler(stream=sys.stdout)

        colored_formatter = coloredlogs.ColoredFormatter(
            fmt="[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s: %(message)s",
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
