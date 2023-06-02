from __future__ import annotations

import sys
from pathlib import Path
import os

from typing import overload, Tuple

from dotenv import find_dotenv, load_dotenv

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.logger import SparkLogger
from src.config import Config
from src.environ.exceptions import EnvironNotSet, DotEnvError


class EnvironManager:
    """Project environment manager.

    ## Examples
    Initialize Class instance:
    >>> environ = EnvironManager()

    Find .env file in project and load variables from it:
    >>> environ.load_environ()

    Check if all required variables set:
    >>> REQUIRED_VARS = ('VAR_1', 'VAR_2')
    >>> result = environ.check_environ(var=REQUIRED_VARS)
    >>> print(result)
    True

    If not set:
    >>> result = environ.check_environ(var='NOT_SET_VAR')
    [2023-05-30 19:54:51] {src.environ.environ:83} CRITICAL: NOT_SET_VAR environment variable not set
    src.environ.exception.EnvironNotSet: Environment variables not set properly. You can find list of required variables here -> $PROJECT_DIR/templates/.env.template # <---- raised exception
    """

    def __init__(self) -> None:
        config = Config(config_name="config.yaml")

        self._find_dotenv = find_dotenv
        self._read_dotenv = load_dotenv
        self.logger = SparkLogger(level=config.python_log_level).get_logger(
            logger_name=__name__
        )

    def load_environ(self, dotenv_file_name: str = ".env") -> bool:
        """Find .env file and load environment variables from it.

        ## Notes
        Overrides system environment variables with same names.

        `.env` file should located in root project directory. Notice the example one: `$PROJECT_DIR/templates/.env.template`

        ## Parameters
        `dotenv_file_name` : Path to .env file, by default ".env"

        ## Raises
        `DotEnvError` : Raise if enable to find or load .env

        ## Returns
        `bool` : Returns True if found file and loaded variables and False in other case
        """
        self.logger.debug("Loading environ")

        self.logger.debug("Trying to find .env in project")
        try:
            _PATH = self._find_dotenv(
                filename=dotenv_file_name, raise_error_if_not_found=True
            )
            self.logger.debug(f"File found. Path to file: {_PATH}")
        except IOError:
            raise DotEnvError(".env file not found. Environ not loaded")

        self.logger.debug("Reading .env file")
        try:
            self._read_dotenv(dotenv_path=_PATH, verbose=True, override=True)
            self.logger.debug("Environ loaded")
            return True

        except IOError:
            raise DotEnvError("Enable to read .env file. Environ not loaded")

    @overload
    def check_environ(self, var: str) -> bool:
        ...

    @overload
    def check_environ(self, var: Tuple[str]) -> bool:
        ...

    def check_environ(self, var) -> bool:
        _ERROR_MSG = "Environment variables not set properly. You can find list of required variables here -> $PROJECT_DIR/templates/.env.template"
        self.logger.debug("Checking if required envrion variables set")

        if isinstance(var, str):
            self.logger.debug(f"Check: {var}")
            if var not in os.environ:
                self.logger.critical(f"{var} environment variable not set")
                raise EnvironNotSet(_ERROR_MSG)

        if isinstance(var, Tuple):
            self.logger.debug(f"Vars to check: {len(var)}")
            for _ in var:
                self.logger.debug(f"Check: {_}")
                if _ not in os.environ:
                    self.logger.critical(f"{_} environment variable not set")

            if not all(os.environ.get(_) for _ in var):
                raise EnvironNotSet(_ERROR_MSG)

        self.logger.debug("All required variables set")
        return True
