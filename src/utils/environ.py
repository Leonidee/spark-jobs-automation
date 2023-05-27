from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logger import SparkLogger
from src.config import Config


class EnvironManager:
    """Project environment manager.

    ## Examples
    Initialize Class instance:
    >>> env = EnvironManager()

    Find .env file in project and load variables from it:
    >>> env.load_environ()
    """

    def __init__(self) -> None:
        config = Config()
        from dotenv import find_dotenv, load_dotenv

        self._find_dotenv = find_dotenv
        self._load_dotenv = load_dotenv
        self.logger = SparkLogger(level=config.python_log_level).get_logger(
            logger_name=__name__
        )

    def load_environ(self, dotenv_file_name: str = ".env") -> bool:
        """Find .env file and load environment variables from it.

        ## Notes
        Overrides system environment variables with same names.

        `.env` file should located in root project directory. Notice the example one: `./.env.template`

        ## Parameters
        `dotenv_file_name` : Path to .env file, by default ".env"

        ## Returns
        `bool` : Returns True if found file and loaded variables and False in other case
        """
        self.logger.debug("Loading environ")

        self.logger.debug("Trying to find .env in project")
        try:
            path = self._find_dotenv(
                filename=dotenv_file_name, raise_error_if_not_found=True
            )
            self.logger.debug("File found")
        except IOError:
            self.logger.critical(".env file not found. Environ not loaded!")
            return False

        self.logger.debug("Reading .env file")
        try:
            self._load_dotenv(dotenv_path=path, verbose=True, override=True)
        except IOError:
            self.logger.critical("Enable to read .env file. Environ not loaded!")
            return False

        self.logger.debug("Environ loaded")
        return True
