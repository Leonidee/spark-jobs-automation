from __future__ import annotations

import re
import sys
from datetime import date
from os import walk
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from os import PathLike
    from typing import Dict

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config.exceptions import UnableToGetConfig


class Config:
    """Parses project's configuration file.

    ## Notes
    Confinguration file should be located in one of the project's dirs.

    ## Examples
    Initialize Class instance:
    >>> config = Config()

    Check if we on prod:
    >>> config.IS_PROD
    False

    Show python logging level:
    >>> config.python_log_level
    INFO

    Get Spark Job arguments:
    >>> config.get_users_info_datamart_config["DATE"]
    2022-03-12
    >>> config.get_users_info_datamart_config["SRC_PATH"]
    s3a://data-ice-lake-05/messager-data/analytics/geo-events
    >>> a, b, c, d = config.get_users_info_datamart_config.values()
    >>> print(a)
    2022-03-12
    """

    __slots__ = ("_CONFIG_NAME", "_CONFIG_PATH", "_config", "_environ")

    def __init__(
        self,
        config_name: str | None = None,
        config_path: PathLike[str] | Path | str | None = None,
    ) -> None:
        """

        ## Notes
        To init class instance you need to specify one of the required arguments: `config_name` or `config_path`.

        ## Parameters
        `config_name` : Config file name, by default None\n
        `config_path` : Path to config file, by default None

        ## Raises
        `ValueError` : If failed to validate config file name or if one of the required arguments not specified\n
        `UnableToGetConfig` : If unable to find or read config file
        """
        if config_name:
            self._validate_config_name(name=config_name)
            self._CONFIG_NAME = config_name
            self._CONFIG_PATH = self._find_config()
        elif config_path:
            self._validate_config_name(name=str(config_path).split("/")[-1])
            self._CONFIG_PATH = config_path
        else:
            raise ValueError(
                "One of the arguments required. Please specify 'config_name' or 'config_path'"
            )

        try:
            with open(self._CONFIG_PATH) as f:
                self._config = yaml.safe_load(f)
                self._environ = self._config["environ"]["type"]
        except FileNotFoundError as err:
            raise UnableToGetConfig(str(err))

    def _validate_config_name(self, name: str) -> bool:
        if not isinstance(name, str):
            raise TypeError("config name must be string type")
        if not re.match(pattern=r"^\w+\.ya?ml$", string=name):
            raise ValueError(
                "Invalid config file extention, config must be an yaml file with 'yml' or 'yaml' extention respectively"
            )

        return True

    def _find_config(self) -> Path:
        for dirpath, _, filenames in walk(Path.cwd()):
            for filename in filenames:
                if filename == self._CONFIG_NAME:
                    return Path(dirpath, filename)

        raise UnableToGetConfig(
            "Unable to find config file in project!\n"
            "Please, create one or explicitly specify the full path to file."
        )

    @property
    def IS_PROD(self) -> bool:
        return bool(self._config["environ"]["is_prod"])

    @property
    def environ(self) -> str:
        return self._environ

    @environ.setter
    def environ(self, v: str) -> ...:
        if not isinstance(v, str):
            raise TypeError("value must be string")

        self._environ = v

    @property
    def get_job_config(
        self,
    ) -> Dict[str, Dict[str, str | int | date]]:
        return self._config["spark"]["jobs"]

    @property
    def get_logging_level(self) -> Dict[str, str]:
        return {k: v.upper() for k, v in self._config["logging"]["level"].items()}

    @property
    def get_spark_app_name(self) -> str:
        return self._config["spark"]["application_name"].upper()
