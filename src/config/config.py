from __future__ import annotations

import yaml
import re
import os
from pathlib import Path
import sys

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike
    from typing import Dict

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config.exception import EnableToGetConfig


class Config:
    """Parses project configuration file. By default file named `config.yaml` and located in project root directory.

    File contains configurations for Spark Jobs and project environment.

    ## Parameters
    `config_name` : Name of config file to search in project directory, defults `config.yaml`

    ## Raises
    `EnableToGetConfig` : Raise if enable to find or load config file
    `ValueError` : If wrong config name specified

    ## Notes
    Confinguration file should be located in root project directory.

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

    def __init__(
        self, config_name: str = None, config_path: PathLike[str] | Path = None  # type: ignore
    ) -> None:
        if config_name:
            self._validate_config_name(name=config_name)
            self._CONFIG_NAME = config_name
            self._CONFIG_PATH = self._find_config()
        elif config_path:
            self._CONFIG_PATH = config_path
        else:
            raise ValueError(
                "One of the arguments required. Please specify 'config_name' or 'config_path'"
            )

        try:
            with open(self._CONFIG_PATH) as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            raise EnableToGetConfig("Enable to load config file")

        self._is_prod = self.config["environ"]["IS_PROD"]

    def _validate_config_name(self, name: str) -> bool:
        if not isinstance(name, str):
            raise ValueError("config name must be string type")
        if not re.match(pattern=r"^\w+\.ya?ml$", string=name):
            raise ValueError("invalid config file extention, must be 'yml' or 'yaml'")

        return True

    def _find_config(self) -> Path:
        CONFIG_PATH = None

        _PROJECT_NAME = "spark-jobs-automation"
        _ = os.path.abspath(__file__)
        i, _ = _.split(_PROJECT_NAME)
        root_path = i + _PROJECT_NAME

        for _, _, files in os.walk(
            top=root_path
        ):  # os.walk returns 3 tuples, we need only last one - with filenames
            if (
                self._CONFIG_NAME in files
            ):  # if project files contains given config_name
                for file in files:
                    if (
                        file == self._CONFIG_NAME
                    ):  # try to find file which name equal to given config_name
                        CONFIG_PATH = Path(
                            file
                        ).resolve()  # resolving path to that file

        if not CONFIG_PATH:  # if not find config_name if project files
            raise EnableToGetConfig(
                "Enable to find config file in project!\n"
                "Please, create one or explicitly specify the file name.\n"
                "You can find config file template here -> `$PROJECT_DIR/templates/config.template.yaml`"
            )
        else:
            return CONFIG_PATH

    @property
    def IS_PROD(self) -> bool:
        return self._is_prod  # type: ignore

    @IS_PROD.setter
    def IS_PROD(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("value must be boolean")

        self._is_prod = value

    @property
    def get_users_info_datamart_config(self) -> Dict[str, str] | Dict[str, int]:
        return self.config["spark"]["jobs"]["users_info_datamart"]

    @property
    def get_location_zone_agg_datamart_config(self) -> Dict[str, str] | Dict[str, int]:
        return self.config["spark"]["jobs"]["location_zone_agg_datamart"]

    @property
    def get_friend_recommendation_datamart_config(
        self,
    ) -> Dict[str, str] | Dict[str, int]:
        return self.config["spark"]["jobs"]["friend_recommendation_datamart"]

    @property
    def get_spark_application_name(self) -> str:
        return self.config["spark"]["application_name"]

    @property
    def log4j_level(self) -> str:
        return self.config["logging"]["log4j_level"]

    @property
    def python_log_level(self) -> str:
        return self.config["logging"]["python_log_level"]
