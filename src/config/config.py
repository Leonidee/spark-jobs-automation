from __future__ import annotations

import yaml
import os
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config.exception import EnableToGetConfig


class Config:
    """Parses project configuration file. By default file named `config.yaml` and located in project root directory.

    File contains configurations for Spark Jobs and project environment.

    ## Parameters
    `config_name` : Name of config file to search in project directory, defults `config.yaml`

    ## Raises
    `EnableToGetConfig` : Raise if enable to find or load config file

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

    def __init__(self, config_name: str = "config.yaml") -> None:  # type: ignore
        self.config_name = config_name
        self._CONFIG_PATH = self._find_config()

        try:
            with open(self._CONFIG_PATH) as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError:
            raise EnableToGetConfig("Enable to load config file")

        self._is_prod = self.config["environ"]["IS_PROD"]

    def _find_config(self) -> Path:
        config_path = None

        for _, _, files in os.walk("."):
            if self.config_name in files:
                for file in files:
                    if file == self.config_name:
                        config_path = Path(file).resolve()

        if not config_path:
            raise EnableToGetConfig(
                "Enable to find config file in project!\n"
                "Please, create one or explicitly specify the file name.\n"
                "You can find config file template here -> `$PROJECT_DIR/templates/config.template.yaml`"
            )
        else:
            return config_path

    @property
    def IS_PROD(self) -> bool:
        return self._is_prod  # type: ignore

    @IS_PROD.setter
    def IS_PROD(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("value must be boolean")

        self._is_prod = value

    @property
    def get_users_info_datamart_config(self) -> dict:
        return self.config["spark"]["jobs"]["users_info_datamart"]

    @property
    def get_location_zone_agg_datamart_config(self) -> dict:
        return self.config["spark"]["jobs"]["location_zone_agg_datamart"]

    @property
    def get_friend_recommendation_datamart_config(self) -> dict:
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
