from __future__ import annotations

import yaml


class Config:
    """Parses project configuration file: `config.yaml`.

    File contains configurations for Spark Jobs and project environment.

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

    def __init__(self) -> None:
        with open("config.yaml") as f:
            self.config = yaml.safe_load(f)
        self._is_prod = self.config["environ"]["IS_PROD"]

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
