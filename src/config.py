import yaml


class Config:
    def __init__(self) -> None:
        """Project config

        Contains configurations for Spark Jobs and environment
        """
        with open("config.yaml") as f:
            self.config = yaml.safe_load(f)
        self._is_prod = self.config["environ"]["IS_PROD"]

    @property
    def IS_PROD(self) -> bool:
        return self._is_prod

    @IS_PROD.setter
    def IS_PROD(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("value must be boolean!")

        self._is_prod = value

    @IS_PROD.deleter
    def IS_PROD(self) -> None:
        self._is_prod = None

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
