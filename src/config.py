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
    def tags_job_config(self) -> dict:
        return self.config["spark"]["tags-job"]

    @property
    def log_level(self) -> str:
        return self.config["logging"]["level"]
