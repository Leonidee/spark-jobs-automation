import sys
from os import getenv
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config, UnableToGetConfig
from src.environ import EnvironManager

EnvironManager().load_environ()


class TestConfig:
    def test_init_instance_valid_path_str(self):
        Config(config_path=f"{getenv('PROJECT_PATH')}/config/config.yaml")

    def test_init_instance_valid_path_pathlib(self):
        Config(config_path=Path(getenv("PROJECT_PATH"), "config/config.yaml"))  # type: ignore

    def test_init_instance_raises_if_wrong_path(self):
        with pytest.raises(UnableToGetConfig) as err:
            Config(config_path=Path(Path.home(), "Code/config.yaml"))

        assert err.type is UnableToGetConfig
        assert "No such file or directory" in str(err.value)

    def test_init_instance_raises_if_without_args(self):
        with pytest.raises(ValueError) as err:
            Config()

        assert err.type is ValueError
        assert (
            "One of the arguments required. Please specify 'config_name' or 'config_path'"
            in str(err.value)
        )

    def test_init_instance_raises_if_not_found_config(self):
        with pytest.raises(UnableToGetConfig) as err:
            Config(config_name="test.yaml")

        assert err.type is UnableToGetConfig
        assert "Unable to find config file in project!" in str(err.value)

    def test_init_instance_with_config_name_success(self):
        Config(config_name="config.yaml")

    def test_init_instance_with_config_name_raises_if_invalid_extension(self):
        with pytest.raises(ValueError) as err:
            Config(config_name="config.txt")

        assert err.type is ValueError
        assert (
            "Invalid config file extention, config must be an yaml file with 'yml' or 'yaml' extention respectively"
            in str(err.value)
        )

    def test_is_prod_type(self, config):
        assert isinstance(config.IS_PROD, bool)

    def test_environ_type(self, config):
        assert isinstance(config.environ, str)

    def test_get_job_config_type(self, config):
        assert isinstance(config.get_job_config, dict)

    def test_get_logging_level_type(self, config):
        assert isinstance(config.get_logging_level, dict)

    def test_get_spark_app_name_type(self, config):
        assert isinstance(config.get_spark_app_name, str)

    def test_get_spark_app_name_value_upper(self, config):
        assert config.get_spark_app_name == "DATAMART-COLLECTOR-APP"
