import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config, EnableToGetConfig


class TestConfig:
    def test_init_instance_valid_path_str(self):
        Config(
            config_path="/Users/leonidgrisenkov/Code/spark-jobs-automation/config.yaml"  # type: ignore
        )

    def test_init_instance_valid_path_pathlib(self):
        Config(config_path=Path(Path.home(), "Code/spark-jobs-automation/config.yaml"))

    def test_init_instance_wrong_path(self):
        with pytest.raises(EnableToGetConfig, match="Enable to load config file"):
            Config(config_path="nonexistent/path/to/config.yaml")  # type: ignore

    def test_init_instance_without_args(self):
        with pytest.raises(
            ValueError,
            match="One of the arguments required. Please specify 'config_name' or 'config_path'",
        ):
            Config()

    def test_validate_config_name_success(self, config):
        assert config._validate_config_name(name="config_test.yaml") == True

    def test_validate_config_name_invalid_extension(self, config):
        with pytest.raises(
            ValueError, match="invalid config file extention, must be 'yml' or 'yaml'"
        ):
            config._validate_config_name(name="config_test.txt")

    def test_find_config_raises_if_not_find(self):
        with pytest.raises(EnableToGetConfig):
            Config(config_name="test_config.yaml")

    def test_is_prod_type(self, config):
        assert isinstance(config.IS_PROD, bool)

    def test_is_prod_setter(self, config):
        config.IS_PROD = True
        assert config.IS_PROD == True
        config.IS_PROD = False
        assert config.IS_PROD == False

    def test_get_dict_properties(self, config):
        assert isinstance(config.get_users_info_datamart_config, dict)
        assert isinstance(config.get_location_zone_agg_datamart_config, dict)
        assert isinstance(config.get_friend_recommendation_datamart_config, dict)

    def test_get_str_properties(self, config):
        assert isinstance(config.get_spark_application_name, str)
        assert isinstance(config.log4j_level, str)
        assert isinstance(config.python_log_level, str)
