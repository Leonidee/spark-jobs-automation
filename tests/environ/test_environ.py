import pytest
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.environ import EnvironNotSet
from src.environ import EnvironManager


@pytest.fixture
def environ():
    """Returns instance of `EnvironManager` class"""
    return EnvironManager()


class TestCheckEnviron:
    def test_check_environ_single(self, environ):
        os.environ["TEST_VAR"] = "test_value"
        assert environ.check_environ("TEST_VAR") == True

    def test_check_environ_multiple(self, environ):
        _VARS = ("TEST_VAR_1", "TEST_VAR_2")

        for var in _VARS:
            os.environ[var] = "test_value"
        assert environ.check_environ(var=_VARS) == True

    def test_check_environ_multiple_missing_var(self, environ):
        os.environ["EXACTLY_NOT_SET_VAR"] = None

        with pytest.raises(EnvironNotSet):
            environ.check_environ("EXACTLY_NOT_SET_VAR")

    # def test_check_environ_multiple_empty_var(self, environ):
    #     os.environ["SET_VAR"] = "test_value_1"
    #     os.environ["NOT_SET_VAR"] = ""

    #     with pytest.raises(EnvironNotSet):
    #         environ.check_environ(("SET_VAR", "NOT_SET_VAR"))
