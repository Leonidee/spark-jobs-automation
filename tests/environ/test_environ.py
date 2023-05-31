import os
import sys
from pathlib import Path

import pytest
from unittest.mock import patch

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.environ import EnvironNotSet, DotEnvError


class TestLoadEnviron:
    def test_load_environ_success(self, environ):
        with patch.object(environ, "_find_dotenv", return_value="/path/to/.env"):
            with patch.object(environ, "_read_dotenv", return_value=None):
                assert environ.load_environ() == True

    def test_load_environ_file_not_found(self, environ):
        with patch.object(environ, "_find_dotenv", side_effect=IOError):
            with pytest.raises(
                DotEnvError, match=".env file not found. Environ not loaded"
            ):
                environ.load_environ()

    def test_load_environ_file_not_found_real(self, environ):
        with pytest.raises(
            DotEnvError, match=".env file not found. Environ not loaded"
        ):
            environ.load_environ("nonexistent_name")

    def test_load_environ_file_read_error(self, environ):
        with patch.object(environ, "_find_dotenv", return_value="/path/to/.env"):
            with patch.object(environ, "_read_dotenv", side_effect=IOError):
                with pytest.raises(
                    DotEnvError, match="Enable to read .env file. Environ not loaded"
                ):
                    environ.load_environ()


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
        with pytest.raises(EnvironNotSet):
            environ.check_environ("NOT_SET_VAR")

    def test_check_environ_multiple_empty_var(self, environ):
        _VARS = ("SET_VAR", "NOT_SET_VAR")

        os.environ["SET_VAR"] = "test_value"

        with pytest.raises(EnvironNotSet):
            environ.check_environ(var=_VARS)
