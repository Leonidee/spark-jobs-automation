import os
import sys
from pathlib import Path
import time
from typing import Literal
from enum import Enum

from requests.exceptions import JSONDecodeError
from dataclasses import dataclass
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

_CONFIG_NAME = "config.yaml"


def find_config() -> Path:
    _CONFIG_NAME = "config.yaml"
    CONFIG_PATH = None

    _PROJECT_NAME = "spark-jobs-automation"
    _ = os.path.abspath(__file__)
    i, _ = _.split(_PROJECT_NAME)
    root_path = i + _PROJECT_NAME

    for dirpath, _, files in os.walk(
        top=root_path
    ):  # os.walk returns 3 tuples, we need only last one - with filenames
        # print(dirpath)
        if _CONFIG_NAME in files:  # if project files contains given config_name
            for _ in files:
                if _ == _CONFIG_NAME:
                    print(Path(dirpath, _))
        #     for file in files:
        #         if (
        #             file == _CONFIG_NAME
        #         ):  # try to find file which name equal to given config_name
        #             CONFIG_PATH = Path(file).resolve()  # resolving path to that file

    # if not CONFIG_PATH:  # if not find config_name if project files
    #     raise ValueError(
    #         "Enable to find config file in project!\n"
    #         "Please, create one or explicitly specify the file name.\n"
    #         "You can find config file template here -> `$PROJECT_DIR/templates/config.template.yaml`"
    #     )
    # else:
    # return CONFIG_PATH


def main():
    find_config()


if __name__ == "__main__":
    main()
