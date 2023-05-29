from __future__ import annotations

import sys
from logging import getLogger
import os
from pathlib import Path
from time import sleep
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal

import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.environ import EnvironManager
from src.cluster.exception import YandexAPIError
from src.environ import EnvironError


class DataProcCluster:
    """Class for manage DataProc Clusters. Sends requests to Yandex Cloud API.

    ## Notes
    To initialize Class instance you need to specify environment variables in `.env` project file or as a global environment variables. See `.env.template` for mote details.

    At initializing moment will try to get IAM token from environment variables if no ones, sends request to get token and than set to environ. IAM token needs to communicate with Yandex Cloud API.

    ## Examples
    Initialize Class instance:
    >>> cluster = DataProcCluster()

    Send request to start Cluster:
    >>> cluster.exec_command(command="start")

    If request was sent successfully we can check Cluster status:
    >>> cluster.check_status(target_status="running")
    ... [2023-05-26 12:51:20] {src.cluster:109} INFO: Sending request to execute Cluster command: start
    ... [2023-05-26 12:51:21] {src.cluster:133} INFO: Sending request to check Cluster status. Target status: running
    ... [2023-05-26 12:51:21] {src.cluster:156} INFO: Current cluster status is: STARTING
    ... [2023-05-26 12:51:41] {src.cluster:156} INFO: Current cluster status is: STARTING
    ... [2023-05-26 12:59:39] {src.cluster:160} INFO: Current cluster status is: RUNNING
    ... [2023-05-26 12:59:39] {src.cluster:165} INFO: The target status has been reached!
    """

    def __init__(self, max_retries: int = 10, retry_delay: int = 60) -> None:
        config = Config()

        self.logger = (
            getLogger("aiflow.task")
            if config.IS_PROD
            else SparkLogger(level=config.python_log_level).get_logger(
                logger_name=__name__
            )
        )
        try:
            env = EnvironManager()
            env.load_environ()
        except EnvironError as e:
            self.logger.critical(e)

        self._MAX_RETRIES = max_retries
        self._DELAY = retry_delay
        self._CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
        self._BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
        self._OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")

        if "YC_IAM_TOKEN" not in os.environ:
            self._get_iam_token()
        self._IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

    @property
    def max_retries(self) -> int:
        """
        Max retries to check Cluster status, defaults 10. Must be lower than 20.
        """
        return self._MAX_RETRIES

    @max_retries.setter
    def max_retries(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value > 20:
            raise ValueError("value must be lower than 20")

        if value < 0:
            raise ValueError("value must be positive")

        self._MAX_RETRIES = value

    @max_retries.deleter
    def max_retries(self) -> None:
        "Resets to default value"
        self._MAX_RETRIES = 10

    @property
    def retry_delay(self) -> int:
        """Delay between retries in secs. Must be lower than 30 minutes"""
        return self._DELAY

    @retry_delay.setter
    def retry_delay(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value < 0:
            raise ValueError("value must be positive")

        if value > 60 * 30:
            raise ValueError("must be lower than 30 minutes")

        self._DELAY = value

    @retry_delay.deleter
    def retry_delay(self) -> None:
        "Resets to default value"
        self._DELAY = 60

    def _get_iam_token(self) -> None:
        """
        Gets IAM token from Yandex Cloud API. If recieved, set as environment variable.

        ## Raises
        `YandexAPIError` : If enable to get IAM token or error occured while sending requests to API
        """

        self.logger.debug("Getting Yandex Cloud IAM token")

        self.logger.debug(f"Max retries: {self._MAX_RETRIES}")
        self.logger.debug(f"Delay between retries: {self._DELAY} secs")

        _TRY = 1
        _OK = False

        while not _OK:
            try:
                self.logger.debug(f"Send request to API. Try: {_TRY}")
                response = requests.post(
                    url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                    json={"yandexPassportOauthToken": self._OAUTH_TOKEN},
                    timeout=60 * 2,
                )
                response.raise_for_status()

            except (HTTPError, ConnectionError, InvalidSchema, Timeout) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API and no more retries left. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    sleep(self._DELAY)
                    continue

            if response.status_code == 200:
                self.logger.debug("Response received")
                response = response.json()

                try:
                    # fmt: off
                    token_key = next(_ for _ in response.keys() if re.search("iamtoken", _, re.IGNORECASE))

                    # fmt: on
                    self.logger.debug("IAM token collected!")
                    os.environ["YC_IAM_TOKEN"] = response[token_key]
                    _OK = True
                    break

                except StopIteration:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError(
                            "Enable to get IAM token from API response!"
                        )
                    else:
                        self.logger.warning(
                            "No IAM token in API response. Will make another try after delay"
                        )
                        _TRY += 1
                        sleep(self._DELAY)
                        continue

            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        "Enable to get IAM token from API. No more retries left"
                    )
                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Will make another try after delay"
                    )
                    _TRY += 1
                    sleep(self._DELAY)
                    continue

    def exec_command(self, command: Literal["start", "stop"]) -> None:
        """Sends request to execute Cluster command.

        ## Parameters
        `command` : Command to execute

        ## Raises
        `YandexAPIError` : If enable get response or error occured while sending requests to API and no more retries left
        """
        self.logger.info(f"Sending request to execute Cluster command: {command}")

        self.logger.debug(f"Max retries: {self._MAX_RETRIES}")
        self.logger.debug(f"Delay between retries: {self._DELAY} secs")

        _TRY = 1
        _OK = False

        while not _OK:
            try:
                self.logger.debug(f"Requesting... Try: {_TRY}")
                response = requests.post(
                    url=f"{self._BASE_URL}/{self._CLUSTER_ID}:{command}",
                    headers={"Authorization": f"Bearer {self._IAM_TOKEN}"},
                    timeout=60 * 2,
                )
                response.raise_for_status()

            except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API and no more retries left. Possible because of exception:\n{e}"
                    )

                self.logger.warning(
                    "An error occured! See traceback below. Will make another try after delay"
                )
                self.logger.exception(e)
                _TRY += 1
                sleep(self._DELAY)
                continue

            if response.status_code == 200:
                self.logger.debug("Response received")
                self.logger.debug(f"API response:\n{response.json()}")
                _OK = True
                break

            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Enable send request to API!")

                self.logger.debug(
                    "Ops, seems like something went wrong. Will make another try after delay"
                )
                _TRY += 1
                sleep(self._DELAY)
                continue

    def check_status(self, target_status: Literal["running", "stopped"]) -> None:
        """Sends request to check current Cluster status.

        Waits until Cluster status will be equal to `target_status`.

        ## Parameters
        `target_status` : The target Cluster status

        ## Raises
        `YandexAPIError` : If enable get response or error occured while sending requests to API and no more retries left

        ## Notes

        The default `max_retries_to_check_status` attribute set to 10 (ten attempts with 60 secs pause between each)
        You can change it like that:
        >>> cluster.max_retries_to_check_status = 2
        """
        self.logger.info(
            f"Sending request to check Cluster status. Target status: {target_status}"
        )

        self.logger.debug(f"Max retries: {self._MAX_RETRIES}")
        self.logger.debug(f"Delay between retries: {self._DELAY} secs")

        _TRY = 1
        _IS_TARGET = False

        while not _IS_TARGET:
            try:
                self.logger.debug(f"Requesting... Try: {_TRY}")
                response = requests.get(
                    url=f"{self._BASE_URL}/{self._CLUSTER_ID}",
                    headers={"Authorization": f"Bearer {self._IAM_TOKEN}"},
                    timeout=60 * 2,
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API and no more retries left. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    sleep(self._DELAY)
                    continue

            if response.status_code == 200:
                self.logger.debug("Response recieved")

                response = response.json()
                self.logger.debug(f"API response:\n{response}")

                # fmt: off
                status_key = next(_ for _ in response.keys() if re.search("status", _, re.IGNORECASE))

                # fmt: on
                if status_key in response.keys():
                    self.logger.info(
                        f"Current cluster status is: {response.get(status_key, 'unknown')}"
                    )
                    if response[status_key].strip().lower() == target_status:
                        self.logger.info("The target status has been reached!")
                        _IS_TARGET = True
                        break

                    else:
                        if _TRY == self._MAX_RETRIES:
                            raise YandexAPIError(
                                "No more retries left to check Cluster status!\n"
                                f"Last received status was: {response.get('status', 'unknown')}"
                            )
                        else:
                            self.logger.debug(
                                "Not target yet. Will make another try after delay"
                            )
                            _TRY += 1
                            sleep(self._DELAY)
                            continue
                else:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError(
                            "Enable to get 'status' from API response. No more retries left"
                        )
                    else:
                        self.logger.warning(
                            "No 'status' in API response. Will make another try after delay"
                        )
                        _TRY += 1
                        sleep(self._DELAY)
                        continue
            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        "Enable to get Cluster status from API. No more retries left"
                    )
                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Will make another try after delay"
                    )
                    _TRY += 1
                    sleep(self._DELAY)
                    continue
