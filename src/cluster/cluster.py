from __future__ import annotations

import sys
import os
from pathlib import Path
import time
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal

import requests

from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    InvalidURL,
    MissingSchema,
    JSONDecodeError,
)

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.environ import EnvironManager
from src.cluster.exceptions import YandexAPIError
from src.base import BaseRequestHandler


class DataProcCluster(BaseRequestHandler):
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

    def __init__(
        self,
        *,
        max_retries: int = 10,
        retry_delay: int = 60,
        session_timeout: int = 60 * 2,
    ) -> None:
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            session_timeout=session_timeout,
        )

        environ = EnvironManager()
        environ.load_environ()

        _REQUIRED_VARS = (
            "YC_DATAPROC_CLUSTER_ID",
            "YC_DATAPROC_BASE_URL",
            "YC_OAUTH_TOKEN",
            "YC_IAM_TOKEN",
        )

        self._CLUSTER_ID, self._BASE_URL, self._OAUTH_TOKEN, self._IAM_TOKEN = map(
            os.getenv, _REQUIRED_VARS
        )

        if not self._IAM_TOKEN:
            self._get_iam_token()
            self._IAM_TOKEN = os.getenv("YC_IAM_TOKEN")

        environ.check_environ(var=_REQUIRED_VARS)  # type: ignore

    def _get_iam_token(self) -> bool:  # type: ignore
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
                    timeout=self._SESSION_TIMEOUT,
                )
                response.raise_for_status()

            except Timeout:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Timeout error. Unable to send request")

                self.logger.warning("Timeout error occured. Retrying...")
                _TRY += 1
                time.sleep(self._DELAY)
                continue

            except (InvalidSchema, InvalidURL, MissingSchema):
                raise YandexAPIError("Invalid url or schema provided")

            except (HTTPError, ConnectionError) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue

            if response.status_code == 200:
                self.logger.debug("Response received")

                try:
                    self.logger.debug("Decoding response")
                    response = response.json()

                except JSONDecodeError as e:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError(
                            f"Unable to decode API reponse.\n"
                            f"Decode error was -> {e}"
                        )
                    else:
                        self.logger.warning(
                            "Enable to decode API response. Retrying..."
                        )
                        _TRY += 1
                        time.sleep(self._DELAY)
                        continue
                try:
                    # fmt: off
                    token_key = next(_ for _ in response.keys() if re.search("iamtoken", _, re.IGNORECASE))

                    # fmt: on
                    self.logger.debug("IAM token collected!")
                    os.environ["YC_IAM_TOKEN"] = response[token_key]
                    _OK = True
                    return _OK

                except StopIteration:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError(
                            "Enable to get IAM token from API response"
                        )
                    else:
                        self.logger.warning("No IAM token in API response. Retrying...")
                        _TRY += 1
                        time.sleep(self._DELAY)
                        continue

            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Enable to get IAM token from API")
                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Will make another try after delay"
                    )
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue

    def exec_command(self, command: Literal["start", "stop"]) -> bool:  # type: ignore
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
                    timeout=self._SESSION_TIMEOUT,
                )
                response.raise_for_status()

            except Timeout:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Timeout error. Unable to send request to execute command: {command}"
                    )

                self.logger.warning("Timeout error occured. Retrying...")
                _TRY += 1
                time.sleep(self._DELAY)
                continue

            except (InvalidSchema, InvalidURL, MissingSchema):
                raise YandexAPIError(
                    "Invalid url or schema provided.\n"
                    "Please check 'YC_DATAPROC_BASE_URL' and 'YC_DATAPROC_CLUSTER_ID' environment variables"
                )

            except (HTTPError, ConnectionError) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API. Possible because of exception:\n{e}"
                    )

                self.logger.warning(
                    "An error occured! See traceback below. Will make another try after delay"
                )
                self.logger.exception(e)
                _TRY += 1
                time.sleep(self._DELAY)
                continue

            if response.status_code == 200:
                self.logger.debug("Response received")
                self.logger.info("Command in progress")
                _OK = True
                return _OK

            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Enable send request to API!")

                self.logger.warning(
                    "Ops, seems like something went wrong. Will make another try after delay"
                )
                _TRY += 1
                time.sleep(self._DELAY)
                continue

    def check_status(self, target_status: Literal["running", "stopped"]) -> bool:  # type: ignore
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
                    timeout=self._SESSION_TIMEOUT,
                )
                response.raise_for_status()

            except Timeout:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Timeout error. Unable to get cluster status")

                self.logger.warning("Timeout error occured. Retrying...")
                _TRY += 1
                time.sleep(self._DELAY)
                continue

            except (InvalidSchema, InvalidURL, MissingSchema):
                raise YandexAPIError(
                    "Invalid url or schema provided.\n"
                    "Please check 'YC_DATAPROC_BASE_URL' and 'YC_DATAPROC_CLUSTER_ID' environment variables"
                )

            except (HTTPError, ConnectionError) as e:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError(
                        f"Enable to send request to API. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue

            if response.status_code == 200:
                self.logger.debug("Response recieved")

                try:
                    self.logger.debug("Decoding response")
                    response = response.json()

                except JSONDecodeError as e:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError(
                            f"Unable to decode API reponse.\n"
                            f"Decode error was -> {e}"
                        )
                    else:
                        self.logger.warning(
                            "Enable to decode API response. Retrying..."
                        )
                        _TRY += 1
                        time.sleep(self._DELAY)
                        continue
                try:
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
                            return _IS_TARGET
                    else:
                        if _TRY == self._MAX_RETRIES:
                            raise YandexAPIError(
                                "No more retries left to check Cluster status!\n"
                                f"Last received status was: {response.get('status', 'unknown')}"
                            )
                        else:
                            self.logger.debug("Not target yet")
                            _TRY += 1
                            time.sleep(self._DELAY)
                            continue

                except StopIteration:
                    if _TRY == self._MAX_RETRIES:
                        raise YandexAPIError("Enable to get 'status' from API response")
                    else:
                        self.logger.warning("No 'status' in API response")
                        _TRY += 1
                        time.sleep(self._DELAY)
                        continue

            else:
                if _TRY == self._MAX_RETRIES:
                    raise YandexAPIError("Enable to get Cluster status from API")
                else:
                    self.logger.warning(
                        "Ops, seems like something went wrong. Will make another try after delay"
                    )
                    _TRY += 1
                    time.sleep(self._DELAY)
                    continue
