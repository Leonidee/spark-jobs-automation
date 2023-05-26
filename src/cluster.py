import sys
from logging import getLogger
from os import getenv
from pathlib import Path
from time import sleep
from typing import Literal

import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.environ import EnvironManager


class DataProcCluster:
    """Class for manage DataProc Clusters. Sends requests to Yandex Cloud API.

    ## Notes
    To initialize Class instance you need to specify environment variables in `.env` project file or as a global environment variables. See `.env.template` for mote details.

    At iinitializing moment gets IAM token to communicate to Yandex Cloud API.

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

    We can change max attempts to check cluster status:
    >>> cluster.max_attempts_to_check_status = 1
    >>> cluster.check_status(target_status="stopped")
    ... [2023-05-26 13:03:38] {src.cluster:140} INFO: Sending request to check Cluster status. Target status: stopped
    ... [2023-05-26 13:03:39] {src.cluster:163} INFO: Current cluster status is: RUNNING
    ... [2023-05-26 13:03:39] {src.cluster:173} ERROR: No more attemts left to check Cluster status! Cluster status is unknown.
    """

    def __init__(self, max_attempts_to_check_status: int = 10) -> None:
        config = Config()
        env = EnvironManager()
        env.load_environ()
        self.logger = (
            getLogger("aiflow.task")
            if config.IS_PROD
            else SparkLogger(level=config.python_log_level).get_logger(
                logger_name=__name__
            )
        )

        self.cluster_id = getenv("YC_DATAPROC_CLUSTER_ID")
        self.base_url = getenv("YC_DATAPROC_BASE_URL")
        self.token = self._get_iam_token()
        self._max_attempts_to_check_status = max_attempts_to_check_status

    @property
    def max_attempts_to_check_status(self) -> int:
        """
        Max attempts to check Cluster status, defaults 10. Must be lower than 20.

        Between each attempt will pause on 60 seconds.

        ## Examples
        You can change attribute this way:
        >>> cluster.max_attempts_to_check_status = 2
        """
        return self._max_attempts_to_check_status

    @max_attempts_to_check_status.setter
    def max_attempts_to_check_status(self, value: int) -> None:
        if not isinstance(value, int):
            raise ValueError("value must be integer")

        if value > 20:
            raise ValueError("value must be lower than 20")

        if value < 0:
            raise ValueError("value must be positive")

        self._max_attempts_to_check_status = value

    @max_attempts_to_check_status.deleter
    def max_attempts_to_check_status(self) -> None:
        "Resets to default value"
        self._max_attempts_to_check_status = 10

    def _get_iam_token(self) -> str:
        """
        Gets IAM token to be able to communicate with Yandex Cloud API.

        Used at the time of initialization class instance.

        ## Returns
        `str` : IAM token
        """
        OAUTH_TOKEN = getenv("YC_OAUTH_TOKEN")

        self.logger.debug("Getting Yandex Cloud IAM token")

        try:
            self.logger.debug("Send request to API")
            response = requests.post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": OAUTH_TOKEN},
                timeout=60 * 2,
            )
            response.raise_for_status()

            if response.status_code == 200:
                self.logger.debug("Response received")
                response = response.json()

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                    self.logger.debug("IAM token collected!")
                else:
                    self.logger.error("There is no IAM token in API response!")
                    sys.exit(1)

        except (HTTPError, ConnectionError, InvalidSchema, Timeout) as e:
            self.logger.exception(e)
            sys.exit(1)

        return iam_token  # type: ignore

    def exec_command(self, command: Literal["start", "stop"]) -> None:
        """Sends request to execute Cluster command.

        ## Parameters
        `command` : Command to execute
        """
        self.logger.info(f"Sending request to execute Cluster command: {command}")

        try:
            self.logger.debug("Requesting...")
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:{command}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=60 * 2,
            )
            response.raise_for_status()

            if response.status_code == 200:
                self.logger.debug("Response received")
                self.logger.debug(f"API response: {response.json()}")

        except (HTTPError, InvalidSchema, ConnectionError) as e:
            self.logger.error(e)
            sys.exit(1)

    def check_status(self, target_status: Literal["running", "stopped"]) -> None:
        """Sends request to check current Cluster status.

        Waits until Cluster status will be equal to `target_status`.

        ## Notes
        If the target status is not reached after all attempts, the application is killed with the returning code 1.

        The default `max_attempts_to_check_status` attribute set to 10 (ten attempts with 60 secs pause between each)
        You can change it like that:
        >>> cluster.max_attempts_to_check_status = 2

        ## Parameters
        `target_status` : The target Cluster status
        """
        self.logger.info(
            f"Sending request to check Cluster status. Target status: {target_status}"
        )

        is_target = False
        i = 1

        while not is_target:
            try:
                self.logger.debug(f"Requesting... Attempt: {i}")
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema, ConnectionError) as e:
                self.logger.error(e)
                sys.exit(1)

            if response.status_code == 200:
                response = response.json()
                self.logger.debug(f"API response: {response}")

                self.logger.info(
                    f"Current cluster status is: {response.get('status', 'unknown')}"
                )

                if response["status"].strip().lower() == target_status:
                    self.logger.info("The target status has been reached!")
                    is_target = True

            if not is_target:
                if i == self._max_attempts_to_check_status:
                    self.logger.error(
                        "No more attemts left to check Cluster status! Cluster status is unknown."
                    )
                    sys.exit(1)

                sleep(60)
                self.logger.debug("Another attempt to check status")
                i += 1
