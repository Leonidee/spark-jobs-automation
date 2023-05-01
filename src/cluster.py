import sys
from logging import getLogger
from os import getenv
from pathlib import Path
from time import sleep

from requests import get, post
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import load_environment

config = Config()
load_environment()

logger = (
    getLogger("aiflow.task")
    if config.IS_PROD
    else SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))
)


class DataProcCluster:
    def __init__(
        self,
        max_attempts_to_check_status: int = 10,
    ) -> None:
        """
        Instance for manage DataProc Clusters. Sends requests to Yandex Cloud API

        Args:
            max_attempts_to_check_status (int, optional): Max number of attempts to check if cluster running or stopped. Defaults to 10.
        """
        self.max_attempts_to_check_status = max_attempts_to_check_status
        self.cluster_id = getenv("YC_DATAPROC_CLUSTER_ID")
        self.base_url = getenv("YC_DATAPROC_BASE_URL")
        self.token = self._get_iam_token()

    def _get_iam_token(self) -> str:
        """
        Get IAM token to be able to work with the API

        Returns:
            str: IAM token
        """
        OAUTH_TOKEN = getenv("YC_OAUTH_TOKEN")

        logger.debug("Getting Yandex Cloud IAM token")

        try:
            logger.debug("Send request to API")
            response = post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": OAUTH_TOKEN},
                timeout=60 * 2,
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.debug("Response received")
                response = response.json()

                logger.debug(f"API response: {response}")

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                    logger.debug("IAM token collected!")
                else:
                    logger.error("There is no IAM token in API response!")
                    sys.exit(1)

        except (HTTPError, ConnectionError, InvalidSchema, Timeout) as e:
            logger.exception(e)
            sys.exit(1)

        return iam_token

    def start(self) -> None:
        """Send request to start DataProc Cluster"""
        logger.info("Starting Cluster")

        try:
            logger.debug("Sending request to API")
            response = post(
                url=f"{self.base_url}/{self.cluster_id}:start",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=60 * 2,
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.debug("Response received")
                logger.debug(f"API response: {response.json()}")

        except (HTTPError, InvalidSchema, ConnectionError) as e:
            logger.error(e)
            sys.exit(1)

    def stop(self) -> None:
        """Send request to stop DataProc Cluster"""
        logger.info("Stopping Cluster")
        try:
            logger.debug("Sending request to API")
            response = post(
                url=f"{self.base_url}/{self.cluster_id}:stop",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.debug("Response received")
                logger.debug(f"API response: {response.json()}")

        except (HTTPError, InvalidSchema, ConnectionError) as e:
            logger.error(e)
            sys.exit(1)

    def is_running(self) -> None:
        """Send request to check current Cluster status. \n
        Waits until cluster status will be `RUNNING`. \n
        If current attempt greater then `max_attempts_to_check_status` attribute will exit with 1 status code
        """
        logger.info("Requesting Cluster status...")

        is_active = False
        i = 1

        while not is_active:
            try:
                logger.debug(f"Send request to API. Attempt: {i}")
                response = get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema, ConnectionError) as e:
                logger.error(e)
                sys.exit(1)

            if response.status_code == 200:
                response = response.json()
                logger.debug(f"API response: {response}")

                logger.info(
                    f"Current cluster status is: {response.get('status', 'unknown')}"
                )

                if response["status"].strip().lower() == "running":
                    logger.info("Cluster is ready!")
                    is_active = True

            if i == self.max_attempts_to_check_status:
                logger.error(
                    "No more attemts left to check Cluster status! Cluster status is unknown."
                )
                sys.exit(1)
            else:
                sleep(20)
                logger.debug("Another attempt to check status")
                i += 1

    def is_stopped(self) -> None:
        """Send request to Yandex API to check current Cluster status. \n
        Waits until cluster status will be `STOPPED`. \n
        If current attempt greater then `max_attempts_to_check_status` attribute will exit with 1 status code
        """

        logger.info("Requesting Cluster status...")

        is_stopped = False
        i = 1

        while not is_stopped:
            try:
                logger.debug(f"Send request to API. Attempt: {i}")
                response = get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema, ConnectionError) as e:
                logger.error(e)
                sys.exit(1)

            if response.status_code == 200:
                response = response.json()
                logger.debug(f"API response: {response}")

                logger.info(
                    f"Current cluster status is: {response.get('status', 'unknown')}"
                )

                if response["status"].strip().lower() == "stopped":
                    logger.info("Cluster is stopped!")

                    is_stopped = True

            if i == self.max_attempts_to_check_status:
                logger.error(
                    "No more attemts left to check Cluster status! Cluster status is unknown."
                )
                sys.exit(1)
            else:
                sleep(20)
                logger.debug("Another attempt to check status")
                i += 1
