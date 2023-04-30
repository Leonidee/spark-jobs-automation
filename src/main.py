import sys
from logging import getLogger
from os import getenv
from pathlib import Path
from time import sleep

import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.utils import TagsJobArgsHolder, load_environment

config = Config()
load_environment()

logger = (
    getLogger("aiflow.task")
    if config.IS_PROD
    else SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))
)


class YandexCloudAPI:
    "Instance for interaction with Yandex Cloud Rest API"

    def __init__(self) -> None:
        self.oauth_token = getenv("YC_OAUTH_TOKEN")

    def get_iam_token(self) -> str:
        """
        Get IAM token by specifying OAuth token

        Args:
            oauth_token (str): Yandex Cloud OAuth token

        Returns:
            str: IAM token
        """
        logger.info("Getting Yandex Cloud IAM token")
        iam_token = None
        try:
            response = requests.post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": self.oauth_token},
                timeout=60 * 2,
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.info("Response received")
                response = response.json()

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                    logger.info("IAM token collected!")
                else:
                    logger.error("There is no IAM token in API response!")
                    sys.exit(1)

        except (HTTPError, ConnectionError, InvalidSchema, Timeout) as e:
            logger.exception(e)
            sys.exit(1)

        return iam_token


class DataProcCluster:
    def __init__(
        self,
        iam_token: str,
        max_attempts_to_check_status: int = 10,
    ) -> None:
        """
        Instance for manage DataProc Clusters. Sends requests to Yandex Cloud API

        Args:
            token (str): Yandex Cloud IAM token
            cluster_id (str): Cluster id to interact with
            base_url (str): API base url
            max_attempts_to_check_status (int, optional): Max number of attempts to check if cluster running or stopped. Defaults to 10.
        """
        self.token = iam_token
        self.max_attempts_to_check_status = max_attempts_to_check_status
        self.cluster_id = getenv("YC_DATAPROC_CLUSTER_ID")
        self.base_url = getenv("YC_DATAPROC_BASE_URL")

    def start(self) -> None:
        """Send request to start DataProc Cluster"""
        logger.info("Starting Cluster")

        try:
            logger.info("Sending request to API")
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:start",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=60,
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.info("Request was sent")

        except (HTTPError, InvalidSchema) as e:
            logger.error(e)
            sys.exit(1)

        except ConnectionError as e:
            logger.exception(e)
            sys.exit(1)

    def stop(self) -> None:
        """Send request to stop DataProc Cluster"""
        logger.info("Stopping Cluster")
        try:
            logger.info("Sending request to API")
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:stop",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.info("Request was sent")

        except (HTTPError, InvalidSchema) as e:
            logger.error(e)
            sys.exit(1)

        except ConnectionError as e:
            logger.exception(e)
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
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema) as e:
                logger.error(e)
                sys.exit(1)

            except ConnectionError as e:
                logger.exception(e)
                sys.exit(1)

            if response.status_code == 200:
                response = response.json()

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
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
                response.raise_for_status()
            except (HTTPError, InvalidSchema) as e:
                logger.error(e)
                sys.exit(1)

            except ConnectionError as e:
                logger.exception(e)
                sys.exit(1)

            if response.status_code == 200:
                response = response.json()

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
                i += 1


class SparkSubmitter:
    def __init__(self, session_timeout: int = 60 * 60) -> None:
        """Sends request to Fast API upon Hadoop Cluster to submit Spark jobs. \n
        Each Class method should contains different Spark job

        Args:
            api_base_url (str): Fast API base URL on Cluster side
            session_timeout (int, optional): `requests` module standard session timeout
        """
        self.session_timeout = session_timeout
        self.api_base_url = getenv("FAST_API_BASE_URL")

    def submit_tags_job(self, holder: TagsJobArgsHolder) -> None:
        """Send request to API to submit tags job

        Args:
            holder (TagsJobArgsHolder): Argument for submiting tags job inside `TagsJobArgsHolder` object
        """

        logger.info("Requesting API to submit `tags` job.")

        logger.info(f"Spark job args:\n{holder}")

        try:
            logger.info("Processing...")
            response = requests.post(
                url=f"{self.api_base_url}/submit_tags_job",
                timeout=self.session_timeout,
                data=holder.json(),
            )
            response.raise_for_status()

        except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
            logger.exception(e)
            sys.exit(1)

        if response.status_code == 200:
            logger.info("Response received!")

            response = response.json()

            if response.get("returncode") == 0:
                logger.info(
                    f"Spark Job was executed successfully! Results -> `{holder.tgt_path}`"
                )
                logger.info(f"Job stdout:\n{response.get('stdout')}")
                logger.info(f"Job stderr:\n{response.get('stderr')}")

            else:
                logger.error("Unable to submit spark job! API returned non-zero code")
                logger.error(f"Job stdout:\n{response.get('stdout')}")
                logger.error(f"Job stderr:\n{response.get('stderr')}")
                sys.exit(1)
