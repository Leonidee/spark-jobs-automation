from time import sleep
import json
from logging import getLogger
import sys

from requests.exceptions import HTTPError, ConnectionError, InvalidSchema, Timeout
import requests

logger = getLogger("aiflow.task")


class YandexCloudAPI:
    "Instance for interaction with Yandex Cloud Rest API"

    def __init__(self) -> None:
        ...

    def get_iam_token(self, oauth_token: str) -> str:
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
                json={"yandexPassportOauthToken": oauth_token},
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
        token: str,
        cluster_id: str,
        base_url: str,
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
        self.token = token
        self.cluster_id = cluster_id
        self.base_url = base_url
        self.max_attempts_to_check_status = max_attempts_to_check_status

    def start(self) -> bool:
        """Send request to start Cluster

        Returns:
            bool: True if request was sent successfully and False if not
        """
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
                return True

        except (HTTPError, InvalidSchema) as e:
            logger.error(e)
            sys.exit(1)

        except ConnectionError as e:
            logger.exception(e)
            sys.exit(1)

    def stop(self) -> bool:
        """Send request to stop Cluster

        Returns:
            bool: True if request was sent successfully and False if not
        """
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
                return True

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
        is_active = False
        i = 1

        logger.info("Requesting Cluster status...")

        while is_active == False:
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

                if "status" in response.keys():
                    logger.info(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "running":
                        logger.info("Cluster is ready!")
                        is_active = True
                        return is_active
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
        is_stopped = False
        i = 1

        logger.info("Requesting Cluster status...")

        while is_stopped == False:
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

                if "status" in response.keys():
                    print(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "stopped":
                        logger.info("Cluster is stopped!")
                        is_stopped = True
                        return is_stopped

            if i == self.max_attempts_to_check_status:
                logger.error(
                    "No more attemts left to check Cluster status! Cluster status is unknown."
                )
                sys.exit(1)
            else:
                sleep(20)
                i += 1


class SparkSubmitter:
    def __init__(self, api_base_url: str, session_timeout: int = 60 * 60) -> None:
        """Sends request to Fast API upon Hadoop Cluster to submit Spark jobs. \n
        Each Class method should contains different Spark job

        Args:
            api_base_url (str): Fast API base URL on Cluster side
            session_timeout (int, optional): `requests` module standard session timeout
        """
        self.api_base_url = api_base_url
        self.session_timeout = session_timeout

    def submit_tags_job(
        self,
        date: str,
        depth: int | str,
        threshold: int | str,
        tags_verified_path: str,
        src_path: str,
        tgt_path: str,
    ) -> bool:
        """Send request to API to submit tags job

        Args:
            date (str): Report start date. Format: YYYY-MM-DD
            depth (int): Report lenght in days. Report start date minus depth
            threshold (int): Users threshold
            tags_verified_path (str): s3 path with  tags_verified dataset. Tags from this dataset will be exluded from result
            src_path (str): s3 path with source dataset partitions
            tgt_path (str): s3 path where Spark will store the results

        Returns:
            bool: State of submited job. True if job submit successfully and False if failed
        """

        args = dict(
            date=date,
            depth=str(depth),
            threshold=str(threshold),
            tags_verified_path=tags_verified_path,
            src_path=src_path,
            tgt_path=tgt_path,
        )
        logger.info("Requesting API to submit `tags` job.")

        logger.info(
            f"Spark job args:\n\t`date`: {args['date']}\n\t`depth`: {args['depth']}\n\t`threshold`: {args['threshold']}"
        )
        args = json.dumps(args)
        logger.debug(args)

        is_done = False
        try:
            logger.info("Processing...")
            response = requests.post(
                url=f"{self.api_base_url}/submit_tags_job",
                timeout=self.session_timeout,
                data=args,
            )
            response.raise_for_status()

        except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
            logger.exception(e)
            sys.exit(1)

        if response.status_code == 200:
            logger.info("Response received!")

            if response.json()["returncode"] == 0:
                logger.info(
                    f"Spark Job was executed successfully! Results -> `{tgt_path}`"
                )
                is_done = True
                return is_done
            else:
                logger.error("Unable to submit spark job! API returned non-zero code")
                sys.exit(1)
