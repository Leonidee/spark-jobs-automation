from time import sleep
import json
from logging import getLogger
import sys
from pathlib import Path

from requests.exceptions import HTTPError, ConnectionError, InvalidSchema
import requests

# package
sys.path.append(str(Path(__file__).parent.parent))
from src.exeptions import APIServiceError, SparkSubmitError, YandexCloudAPIError
from jobs.logger import SparkLogger

# logger = getLogger("aiflow.task")
logger = SparkLogger().get_logger(logger_name=str(Path(Path(__file__).name)))


class YandexCloudAPI:
    def __init__(self) -> None:
        pass

    def get_iam_token(self, oauth_token: str) -> str:
        logger.info("Getting Yandex Cloud IAM token")
        iam_token = None
        try:
            response = requests.post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": oauth_token},
            )
            response.raise_for_status()

            if response.status_code == 200:
                logger.info("Response received")
                response = response.json()

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                    logger.info("IAM token collected!")
                else:
                    raise YandexCloudAPIError("There is no IAM token in API response!")

        except (requests.HTTPError, YandexCloudAPIError) as e:
            logger.exception(e)
            sys.exit(1)

        return iam_token


class DataProcCluster:
    def __init__(self, token, cluster_id, base_url) -> None:
        self.token = token
        self.cluster_id = cluster_id
        self.base_url = base_url

    def start(self) -> bool:
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
        is_active = False
        i = 1

        logger.info("Requesting Cluster status...")

        while is_active == False:
            try:
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
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
                        break
            if i == 3:
                logger.error(
                    "No more attemts left to check Cluster status! Cluster isn't running."
                )
                sys.exit(1)
            else:
                sleep(10)
                i += 1

    def is_stopped(self) -> None:
        is_stopped = False
        i = 1

        logger.info("Requesting Cluster status...")

        while is_stopped == False:
            try:
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
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
                        break

            if i > 3:
                logger.error(
                    "No more attemts left to check Cluster status! Cluster isn't running."
                )
                sys.exit(1)
            else:
                sleep(10)
                i += 1


class Spark:
    def __init__(self, base_url: str, session_timeout) -> None:
        self.base_url = base_url
        self.session_timeout = session_timeout

    def do_tags_job(
        self,
        date: str,
        depth: int,
        threshold: int,
        tags_verified_path: str,
        src_path: str,
        tgt_path: str,
    ) -> bool:
        args = dict(
            date=date,
            depth=str(depth),
            threshold=str(threshold),
            tags_verified_path=tags_verified_path,
            src_path=src_path,
            tgt_path=tgt_path,
        )
        logger.info("Requesting Cluster to submit `tags` job.")

        logger.info(
            f"Spark job args:\ndate - {args['date']}\ndepth - {args['depth']}\nmax users threshold - {args['threshold']}"
        )
        args = json.dumps(args)
        try:
            logger.info("Processing...")
            response = requests.post(
                url=f"{self.base_url}/submit_tags_job",
                timeout=self.session_timeout,
                data=args,
            )

        except HTTPError as e:
            logger.exception("Bad API request! Something went wrong.")
            raise e

        if response.status_code == 200:
            logger.info("Response received.")
            if response.json()["returncode"] == 0:
                logger.info(
                    f"Spark Job was submited successfully! Check {tgt_path} path to see results."
                )
            else:
                logger.exception("Unable to submit spark job! Something went wrong.")
                raise SparkSubmitError
