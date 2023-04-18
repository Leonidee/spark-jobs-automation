import requests
from time import sleep
from requests import HTTPError
import json
from logging import getLogger
import sys
from pathlib import Path

# package
sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.exeptions import APIServiceError, SparkSubmitError

logger = getLogger("aiflow.task")


class YandexCloudAPI:
    def __init__(self, oauth_token) -> None:
        self.oauth_token = oauth_token

    def get_iam_token(self) -> str:
        iam_token = ""
        logger.info("Getting Yandex Cloud IAM token.")
        try:
            response = requests.post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": self.oauth_token},
            )
            if response.status_code == 200:
                logger.info("Response received.")
                response = response.json()

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                    logger.info("IAM token collected.")
                else:
                    logger.warn(
                        "There is no IAM token in API response. Something went wrong."
                    )

        except HTTPError as e:
            logger.exception("Bad API request! Something went wrong.")
            raise e

        return iam_token


class DataProcCluster:
    def __init__(self, token, cluster_id, base_url) -> None:
        self.token = token
        self.cluster_id = cluster_id
        self.base_url = base_url

    def start(self) -> None:
        logger.info("Starting Cluster.")
        try:
            logger.info("Sending request to API.")
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:start",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            logger.info("Request was send.")
        except HTTPError as e:
            logger.exception("Bad API request! Something went wrong.")
            raise e

    def stop(self) -> None:
        logger.info("Stopping Cluster.")
        try:
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:stop",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            logger.info("Request was send.")
        except HTTPError as e:
            logger.exception("Bad API request! Something went wrong.")
            raise e

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
            except HTTPError as e:
                logger.exception("Bad API request! Something went wrong.")
                raise e

            if response.status_code == 200:
                response = response.json()

                if "status" in response.keys():
                    logger.info(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "running":
                        logger.info("Cluster is ready!")
                        is_active = True
                        break
            if i > 25:
                logger.exception(
                    f"No more attemts left to check Cluster status! Cluster isn't running."
                )
                raise APIServiceError
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
            except HTTPError as e:
                logger.exception("Bad API request! Something went wrong.")
                raise e

            if response.status_code == 200:
                response = response.json()

                if "status" in response.keys():
                    print(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "stopped":
                        logger.info("Cluster is stopped!")
                        is_stopped = True
                        break

            if i > 25:
                logger.exception(
                    f"No more attemts left to check Cluster status! Cluster wasn't stopped."
                )
                raise APIServiceError
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


if __name__ == "__main__":
    import os
    from dotenv import find_dotenv, load_dotenv

    find_dotenv()
    load_dotenv()

    # YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
    # YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
    # YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")

    FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

    # yc = YandexCloudAPI(oauth_token=YC_OAUTH_TOKEN)
    # IAM_TOKEN = yc.get_iam_token()

    # dataproc = DataProcCluster(
    #     token=IAM_TOKEN,
    #     cluster_id=YC_DATAPROC_CLUSTER_ID,
    #     base_url=YC_DATAPROC_BASE_URL,
    # )
    # dataproc.start()
    # dataproc.is_running()

    spark = Spark(base_url=FAST_API_BASE_URL, session_timeout=60 * 60)
    spark.do_tags_job(
        date="2021-10-01",
        depth="5",
        threshold="50",
        tags_verified_path="s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual",
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
        tgt_path="s3a://data-ice-lake-04/messager-data/analytics/tag-candidates",
    )
