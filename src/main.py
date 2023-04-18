import requests
from time import sleep
from requests import HTTPError
import json


class YandexCloudAPI:
    def __init__(self, oauth_token) -> None:
        self.oauth_token = oauth_token

    def get_iam_token(self) -> str:
        iam_token = ""

        try:
            response = requests.post(
                url="https://iam.api.cloud.yandex.net/iam/v1/tokens",
                json={"yandexPassportOauthToken": self.oauth_token},
            )
            if response.status_code == 200:
                response = response.json()

                if "iamToken" in response.keys():
                    iam_token = response["iamToken"]
                else:
                    print("There is no iamToken in API response.")

        except HTTPError as e:
            raise e

        return iam_token


class DataProcCluster:
    def __init__(self, token, cluster_id, base_url) -> None:
        self.token = token
        self.cluster_id = cluster_id
        self.base_url = base_url

    def start(self) -> None:
        try:
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:start",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            print(response.json())
        except HTTPError as e:
            raise e

    def stop(self) -> None:
        try:
            response = requests.post(
                url=f"{self.base_url}/{self.cluster_id}:stop",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            print(response.json())
        except HTTPError as e:
            raise e

    def is_running(self) -> None:
        is_active = False

        while is_active == False:
            response = requests.get(
                url=f"{self.base_url}/{self.cluster_id}",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            response.raise_for_status()

            if response.status_code == 200:
                response = response.json()

                if "status" in response.keys():
                    print(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "running":
                        print("Cluster is ready!")
                        is_active = True
                        break

            sleep(10)

    def is_stopped(self) -> None:
        is_stopped = False

        while is_stopped == False:
            try:
                response = requests.get(
                    url=f"{self.base_url}/{self.cluster_id}",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
            except HTTPError:
                pass

            if response.status_code == 200:
                response = response.json()

                if "status" in response.keys():
                    print(f"Current cluster status is: {response['status']}")

                    if response["status"].strip().lower() == "stopped":
                        is_stopped = True
                        break

            sleep(10)


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
        args = json.dumps(args)
        try:
            response = requests.post(
                url=f"{self.base_url}/submit_tags_job",
                timeout=self.session_timeout,
                data=args,
            )

        except HTTPError as e:
            raise e

        if response.status_code == 200:
            if response.json()["returncode"] == 0:
                print("Vse zaebca!")
            else:
                print("wrong")


if __name__ == "__main__":
    import os
    from dotenv import find_dotenv, load_dotenv

    find_dotenv()
    load_dotenv()

    YC_DATAPROC_CLUSTER_ID = os.getenv("YC_DATAPROC_CLUSTER_ID")
    YC_DATAPROC_BASE_URL = os.getenv("YC_DATAPROC_BASE_URL")
    YC_OAUTH_TOKEN = os.getenv("YC_OAUTH_TOKEN")

    FAST_API_BASE_URL = os.getenv("FAST_API_BASE_URL")

    # yc = YandexCloudAPI(oauth_token=YC_OAUTH_TOKEN)
    # IAM_TOKEN = yc.get_iam_token()

    # dataproc = DataProcCluster(token=IAM_TOKEN, cluster_id=CLUSTER_ID, base_url=BASE_URL)
    # dataproc.start()
    # dataproc.is_running()

    spark = Spark(base_url=FAST_API_BASE_URL, session_timeout=60 * 60)
    r = spark.do_tags_job(
        date="2022-05-05",
        depth="2",
        threshold="100",
        tags_verified_path="s3a://data-ice-lake-04/messager-data/snapshots/tags_verified/actual",
        src_path="s3a://data-ice-lake-04/messager-data/analytics/cleaned-events",
        tgt_path="s3a://data-ice-lake-04/messager-data/analytics/tag-candidates",
    )
    print(r)
