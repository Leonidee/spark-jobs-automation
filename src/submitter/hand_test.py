import sys
from pathlib import Path


def main() -> ...:
    import os

    # ubuntu/home/opt/airflow/config.yaml
    # _PROJECT_NAME = "spark-jobs-automation"
    # _ = os.path.abspath(__file__)
    # i, _ = _.split(_PROJECT_NAME)
    # print(i, _)
    _REQUIRED_VARS = (
        "YC_DATAPROC_CLUSTER_ID",
        "YC_DATAPROC_BASE_URL",
        "YC_OAUTH_TOKEN",
        "YC_IAM_TOKEN",
    )
    print(_REQUIRED_VARS[:3])
    print(_REQUIRED_VARS[3])


if __name__ == "__main__":
    main()
