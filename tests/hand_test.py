import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.environ import EnvironManager
from src.config import Config


def main():
    env = EnvironManager()
    os.environ["YC_DATAPROC_CLUSTER_ID"] = "some"
    os.environ["YC_DATAPROC_BASE_URL"] = "some"
    # env_ = ("YC_DATAPROC_CLUSTER_ID", "YC_DATAPROC_BASE_URL")
    # env.check_environ(env=env_)

    _ENV = (
        "YC_DATAPROC_CLUSTER_ID",
        "YC_DATAPROC_BASE_URL",
        "YC_OAUTH_TOKEN",
        "YC_IAM_TOKEN",
    )
    # print(_ENV[:3])
    a, b, c = map(os.getenv, _ENV[:3])
    print(a)
    print(b)


if __name__ == "__main__":
    main()
