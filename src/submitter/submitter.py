from __future__ import annotations

import sys
from logging import getLogger
from os import getenv
from pathlib import Path

from typing import TYPE_CHECKING


import requests
from requests.exceptions import ConnectionError, HTTPError, InvalidSchema, Timeout

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.environ import EnvironManager, EnvironError

if TYPE_CHECKING:
    from typing import Literal
    from src.datamodel import ArgsKeeper


class SparkSubmitter:
    """Sends request to Fast API upon Hadoop Cluster to submit Spark jobs.

    ## Notes
    To initialize instance of Class you need to specify `FAST_API_BASE_URL` in `.env` project file or as a global environment variable.
    """

    def __init__(self, session_timeout: int = 60 * 60) -> None:
        config = Config()

        self.logger = (
            getLogger("aiflow.task")
            if config.IS_PROD
            else SparkLogger(level=config.python_log_level).get_logger(
                logger_name=__name__
            )
        )
        try:
            env = EnvironManager()
            env.load_environ()
        except EnvironError as e:
            self.logger.critical(e)

        self.api_base_url = getenv("FAST_API_BASE_URL")
        self._session_timeout = session_timeout

    @property
    def session_timeout(self) -> int:
        self._session_timeout = 60 * 60
        return self._session_timeout

    @session_timeout.setter
    def session_timeout(self, v: int) -> None:
        if not isinstance(v, int):
            raise ValueError("value must be integer")
        if v < 0:
            raise ValueError("must be positive")

        self._session_timeout = v

    def submit_job(self, job: Literal["users_info_datamart_job", "location_zone_agg_datamart_job", "friend_recommendation_datamart_job"], keeper: ArgsKeeper) -> bool:  # type: ignore
        """Sends request to API to submit Spark job in Hadoop Cluster.

        ## Parameters
        `job` : Spark Job to submit
        `keeper` : Arguments keeper object

        ## Returns
        `bool` : State of submit operation

        ## Examples
        Initialize Class instance:
        >>> submitter = SparkSubmitter()

        Send request to submit 'users_info_datamart_job.py' job:
        >>> submitter.submit_job(job="users_info_datamart_job", keeper=keeper)
        """
        self.logger.info(f"Submiting {job}.py job")

        s = "".join(
            f"\t{i[0]}: {i[1]}\n" for i in keeper
        )  # for print job arguments in logs
        self.logger.info("Spark job args:\n" + s)

        try:
            self.logger.debug("Send request to API")
            response = requests.post(
                url=f"{self.api_base_url}/submit_{job}",
                timeout=self.session_timeout,
                data=keeper.json(),
            )
            self.logger.debug("Processing...")
            response.raise_for_status()

        except (HTTPError, InvalidSchema, ConnectionError, Timeout) as e:
            self.logger.exception(e)
            return False

        if response.status_code == 200:
            self.logger.debug("Response received")

            response = response.json()
            self.logger.debug(f"API response: {response}")

            if response.get("returncode") == 0:
                self.logger.info(
                    f"{job} job was executed successfully! Results -> {keeper.tgt_path}"
                )
                self.logger.debug(f"Job stdout:\n\n{response.get('stdout')}")
                self.logger.debug(f"Job stderr:\n\n{response.get('stderr')}")
                return True

            else:
                self.logger.error(
                    "Unable to submit spark job! API returned non-zero code"
                )
                self.logger.error(f"Job stdout:\n\n{response.get('stdout')}")
                self.logger.error(f"Job stderr:\n\n{response.get('stderr')}")
                return False