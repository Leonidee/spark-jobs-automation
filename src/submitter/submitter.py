from __future__ import annotations

import sys
from logging import getLogger
import os
from pathlib import Path
import time

from typing import TYPE_CHECKING


import requests
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    InvalidSchema,
    Timeout,
    InvalidURL,
    MissingSchema,
    JSONDecodeError,
)

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.config import Config
from src.logger import SparkLogger
from src.environ import EnvironManager
from src.submitter.exception import UnableToSubmitJob

if TYPE_CHECKING:
    from typing import Literal
    from src.datamodel import ArgsKeeper


class SparkSubmitter:
    """Sends request to Fast API upon Hadoop Cluster to submit Spark jobs.

    ## Notes
    To initialize instance of Class you need to specify `FAST_API_BASE_URL` in `.env` project file or as a global environment variable.
    """

    def __init__(self, session_timeout: int = 60 * 60) -> None:
        """_summary_

        ## Parameters
        `session_timeout` : _description_, by default 60*60
        """

        self._SESSION_TIMEOUT = session_timeout

        config = Config(config_name="config.yaml")

        self.logger = (
            getLogger("aiflow.task")
            if config.IS_PROD
            else SparkLogger(level=config.python_log_level).get_logger(
                logger_name=__name__
            )
        )
        environ = EnvironManager()
        environ.load_environ()

        _REQUIRED_VAR = "CLUSTER_API_BASE_URL"

        self._API_BASE_URL = os.getenv(_REQUIRED_VAR)
        environ.check_environ(var=_REQUIRED_VAR)

    @property
    def session_timeout(self) -> int:
        self._SESSION_TIMEOUT = 60 * 60
        return self._SESSION_TIMEOUT

    @session_timeout.setter
    def session_timeout(self, v: int) -> None:
        if not isinstance(v, int):
            raise ValueError("value must be integer")
        if v < 0:
            raise ValueError("must be positive")

        self._SESSION_TIMEOUT = v

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
        self.logger.info(f"Submiting '{job}.py' job")

        s = "".join(
            f"\t{i[0]}: {i[1]}\n" for i in keeper
        )  # for print each job argument in logs
        self.logger.info("Spark job args:\n" + s)

        _TRY = 1
        _OK = False
        _MAX_RETRIES = 3
        _DELAY = 10

        while not _OK:
            try:
                self.logger.debug(f"Requesting API. Try: {_TRY}")
                response = requests.post(
                    url=f"{self._API_BASE_URL}/submit_{job}",
                    timeout=self._SESSION_TIMEOUT,
                    data=keeper.json(),
                )
                response.raise_for_status()
                _OK = True
                break

            except Timeout:
                raise UnableToSubmitJob(f"Timeout error. Unable to submit '{job}' job.")

            except (InvalidSchema, InvalidURL, MissingSchema):
                raise UnableToSubmitJob(
                    "Invalid schema provided. Please check 'CLUSTER_API_BASE_URL' environ variable"
                )

            except (HTTPError, ConnectionError) as e:
                if _TRY == _MAX_RETRIES:
                    raise UnableToSubmitJob(
                        f"Unable to send request to API and no more retries left. Possible because of exception:\n{e}"
                    )
                else:
                    self.logger.warning(
                        "An error occured! See traceback below. Will make another try after delay"
                    )
                    self.logger.exception(e)
                    _TRY += 1
                    time.sleep(_DELAY)
                    continue

        self.logger.debug("Request sent")
        self.logger.debug("Processing requested Job on Cluster side...")

        if response.status_code == 200:  # type: ignore
            self.logger.debug("Response received")

            try:
                self.logger.debug("Decoding response")
                response = response.json()  # type: ignore

            except JSONDecodeError as e:
                raise UnableToSubmitJob(
                    f"Unable to decode API reponse.\n"
                    "Posible submiting job process failed.\n"
                    f"Decode error was -> {e}"
                )

            if response.get("returncode") == 0:
                self.logger.info(
                    f"{job} job was submitted successfully! Results -> {keeper.tgt_path}"
                )
                self.logger.debug(f"Job stdout:\n{response.get('stdout')}")
                self.logger.debug(f"Job stderr:\n{response.get('stderr')}")
                return True

            if response.get("returncode") == 1:
                self.logger.error(f"Job stdout:\n{response.get('stdout')}")
                self.logger.error(f"Job stderr:\n{response.get('stderr')}")

                raise UnableToSubmitJob(
                    f"Unable to submit {job} job! API returned 1 code. See job output in logs"
                )
            else:
                raise UnableToSubmitJob(
                    f"Unable to submit {job} job.\n" f"API response: {response}"
                )
        else:
            raise UnableToSubmitJob(
                f"Unable to submit {job} job. Something went wrong.\n"
                f"API response status code: {response.status_code}"  # type: ignore
            )
