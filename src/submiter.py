import sys
from logging import getLogger
from os import getenv
from pathlib import Path

from requests import post
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


class SparkSubmitter:
    def __init__(self, session_timeout: int = 60 * 60) -> None:
        """Sends request to Fast API upon Hadoop Cluster to submit Spark jobs. \n
        Each Class method should contains different Spark job

        Args:
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
            response = post(
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
