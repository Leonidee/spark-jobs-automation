class UnableToSubmitJob(Exception):
    def __init__(self, msg: str) -> None:
        """High-level exception.

        Raises if failed while submitting Spark job in Cluster

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)


class UnableToSendRequest(UnableToSubmitJob):
    """Raises if failed while sending request to Cluster API"""

    ...


class UnableToGetResponse(UnableToSubmitJob):
    """Raises if failed to get or decode response from Cluster API"""

    ...
