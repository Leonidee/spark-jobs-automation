class UnableToSubmitJob(Exception):
    def __init__(self, msg: str) -> None:
        """High-level exception.

        Reises if unable request Cluster Rest API to submit Spark job for some reason.

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)
