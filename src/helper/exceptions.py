class S3ServiceError(Exception):
    def __init__(self, msg: str) -> None:
        """Failed to communicate with S3 service.

        While downloading, uploading files or getting information about them.

        `msg` : Error message to raise
        """
        super().__init__(msg)
