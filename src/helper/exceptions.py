class S3ServiceError(Exception):
    """Failed to communicate with S3 service. While downloading, uploading files or getting information about them."""

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
