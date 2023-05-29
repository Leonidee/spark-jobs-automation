class YandexAPIError(Exception):
    def __init__(self, msg: str) -> None:
        """Occurs while interacting with Yandex Cloud Rest API.

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)
