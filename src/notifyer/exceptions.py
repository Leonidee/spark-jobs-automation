from __future__ import annotations


class TelegramBaseError(Exception):
    ...


class AirflowContextError(TelegramBaseError):
    def __init__(self, msg: str) -> None:
        """Can be occur when enable to get airflow context from executing task.

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)


class UnableToSendMessage(TelegramBaseError):
    def __init__(self, msg: str) -> None:
        """Can be occur when enable to send telegram message.

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)
