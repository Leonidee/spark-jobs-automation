class EnvironError(Exception):
    def __init__(self, msg: str) -> None:
        """Can be raised if EnvironManager enable to load environment variables.

        ## Parameters
        `msg` : Error message
        """
        super().__init__(msg)
