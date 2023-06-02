class EnableToGetConfig(Exception):
    def __init__(self, msg: str) -> None:
        """Can raise if enable to find or load `config.yaml` file"""
        super().__init__(msg)
