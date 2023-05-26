import sys
from logging import getLogger
from os import getenv
from pathlib import Path
from time import sleep
from typing import Literal

sys.path.append(str(Path(__file__).parent.parent))
from src.cluster import DataProcCluster

if __name__ == "__main__":
    # cluster = DataProcCluster()
    # cluster.exec_command(command="stop")
    # cluster.max_attempts_to_check_status = 10
    # cluster.check_status(target_status="stopped")

    from datetime import datetime

    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S").replace(" ", "T"))
