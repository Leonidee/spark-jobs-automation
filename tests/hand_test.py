import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.notifyer import TelegramNotifyer
from src.submitter import SparkSubmitter


def main():
    tg = TelegramNotifyer()
    tg.retry_delay = 23
    print(tg._DELAY)


if __name__ == "__main__":
    main()
