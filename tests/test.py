import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.config import Config


def main():
    print(os.getenv("PYSPARK_PYTHON"))


if __name__ == "__main__":
    main()
