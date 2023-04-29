import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from src.logger import SparkLogger
from src.utils import TagsJobArgsHolder

if __name__ == "__main__":
    holder = TagsJobArgsHolder(
        date="...",
        depth=8,
        threshold=100,
        tags_verified_path="...",
        src_path="...",
        tgt_path="...",
    )
    print(holder.json())
