import sys
from pathlib import Path
from datetime import date, timedelta

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.datamodel import ArgsKeeper


class TestArgsKeeper:
    def test_argskeeper_date_valid(self, keeper):
        assert keeper.date == "2022-04-03"

    def test_argskeeper_depth_valid(self, keeper):
        assert keeper.depth == 10

    def test_argskeeper_src_path_valid(self, keeper):
        assert keeper.src_path == "s3a://..."

    def test_argskeeper_tgt_path_valid(self, keeper):
        assert keeper.tgt_path == "s3a://..."

    def test_argskeeper_processed_dttm_valid(self, keeper):
        assert keeper.processed_dttm == "2023-05-22T12:03:25"

    def test_argskeeper_date_raises_if_wrong_pattern(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2023",
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22T12:03:25",
            )

    def test_argskeeper_date_raises_if_too_early(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2013-01-01",
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22T12:03:25",
            )

    def test_argskeeper_date_raises_if_gt_today(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date=(date.today() + timedelta(days=2)).strftime("%Y-%m-%d"),
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22T12:03:25",
            )

    def test_argskeeper_src_path_raises_if_wrong_pattern(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2022-04-03",
                depth=10,
                src_path="/path/to/local/data",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22T12:03:25",
            )

    def test_argskeeper_tgt_path_raises_if_wrong_pattern(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2022-04-03",
                depth=10,
                src_path="s3a://...",
                tgt_path="/path/to/local/data",
                processed_dttm="2023-05-22T12:03:25",
            )

    def test_argskeeper_processed_dttm_if_wrong_pattern(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2022-04-03",
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22 12:03:25",
            )

    def test_argskeeper_processed_dttm_if_wrong_pattern_2(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2022-04-03",
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22T12:03:25.000000",
            )

    def test_argskeeper_processed_dttm_if_wrong_pattern_3(self):
        with pytest.raises(ValueError):
            ArgsKeeper(
                date="2022-04-03",
                depth=10,
                src_path="s3a://...",
                tgt_path="s3a://...",
                processed_dttm="2023-05-22",
            )
