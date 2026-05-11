from itertools import pairwise
from pathlib import Path

import numpy as np
import polars as pl

from roll_rate_analysis import MOMRollRateTable

DATA = Path(__file__).parent / "test_data"
SIM = Path(__file__).parent / "simulation_data"


def test_init_does_not_touch_filesystem(tmp_path):
    """Constructing the table only stores config; I/O is deferred to compute()."""
    bogus = tmp_path / "does-not-exist.csv"
    table = MOMRollRateTable(
        bogus,
        bogus,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    assert table.unique_key_col == "id"
    assert table.delinquency_col == "delq"
    assert table.tags[0] == "0_cycle_delinquent"
    assert table.tags[-1] == "6+_cycle_delinquent"


def test_build_max_delq_6_matches_expected_csv():
    table = MOMRollRateTable(
        DATA / "test_data_i.csv",
        DATA / "test_data_i_1.csv",
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    matrix = table.compute()
    expected = pl.read_csv(DATA / "test_mom_rr_result_6.csv").to_numpy()
    actual = matrix.drop("from_state").to_numpy()
    assert actual.shape == expected.shape == (7, 7)
    assert np.array_equal(actual, expected)


def test_compute_is_cached_and_returns_polars():
    table = MOMRollRateTable(
        DATA / "test_data_i.csv",
        DATA / "test_data_i_1.csv",
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    m1 = table.compute()
    m2 = table.matrix
    assert isinstance(m1, pl.DataFrame)
    assert m1 is m2  # cached


def test_row_and_column_sums_match_raw_data_for_simulation_files():
    files = sorted(SIM.glob("test_sample_*.csv"))
    for file_i, file_i_1 in pairwise(files):
        joined = (
            pl.scan_csv(file_i)
            .select(["id", "delq"])
            .join(
                pl.scan_csv(file_i_1).select(["id", "delq"]),
                how="left",
                on="id",
                suffix="_secondary",
            )
            .collect()
        )

        table = MOMRollRateTable(
            file_i,
            file_i_1,
            unique_key_col="id",
            delinquency_col="delq",
            max_delq=6,
        )
        m = table.compute().drop("from_state").to_numpy()
        rowsums = m.sum(axis=1).tolist()
        colsums = m.sum(axis=0).tolist()

        expected_rowsums = []
        expected_colsums = []
        for i in range(6):
            expected_rowsums.append(
                joined.filter(pl.col("delq") == i)
                .filter(pl.col("delq_secondary").is_not_null())
                .height
            )
            expected_colsums.append(joined.filter(pl.col("delq_secondary") == i).height)
        expected_rowsums.append(
            joined.filter(pl.col("delq") >= 6).filter(pl.col("delq_secondary").is_not_null()).height
        )
        expected_colsums.append(joined.filter(pl.col("delq_secondary") >= 6).height)

        assert rowsums == expected_rowsums
        assert colsums == expected_colsums


def test_accepts_polars_dataframe_input():
    df_i = pl.read_csv(DATA / "test_data_i.csv")
    df_i_1 = pl.read_csv(DATA / "test_data_i_1.csv")
    table = MOMRollRateTable(
        df_i,
        df_i_1,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    matrix = table.compute()
    expected = pl.read_csv(DATA / "test_mom_rr_result_6.csv").to_numpy()
    assert np.array_equal(matrix.drop("from_state").to_numpy(), expected)


def test_reduce_returns_polars_with_expected_columns():
    table = MOMRollRateTable(
        DATA / "test_data_i.csv",
        DATA / "test_data_i_1.csv",
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    reduced = table.reduce()
    assert isinstance(reduced, pl.DataFrame)
    assert reduced.columns == ["from_state", "roll_down", "stable", "roll_up"]
    # Each row in percentage mode should sum to ~100 (or 0 if the row is empty).
    totals = (
        reduced.select(pl.col("roll_down") + pl.col("stable") + pl.col("roll_up"))
        .to_numpy()
        .ravel()
    )
    for t in totals:
        assert t == 0.0 or abs(t - 100.0) < 0.5
