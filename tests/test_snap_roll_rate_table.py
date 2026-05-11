from pathlib import Path

import numpy as np
import polars as pl
import pytest

from roll_rate_analysis import SnapshotRollRateTable

SIM = Path(__file__).parent / "simulation_data"


def _sim_files():
    files = sorted(SIM.glob("test_sample_*.csv"))
    return files


def test_total_count_invariant_across_detailed_and_basic():
    files = _sim_files()
    snap = files[4]
    obs = files[:4]
    perf = files[5:9]

    basic = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        keep_cols=["delq"],
    )
    detailed = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        keep_cols=["delq"],
        detailed=True,
        granularity=2,
    )
    basic_total = basic.compute().drop("from_state").to_numpy().sum()
    detailed_total = detailed.compute().drop("from_state").to_numpy().sum()
    assert basic_total == detailed_total


def test_detailed_rows_sum_back_to_basic_rows():
    """The two sub-rows for cycle 3 (or 4) must sum to the single cycle-3 (or 4) row in basic mode."""
    files = _sim_files()
    snap = files[4]
    obs = files[:4]
    perf = files[5:9]

    basic_matrix = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        keep_cols=["delq"],
    ).compute()
    detailed_matrix = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        keep_cols=["delq"],
        detailed=True,
        granularity=2,
    ).compute()

    def row(matrix: pl.DataFrame, label: str) -> np.ndarray:
        return matrix.filter(pl.col("from_state") == label).drop("from_state").to_numpy().ravel()

    basic_3 = row(basic_matrix, "3_cycle_delinquent")
    detailed_3 = row(detailed_matrix, "3x1_cycle_delinquent") + row(
        detailed_matrix, "3x2+_cycle_delinquent"
    )
    assert np.array_equal(basic_3, detailed_3)

    basic_4 = row(basic_matrix, "4_cycle_delinquent")
    detailed_4 = row(detailed_matrix, "4x1_cycle_delinquent") + row(
        detailed_matrix, "4x2+_cycle_delinquent"
    )
    assert np.array_equal(basic_4, detailed_4)


def test_handcrafted_correctness():
    """Build a tiny known dataset and verify each cell of the resulting matrix."""
    snap = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
    obs = [
        pl.DataFrame({"id": [1, 2, 3, 4, 5], "delq": [0, 1, 2, 5, 7]}),
        pl.DataFrame({"id": [1, 2, 3, 4, 5], "delq": [0, 0, 3, 4, 6]}),
    ]
    perf = [
        pl.DataFrame({"id": [1, 2, 3, 4, 5], "delq": [0, 2, 5, 2, 6]}),
        pl.DataFrame({"id": [1, 2, 3, 4, 5], "delq": [1, 1, 4, 3, 8]}),
    ]

    table = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
    )
    matrix = table.compute()

    # obs_max_delq per account: id1=0, id2=1, id3=3, id4=5, id5=max(7,6)→capped to 6+
    # perf_max_delq per account: id1=1, id2=2, id3=5, id4=3, id5=max(6,8)→capped to 6+
    # Expected (row=obs, col=perf):
    #   row 0 (obs=0): id1 → col 1: count 1
    #   row 1 (obs=1): id2 → col 2: count 1
    #   row 3 (obs=3): id3 → col 5: count 1
    #   row 5 (obs=5): id4 → col 3: count 1
    #   row 6+ (obs≥6): id5 → col 6: count 1
    arr = matrix.drop("from_state").to_numpy()
    assert arr.shape == (7, 7)
    expected = np.zeros((7, 7), dtype=np.int64)
    expected[0, 1] = 1
    expected[1, 2] = 1
    expected[3, 5] = 1
    expected[5, 3] = 1
    expected[6, 6] = 1
    assert np.array_equal(arr, expected)


def test_detailed_granularity_correctness():
    """In detailed mode, the obs_times_3_cycle counts must drive the 3x1 vs 3x2+ split."""
    snap = pl.DataFrame({"id": [1, 2, 3]})
    # id1 hits delq=3 once across the obs window; id2 hits it twice; id3 hits it three times.
    obs = [
        pl.DataFrame({"id": [1, 2, 3], "delq": [3, 3, 3]}),
        pl.DataFrame({"id": [1, 2, 3], "delq": [0, 3, 3]}),
        pl.DataFrame({"id": [1, 2, 3], "delq": [0, 0, 3]}),
    ]
    perf = [
        pl.DataFrame({"id": [1, 2, 3], "delq": [0, 0, 0]}),
    ]

    table = SnapshotRollRateTable(
        snap,
        obs,
        perf,
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        detailed=True,
        granularity=2,
    )
    matrix = table.compute()
    # 3x1 row: 1 account (id1 hit 3 exactly once), all rolled down to 0.
    # 3x2+ row: 2 accounts (id2 hit twice, id3 hit thrice), all rolled down to 0.
    arr = matrix.drop("from_state").to_numpy()
    labels = matrix["from_state"].to_list()
    row_3x1 = arr[labels.index("3x1_cycle_delinquent")]
    row_3x2_plus = arr[labels.index("3x2+_cycle_delinquent")]
    assert row_3x1.tolist() == [1, 0, 0, 0, 0, 0, 0]
    assert row_3x2_plus.tolist() == [2, 0, 0, 0, 0, 0, 0]


def test_detailed_requires_granularity_at_least_two():
    with pytest.raises(ValueError):
        SnapshotRollRateTable(
            "snap.csv",
            ["a.csv"],
            ["b.csv"],
            unique_key_col="id",
            delinquency_col="delq",
            detailed=True,
            granularity=1,
        )


def test_reduce_returns_polars_with_expected_columns():
    files = _sim_files()
    table = SnapshotRollRateTable(
        files[4],
        files[:4],
        files[5:9],
        unique_key_col="id",
        delinquency_col="delq",
        max_delq=6,
        keep_cols=["delq"],
    )
    reduced = table.reduce()
    assert reduced.columns == ["from_state", "roll_down", "stable", "roll_up"]
    assert isinstance(reduced, pl.DataFrame)
