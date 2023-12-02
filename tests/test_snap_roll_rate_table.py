import os

from roll_rate_analysis import SnapshotRollRateTable


def test_snap_outputs():
    # test if snap roll rates for same data with different configurations have the same number of counts
    os.chdir("tests/simulation_data/")
    snap_file = "test_sample_4.csv"
    files = os.listdir("./")
    files.sort()
    obs_files = files[:4]
    perf_files = files[5:9]

    table_1 = SnapshotRollRateTable(
        snap_file,
        "id",
        "delq",
        obs_files,
        perf_files,
        keep_cols=["delq"],
        max_delq=6,
        detailed=True,
        granularity=2,
    )
    table_2 = SnapshotRollRateTable(
        snap_file, "id", "delq", obs_files, perf_files, keep_cols=["delq"], max_delq=6
    )

    table_1.compute()
    table_2.compute()

    sum_1 = table_1.get_roll_rates().values.sum()
    sum_2 = table_2.get_roll_rates().values.sum()

    assert sum_1 == sum_2
