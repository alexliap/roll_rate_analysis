import numpy as np
import pandas as pd
import polars as pl

from roll_rate_analysis.mom_roll_rate_table import MOMRollRateTable


def test_init():
    path_i = "tests/test_data/test_data_i.csv"
    pathi_i_1 = "tests/test_data/test_data_i_1.csv"

    table = MOMRollRateTable(
        unique_key_col="id",
        delinquency_col="delq",
        path_i=path_i,
        path_i_plus_one=pathi_i_1,
        max_delq=6,
    )

    assert table.unique_key_col == "id"
    assert table.delinquency_col == "delq"
    assert np.sum(table.roll_rate_matrix) == 0


def test_build_max_delq_6():
    path_i = "tests/test_data/test_data_i.csv"
    pathi_i_1 = "tests/test_data/test_data_i_1.csv"

    table = MOMRollRateTable(
        unique_key_col="id",
        delinquency_col="delq",
        path_i=path_i,
        path_i_plus_one=pathi_i_1,
        max_delq=6,
    )

    table = table.build().values

    test_result = pd.read_csv("tests/test_data/test_mom_rr_result_6.csv").values

    assert np.sum(test_result == table) == 7**2
