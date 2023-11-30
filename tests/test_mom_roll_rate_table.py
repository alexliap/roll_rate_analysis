import os

import numpy as np
import pandas as pd
import polars as pl

from roll_rate_analysis import MOMRollRateTable


def test_init():
    path_i = "tests/test_data/test_data_i.csv"
    pathi_i_1 = "tests/test_data/test_data_i_1.csv"

    table = MOMRollRateTable(
        unique_key_col="id",
        delinquency_col="delq",
        path_i=path_i,
        path_i_1=pathi_i_1,
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
        path_i_1=pathi_i_1,
        max_delq=6,
    )

    table.build()

    test_result = pd.read_csv("tests/test_data/test_mom_rr_result_6.csv").values

    assert np.sum(test_result == table.get_roll_rates().values) == 7**2


def test_build_max_delq_6_complete():
    files = os.listdir("tests/simulation_data/")
    for i in range(len(files) - 1):
        file_i = os.path.join("tests/simulation_data/", files[i])
        file_i_1 = os.path.join("tests/simulation_data/", files[i + 1])

        data_i = pl.scan_csv(file_i).select(["id", "delq"])
        data_i_1 = pl.scan_csv(file_i_1).select(["id", "delq"])

        m_data = data_i.join(
            data_i_1, how="left", on="id", suffix="_secondary"
        ).collect()

        rr_matrix = MOMRollRateTable(
            "id", "delq", path_i=file_i, path_i_1=file_i_1, max_delq=6
        )

        rr_matrix.build()

        # colsums
        colsums = np.sum(rr_matrix.get_roll_rates().values, axis=0).tolist()
        # rowsums
        rowsums = np.sum(rr_matrix.get_roll_rates().values, axis=1).tolist()

        right_rowsums = []
        right_colsums = []
        for i in range(0, 6):
            right_rowsums.append(
                m_data.filter(pl.col("delq") == i)
                .filter(pl.col("delq_secondary").is_not_null())
                .shape[0]
            )
            right_colsums.append(m_data.filter(pl.col("delq_secondary") == i).shape[0])

        right_rowsums.append(
            m_data.filter(pl.col("delq") >= 6)
            .filter(pl.col("delq_secondary").is_not_null())
            .shape[0]
        )
        right_colsums.append(m_data.filter(pl.col("delq_secondary") >= 6).shape[0])

        assert colsums == right_colsums
        assert rowsums == right_rowsums
