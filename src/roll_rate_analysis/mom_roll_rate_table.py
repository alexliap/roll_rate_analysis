import numpy as np
import pandas as pd
import polars as pl


class MOMRollRateTable:
    def __init__(
        self,
        unique_key_col: str,
        delinquency_col: str,
        path_i: str,
        path_i_plus_one: str,
        max_delq: int = 6,
    ):
        """
        Month Over Month Roll Rate Table of two consecutive months. Given a file that represents month i and another one that
        represents month i+1 this class computes the month over moth roll rate.

        Paramaters
        -------
        unique_key_col: str,
                        Unique key column of the two files. The name of the column in the two files must be the same.

        delinquency_col: str,
                         Column which indicates the delinquency of an account. The name of the column in the two files must be the same.

        path_i: str,
                Path of the file that represents month i.

        path_i_plus_one: str
                         Path of the file that represents month i+1.

        max_delq: int,
                  Maximum value of delinquency we want in the table. Every other value for deliqnuency greater than max_delq
                  is summarized and added into this one.
        """
        self.df_i_path = path_i
        self.df_i_plus_one_path = path_i_plus_one
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col

        self.df_i = pl.scan_csv(self.df_i_path)
        self.df_i_plus_one = pl.scan_csv(self.df_i_plus_one_path)
        self.data = self.df_i.join(
            self.df_i_plus_one, how="left", on=self.unique_key_col, suffix="_secondary"
        ).collect()
        self.max_delq = max_delq
        self.tags = self._generate_tags()
        self.roll_rate_matrix = np.zeros(
            [self.max_delq + 1, self.max_delq + 1], dtype=np.int32
        )

    def build(self):
        """
        Computes the month over month roll rate matrix for the two files that were given at initialization.
        """
        for cycle in range(
            self.data[self.delinquency_col].min(),
            self.data[self.delinquency_col].max() + 1,
        ):
            self._n_cycle_performance(self.data, cycle=cycle)

        return pd.DataFrame(self.roll_rate_matrix, index=self.tags, columns=self.tags)

    def _n_cycle_performance(self, data: pl.DataFrame, cycle: int):
        """
        Given the cycle of delinquency the performance of those accounts is calculated
        and the roll rate matrix is updated.

        Paramaters
        -------
        data: pl.DataFrame,
              The combined data of the two files given at initialization.

        cycle: int,
               Delinquency cycle (e.g. 0, 1, 2, 3, ...).
        """
        tmp = (
            data.filter(pl.col(self.delinquency_col) == cycle)
            .group_by([self.delinquency_col, self.delinquency_col + "_secondary"])
            .count()
            .sort(self.delinquency_col + "_secondary")
        )

        idxs = tmp.filter(
            (pl.col(self.delinquency_col + "_secondary") <= (self.max_delq - 1))
        )[self.delinquency_col + "_secondary"].to_numpy()
        values = tmp.filter(
            (pl.col(self.delinquency_col + "_secondary") <= (self.max_delq - 1))
        )["count"].to_numpy()
        values_plus = tmp.filter(
            (pl.col(self.delinquency_col + "_secondary") > (self.max_delq - 1))
        )["count"].sum()

        self._update_matrix(
            cycle=cycle, idxs=idxs, values=values, plus_values=values_plus
        )

    def _update_matrix(self, cycle, idxs, values, plus_values):
        """
        Updates the roll rate matrix given the indexes and their values.

        Parameters
        -------
        cycle: int,
               Delinquency cycle (e.g. 0, 1, 2, 3, ...).

        idxs: array-like,
              Indexes of the values in the roll_rate_matrix that are going to be modified.

        values: array-like,
                Performances for a certain delinquency cycle that are going to be inserted in the roll_rate_matrix.

        plus_values: int,
                     Performancesfor a certain delinquency cycle that are going to be added to the last index of a row,
                     which indicates the largest delinquency that we are taking into account.
        """
        if cycle >= self.max_delq:
            self.roll_rate_matrix[self.max_delq, idxs] += values
            self.roll_rate_matrix[self.max_delq, self.max_delq] += plus_values
        else:
            self.roll_rate_matrix[cycle, idxs] += values
            self.roll_rate_matrix[cycle, self.max_delq] += plus_values

    def get_roll_rates(self):
        """
        Get the roll rate matrix.

        Returns
        -------
        np.ndarray: Roll Rate Matrix
        """
        return self.roll_rate_matrix

    def _generate_tags(self):
        """
        Generate column and row tags for the final month over month roll rate table.

        Returns
        -------
        list: Roll Rate table column and row tags.
        """
        tags = []
        for i in range(self.max_delq):
            tags.append(f"{i}_cycle_deliqnuent")

        tags.append(f"{self.max_delq}+_cycle_deliqnuent")

        return tags
