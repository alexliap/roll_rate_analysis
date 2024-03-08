import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs


class MOMRollRateTable:
    """Month Over Month Roll Rate Table of two consecutive months. Given a file that represents month i and another one that
    represents month i+1 this class computes the month over moth roll rate.

    Parameters
    -------
    unique_key_col: str,
                    Unique key column of the two files. The name of the column in the two files must be the same.

    delinquency_col: str,
                     Column which indicates the delinquency of an account. The name of the column in the two files must be the same.

    path_i: str,
            Path of the file that represents month i.

    path_i_1: str
              Path of the file that represents month i+1.

    max_delq: int,
              Maximum value of delinquency we want in the table. Every other value for delinquency greater than max_delq
              is summarized and added into this one.

    binary_cols: list[str],
                 List of binary columns that you want to inlude in the roll rate table. They have priority over delinquency.
                 Also, if the binary_cols are more than one, they must be added in the list in descinding priority order.
    """

    def __init__(
        self,
        unique_key_col: str,
        delinquency_col: str,
        path_i: str,
        path_i_1: str,
        max_delq: int = 6,
        binary_cols: list[str] = [],
    ):
        self.df_i_path = path_i
        self.df_i_1_path = path_i_1
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.binary_cols = binary_cols
        self.priority_list = list(range(len(binary_cols), 0, -1))

        self.df_i = pl.scan_csv(self.df_i_path).select(
            [self.unique_key_col, self.delinquency_col] + self.binary_cols
        )
        self.df_i_1 = pl.scan_csv(self.df_i_1_path).select(
            [self.unique_key_col, self.delinquency_col] + self.binary_cols
        )

        self.bin_col_dict = {}
        if len(self.binary_cols) != 0:
            self.bin_col_dict = self._priority_dict()
            self.df_i = self._merge_binary_cols(self.df_i)
            self.df_i_1 = self._merge_binary_cols(self.df_i_1)

        self.data = self.df_i.join(
            self.df_i_1, how="left", on=self.unique_key_col, suffix="_secondary"
        ).collect()

        self.max_delq = max_delq
        self.tags = self._generate_tags() + self.binary_cols[::-1]
        self.roll_rate_matrix = np.zeros(
            [
                self.max_delq + 1 + len(self.binary_cols),
                self.max_delq + 1 + len(self.binary_cols),
            ],
            dtype=np.int32,
        )

    def build(self):
        """
        Computes the month over month roll rate matrix for the two files that were given at initialization.
        """
        if len(self.binary_cols) == 0:
            for cycle in range(
                self.data[self.delinquency_col].min(),
                self.data[self.delinquency_col].max() + 1,
            ):
                self._n_cycle_performance(self.data, case=1, cycle=cycle)
        else:
            for cycle in range(
                self.data[self.delinquency_col].min(),
                self.data[self.delinquency_col].max() + 1,
            ):
                self._n_cycle_performance(self.data, case=1, cycle=cycle)
                self._n_cycle_performance(self.data, case=2, cycle=cycle)

            for priority in self.priority_list:
                self._bin_col_performance(self.data, case=3, priority=priority)
                self._bin_col_performance(self.data, case=4, priority=priority)

        self.roll_rate_matrix = pd.DataFrame(
            self.roll_rate_matrix, index=self.tags, columns=self.tags
        )

    def _get_temp_data(
        self, data: pl.DataFrame, case: int, cycle: int = None, priority: int = None
    ):
        """
        Get a temporary grouped part of the data, for every step of the roll rate calculation procedure.

        Parameters
        -------
        data: pl.DataFrame,
              The combined data of the two files given at initialization.

        case: int,
              Represents the case for which we want the relevant data.

        cycle: int,
               Needed only when case equals with 1/2. It represents the delinquency of month i that we want data for.

        priority: int,
                  Needed only when case equals with 3 or 4. It represents the binary indicator column by its given priority.
        """
        if cycle is not None and priority is not None:
            raise ValueError(
                "Only one should be defined at a time, not both cycle and priority."
            )
        elif cycle is None:
            quantity = priority
        elif priority is None:
            quantity = cycle

        if case == 1:
            if len(self.binary_cols) != 0:
                data = data.filter(
                    (pl.col("merged_bin_cols") == 0)
                    & (pl.col("merged_bin_cols_secondary") == 0)
                )

            primary_col = self.delinquency_col
            secondary_col = self.delinquency_col + "_secondary"
        elif case == 2:
            data = data.filter(
                (pl.col("merged_bin_cols") == 0)
                & (pl.col("merged_bin_cols_secondary") > 0)
            )

            primary_col = self.delinquency_col
            secondary_col = "merged_bin_cols_secondary"
        elif case == 3:
            data = data.filter(
                (pl.col("merged_bin_cols") > 0)
                & (pl.col("merged_bin_cols_secondary") == 0)
            )

            primary_col = "merged_bin_cols"
            secondary_col = self.delinquency_col + "_secondary"
        elif case == 4:
            data = data.filter(
                (pl.col("merged_bin_cols") > 0)
                & (pl.col("merged_bin_cols_secondary") > 0)
            )

            primary_col = "merged_bin_cols"
            secondary_col = "merged_bin_cols_secondary"

        tmp = (
            data.filter(pl.col(primary_col) == quantity)
            .group_by([primary_col, secondary_col])
            .count()
            .sort(secondary_col)
        )

        return tmp, secondary_col

    def _get_values(self, tmp: pl.DataFrame, col_of_interest: str):
        """
        Get the roll rate values from the temporary grouped part of the data according to the column of interest.

        Parameters
        -------
        tmp: pl.DataFrame,
             The temporary grouped part of the data.

        col_of_interest: str,
                         Either the delinquency col of month i+1 or the column with the prioritized binary indicators of month i+1.
        """
        if col_of_interest == self.delinquency_col + "_secondary":
            sub_tmp_1 = tmp.filter(
                (pl.col(self.delinquency_col + "_secondary") <= (self.max_delq - 1))
            )
            sub_tmp_2 = tmp.filter(
                (pl.col(self.delinquency_col + "_secondary") > (self.max_delq - 1))
            )

            idxs = sub_tmp_1[col_of_interest].to_numpy()
            values = sub_tmp_1["count"].to_numpy()
            values_plus = sub_tmp_2["count"].sum()

        elif col_of_interest == "merged_bin_cols_secondary":
            idxs = tmp["merged_bin_cols_secondary"].to_numpy() + self.max_delq
            values = tmp["count"].to_numpy()
            values_plus = 0

        return idxs, values, values_plus

    def _n_cycle_performance(self, data: pl.DataFrame, case: int, cycle: int):
        """
        Given the cycle of delinquency, the performance of those accounts is calculated
        and the roll rate matrix is updated.

        Parameters
        -------
        data: pl.DataFrame,
              The combined data of the two files given at initialization.

        case: int,
              Represents the case for which we want the relevant data.

        cycle: int,
               Delinquency cycle (e.g. 0, 1, 2, 3, ...).
        """
        if case not in [1, 2]:
            raise ValueError(
                "Variable case in function _n_cycle_performance() can only be equal to 1 or 2."
            )

        tmp, col_of_interest = self._get_temp_data(data, case=case, cycle=cycle)

        idxs, values, values_plus = self._get_values(tmp, col_of_interest)

        self._update_matrix(
            cycle=cycle, idxs=idxs, values=values, plus_values=values_plus
        )

    def _bin_col_performance(self, data: pl.DataFrame, case: int, priority: int):
        """
        Given the the binary indicator by its priority, the performance of those accounts is calculated
        and the roll rate matrix is updated.

        Parameters
        -------
        data: pl.DataFrame,
              The combined data of the two files given at initialization.

        case: int,
              Represents the case for which we want the relevant data.

        priority: int,
                  Needed only when case equals with 3 or 4. It represents the binary indicator column by its given priority.
        """
        tmp, col_of_interest = self._get_temp_data(data, case=case, priority=priority)

        idxs, values, values_plus = self._get_values(tmp, col_of_interest)

        self._update_matrix_bin(
            priority=priority, idxs=idxs, values=values, plus_values=values_plus
        )

    def _update_matrix_bin(self, priority: int, idxs, values, plus_values: int = 0):
        """
        Updates the roll rate matrix for the binary rows/columns.
        """

        self.roll_rate_matrix[self.max_delq + priority, idxs] += values
        self.roll_rate_matrix[self.max_delq + priority, self.max_delq] += plus_values

    def _update_matrix(self, cycle: int, idxs, values, plus_values: int):
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
                     Performances for a certain delinquency cycle that are going to be added to the last index of a row,
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
        pl.DataFrame: Roll Rate Table
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
            tags.append(f"{i}_cycle_delinquent")

        tags.append(f"{self.max_delq}+_cycle_delinquent")

        return tags

    def reduce(self, percentages=True):
        """
        Return an aggregated view of the roll rate.

        Parameters
        ----------
        percentages: bool
                     If True use percentages, else use total numbers.

        Returns
        -------
        pd.DataFrame: Aggregated roll rate table with or without percentages.
        """
        roll_up = np.sum(
            (
                np.triu(self.roll_rate_matrix.values)
                - np.diag(self.roll_rate_matrix.values.diagonal())
            ),
            axis=1,
        )
        roll_down = np.sum(
            (
                np.tril(self.roll_rate_matrix.values)
                - np.diag(self.roll_rate_matrix.values.diagonal())
            ),
            axis=1,
        )
        stable = self.roll_rate_matrix.values.diagonal()

        reduced_matrix = np.matrix([roll_down, stable, roll_up]).getT()

        if percentages:
            reduced_matrix = 100 * reduced_matrix / np.sum(reduced_matrix, axis=1)
            reduced_matrix = np.round(reduced_matrix, 1)

        reduced_table = pd.DataFrame(
            reduced_matrix, columns=["roll_down", "stable", "roll_up"], index=self.tags
        )

        return reduced_table

    def _priority_dict(self):
        bin_col_dict = {}
        for i in range(len(self.binary_cols)):
            bin_col_dict[self.binary_cols[i]] = self.priority_list[i]

        return bin_col_dict

    def _merge_binary_cols(self, data: pl.DataFrame):
        for col in self.binary_cols:
            data = data.with_columns(
                pl.when(pl.col(col) == 1)
                .then(pl.lit(self.bin_col_dict[col]))
                .otherwise(pl.col(col))
                .alias(col + "_new")
            )

        data = data.with_columns(
            pl.max_horizontal(cs.ends_with("_new")).alias("merged_bin_cols")
        )

        return data
