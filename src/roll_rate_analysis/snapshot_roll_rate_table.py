from typing import Optional

import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs


class SnapshotRollRateTable:
    """Snapshot Roll Rate Table: Given a list of files as an observation window and another list fo files as
    a performance window, a roll rate table is calculated for tha accounts of the snapshot month.
    By default, max delinquency is measured in both windows.

    Parameters
    ----------
    snapshot_file: str
                   Path to the file referring to the snapshot month.

    unique_key_col: str
                    Unique key column of the files.
                    The name of the unique_key_col should be the same in all files used.

    delinquency_col: str
                     Column which indicates the delinquency of an account in months.
                     The name of the delinquency_col should be the same in all files used.

    obs_files: str
               List of file paths referring to observation window months.

    perf_files: str
                List of file paths referring to performance window months.

    keep_cols: str, optional
               List of columns to be kept. In general, one might not need every column of the file for the calculation,
               so by keeping the ones needed the process is more efficient and faster.

    max_delq: int
              Maximum value of delinquency we want in the table. Every other value for delinquency greater than max_delq
              is summarized and added into this one.

    detailed: bool
              Boolean variable indicating whether or not the table should be more detailed.
              The detailed view concerns delinquencies equal to 3 and 4.

    granularity: int
                 The level of granularity was want to see in our final more detailed table.
                 The value must be greater or equal to 2 when detailed=True.
    """

    def __init__(
        self,
        snapshot_file: str,
        unique_key_col: str,
        delinquency_col: str,
        obs_files: list[str],
        perf_files: list[str],
        keep_cols: Optional[list[str]] = None,
        max_delq: int = 6,
        detailed: bool = False,
        granularity: int = 1,
    ):
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.max_delq = max_delq
        self.detailed = detailed
        if self.detailed and granularity <= 1:
            raise ValueError(
                "Granularity should be at least equal to 2 when using detailed=True variable."
            )
        self.granularity = granularity
        self.main_file = pl.scan_csv(source=snapshot_file).select([self.unique_key_col])
        self.obs_files = dict()
        self.perf_files = dict()

        for file in obs_files:
            self.obs_files[file] = pl.scan_csv(file)

        for file in perf_files:
            self.perf_files[file] = pl.scan_csv(file)

        if keep_cols is not None:
            self._keep(keep_cols)

        self.obs_data = None
        self.perf_data = None

        self.column_tags = self._generate_column_tags()
        self.row_tags = self._generate_row_tags()
        # extra rows due to granularity
        self.extra_rows = 2 * (self.granularity - 1)
        self.roll_rate_matrix = np.zeros(
            [self.max_delq + 1 + self.extra_rows, self.max_delq + 1], dtype=np.int32
        )

    def compute(self):
        """
        Computes the snapshot roll rate table for the snapshot, observation and performance files that were given at initialization.
        """
        data = self.build().collect()
        for cycle in range(data["obs_max_delq"].min(), data["obs_max_delq"].max() + 1):
            self._n_cycle_performance(data, cycle=cycle)

        self.roll_rate_matrix = pd.DataFrame(
            self.roll_rate_matrix, index=self.row_tags, columns=self.column_tags
        )

    def _n_cycle_performance(self, data: pl.DataFrame, cycle: int):
        tmp = data.filter(pl.col("obs_max_delq") == cycle)

        if self.detailed and cycle in [3, 4]:
            tmp = (
                tmp.filter(pl.col(f"obs_times_{cycle}_cycle") >= 1)
                .group_by([f"obs_times_{cycle}_cycle", "perf_max_delq"])
                .len()
                .sort([f"obs_times_{cycle}_cycle", "perf_max_delq"])
            )
            for i in range(1, self.granularity + 1):
                idxs, values, values_plus = self._detailed_delinquencies(
                    data=tmp, cycle=cycle, rank=i
                )

                self._update_matrix(
                    cycle=cycle,
                    idxs=idxs,
                    values=values,
                    plus_values=values_plus,
                    rank=i,
                )

        else:
            tmp = (
                tmp.group_by(["obs_max_delq", "perf_max_delq"])
                .len()
                .sort("perf_max_delq")
            )

            idxs = tmp.filter((pl.col("perf_max_delq") <= (self.max_delq - 1)))[
                "perf_max_delq"
            ].to_numpy()
            values = tmp.filter((pl.col("perf_max_delq") <= (self.max_delq - 1)))[
                "len"
            ].to_numpy()
            values_plus = tmp.filter((pl.col("perf_max_delq") > (self.max_delq - 1)))[
                "len"
            ].sum()

            self._update_matrix(
                cycle=cycle, idxs=idxs, values=values, plus_values=values_plus
            )

    def build(self):
        """
        Merges the relevant data into one dataset in order to proceed with the computation afterwards.
        """
        self._build_obs_part()
        self._build_perf_part()

        result = self.obs_data.join(
            self.perf_data, how="left", on=self.unique_key_col, suffix="_perf"
        )
        if self.detailed:
            result = result.select(
                [
                    self.unique_key_col,
                    "obs_max_delq",
                    "obs_times_3_cycle",
                    "obs_times_4_cycle",
                    "perf_max_delq",
                ]
            )
        else:
            result = result.select(
                [self.unique_key_col, "obs_max_delq", "perf_max_delq"]
            )

        return result

    def _build_obs_part(self):
        self.obs_data = self.main_file
        for item in self.obs_files.keys():
            self.obs_data = self.obs_data.join(
                self.obs_files[item],
                how="left",
                on=self.unique_key_col,
                suffix="_" + item,
            )

        self.obs_data = self.obs_data.with_columns(
            pl.max_horizontal(cs.starts_with(self.delinquency_col)).alias(
                "obs_max_delq"
            )
        )

        if self.detailed:
            self.obs_data = self.obs_data.with_columns(
                [
                    pl.sum_horizontal(cs.starts_with(self.delinquency_col) == 3).alias(
                        "obs_times_3_cycle"
                    ),
                    pl.sum_horizontal(cs.starts_with(self.delinquency_col) == 4).alias(
                        "obs_times_4_cycle"
                    ),
                ]
            )

    def _build_perf_part(self):
        self.perf_data = self.main_file
        for item in self.perf_files.keys():
            self.perf_data = self.perf_data.join(
                self.perf_files[item],
                how="left",
                on=self.unique_key_col,
                suffix="_" + item,
            )

        self.perf_data = self.perf_data.with_columns(
            pl.max_horizontal(cs.starts_with(self.delinquency_col)).alias(
                "perf_max_delq"
            )
        )

    def _keep(self, cols_to_keep: list[str]):
        for item in self.obs_files.keys():
            self.obs_files[item] = self.obs_files[item].select(
                [self.unique_key_col] + cols_to_keep
            )

        for item in self.perf_files.keys():
            self.perf_files[item] = self.perf_files[item].select(
                [self.unique_key_col] + cols_to_keep
            )

    def _detailed_delinquencies(self, data: pl.DataFrame, cycle: int, rank: int):
        if rank != self.granularity:
            idxs = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") < self.max_delq)
            )["perf_max_delq"].to_numpy()
            values = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") < self.max_delq)
            )["len"].to_numpy()
            values_plus = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") >= self.max_delq)
            )["len"].sum()
        else:
            data = (
                data.filter(pl.col(f"obs_times_{cycle}_cycle") >= rank)
                .select(["perf_max_delq", "len"])
                .group_by(["perf_max_delq"])
                .sum()
            )

            idxs = data.filter((pl.col("perf_max_delq") < self.max_delq))[
                "perf_max_delq"
            ].to_numpy()
            values = data.filter((pl.col("perf_max_delq") < self.max_delq))[
                "len"
            ].to_numpy()
            values_plus = data.filter((pl.col("perf_max_delq") >= self.max_delq))[
                "len"
            ].sum()

        return idxs, values, values_plus

    def _update_matrix(self, cycle, idxs, values, plus_values, rank: int = 1):
        """
        Updates the roll rate matrix given the indexes and their values.

        Parameters
        ----------
        cycle: int,
               Delinquency cycle (e.g. 0, 1, 2, 3, ...).

        idxs: array-like
              Indexes of the values in the roll_rate_matrix that are going to be modified.

        values: array-like
                Values that are going to be inserted in the roll_rate_matrix.

        plus_values: int
                     Value that is going to be added to the last index of a row, which indicates the largest delinquency
                     that we are taking into account.

        rank: int
              Indicator of the degree of granularity to compute at each update.
        """

        if cycle in [0, 1, 2]:
            self.roll_rate_matrix[cycle, idxs] += values
            self.roll_rate_matrix[cycle, self.max_delq] += plus_values
        elif cycle == 3:
            self.roll_rate_matrix[cycle + rank - 1, idxs] += values
            self.roll_rate_matrix[cycle + rank - 1, self.max_delq] += plus_values
        elif cycle == 4:
            self.roll_rate_matrix[
                cycle - 1 + self.granularity + rank - 1, idxs
            ] += values
            self.roll_rate_matrix[
                cycle - 1 + self.granularity + rank - 1, self.max_delq
            ] += plus_values
        elif cycle == 5:
            self.roll_rate_matrix[cycle + self.extra_rows, idxs] += values
            self.roll_rate_matrix[cycle + self.extra_rows, self.max_delq] += plus_values
        elif cycle >= self.max_delq:
            self.roll_rate_matrix[self.max_delq + self.extra_rows, idxs] += values
            self.roll_rate_matrix[
                self.max_delq + self.extra_rows, self.max_delq
            ] += plus_values

    def get_roll_rates(self):
        """
        Returns the roll rate table.
        """
        return self.roll_rate_matrix

    def _generate_row_tags(self):
        tags = []
        for i in range(self.max_delq):
            if i in [3, 4] and self.granularity > 1:
                for j in range(1, self.granularity):
                    tags.append(f"{i}x{j}_cycle_delinquent")

                tags.append(f"{i}x{self.granularity}+_cycle_delinquent")
            else:
                tags.append(f"{i}_cycle_delinquent")

        tags.append(f"{self.max_delq}+_cycle_delinquent")

        return tags

    def _generate_column_tags(self):
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

        roll_rate_matrix = self.roll_rate_matrix.values

        roll_down = []
        roll_up = []
        stable = []

        # 0, 1, 2 delinquencies
        up_1 = np.triu(roll_rate_matrix[:3, :]) - np.hstack(
            (np.diag(roll_rate_matrix[:3, :].diagonal()), np.zeros((3, 4), dtype=int))
        )
        lo_1 = np.tril(roll_rate_matrix[:3, :]) - np.hstack(
            (np.diag(roll_rate_matrix[:3, :].diagonal()), np.zeros((3, 4), dtype=int))
        )

        up_1 = np.sum(up_1, axis=1)
        lo_1 = np.sum(lo_1, axis=1)
        st_1 = roll_rate_matrix[:3, :].diagonal()

        # 5 and upwards delinquencies
        tmp = np.flip(np.flip(roll_rate_matrix[-2:, :], 1), 0)
        up_2 = np.flip(
            np.tril(tmp)
            - np.hstack((np.diag(tmp.diagonal()), np.zeros((2, 5), dtype=int))),
            0,
        )
        lo_2 = np.flip(
            np.triu(tmp)
            - np.hstack((np.diag(tmp.diagonal()), np.zeros((2, 5), dtype=int))),
            0,
        )

        up_2 = np.sum(up_2, axis=1)
        lo_2 = np.sum(lo_2, axis=1)
        st_2 = np.flip(tmp.diagonal(), 0)

        # 3, 4 delinquencies
        for i, j in zip(range(self.granularity), (3, 4)):
            tmp = roll_rate_matrix[
                (3 + (i * self.granularity)) : (-2 - self.granularity * (1 - i)), :
            ]

            up_tmp = np.sum(tmp[:, j + 1 :], 1)
            lo_tmp = np.sum(tmp[:, :j], 1)
            st_tmp = tmp[:, j]

            roll_down += lo_tmp.tolist()
            roll_up += up_tmp.tolist()
            stable += st_tmp.tolist()

        roll_up = up_1.tolist() + roll_up + up_2.tolist()
        roll_down = lo_1.tolist() + roll_down + lo_2.tolist()
        stable = st_1.tolist() + stable + st_2.tolist()

        reduced_matrix = np.matrix([roll_down, stable, roll_up]).getT()

        if percentages:
            reduced_matrix = 100 * reduced_matrix / np.sum(reduced_matrix, axis=1)
            reduced_matrix = np.round(reduced_matrix, 1)

        reduced_matrix = pd.DataFrame(
            reduced_matrix,
            columns=["roll_down", "stable", "roll_up"],
            index=self.row_tags,
        )

        return reduced_matrix
