import numpy as np
import polars as pl
import polars.selectors as cs


class SnapshotRollRateTable:
    def __init__(
        self,
        snapshot_file: str,
        unique_key_col: str,
        delinquency_col: str,
        obs_files: list[str],
        perf_files: list[str],
        keep_cols: list[str] = None,
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
            self.keep(keep_cols)

        self.obs_data = None
        self.perf_data = None

        # extra rows due to granularity
        self.extra_rows = 2 * (self.granularity - 1)
        self.roll_rate_matrix = np.zeros(
            [self.max_delq + 1 + self.extra_rows, self.max_delq + 1], dtype=np.int32
        )

    def compute(self):
        data = self.build().collect()
        for cycle in range(data["obs_max_delq"].min(), data["obs_max_delq"].max() + 1):
            self._n_cycle_performance(data, cycle=cycle)

        return pl.from_numpy(self.roll_rate_matrix, orient="row")

    def _n_cycle_performance(self, data: pl.DataFrame, cycle: int):
        tmp = data.filter(pl.col("obs_max_delq") == cycle)

        if self.detailed and cycle in [3, 4]:
            tmp = (
                tmp.filter(pl.col(f"obs_times_{cycle}_cycle") >= 1)
                .groupby([f"obs_times_{cycle}_cycle", "perf_max_delq"])
                .count()
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
                tmp.groupby(["obs_max_delq", "perf_max_delq"])
                .count()
                .sort("perf_max_delq")
            )

            idxs = tmp.filter((pl.col("perf_max_delq") <= (self.max_delq - 1)))[
                "perf_max_delq"
            ].to_numpy()
            values = tmp.filter((pl.col("perf_max_delq") <= (self.max_delq - 1)))[
                "count"
            ].to_numpy()
            values_plus = tmp.filter((pl.col("perf_max_delq") > (self.max_delq - 1)))[
                "count"
            ].sum()

            self._update_matrix(
                cycle=cycle, idxs=idxs, values=values, plus_values=values_plus
            )

    def build(self):
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
            self.obs_data = self.null_to_zero(self.obs_data)
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

    def keep(self, cols_to_keep: list[str]):
        for item in self.obs_files.keys():
            self.obs_files[item] = self.obs_files[item].select(cols_to_keep)

        for item in self.perf_files.keys():
            self.perf_files[item] = self.perf_files[item].select(cols_to_keep)

    def _detailed_delinquencies(self, data: pl.DataFrame, cycle: int, rank: int):
        if rank != self.granularity:
            idxs = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") < self.max_delq)
            )["perf_max_delq"].to_numpy()
            values = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") < self.max_delq)
            )["count"].to_numpy()
            values_plus = data.filter(
                (pl.col(f"obs_times_{cycle}_cycle") == rank)
                & (pl.col("perf_max_delq") >= self.max_delq)
            )["count"].sum()
        else:
            data = (
                data.filter(pl.col(f"obs_times_{cycle}_cycle") >= rank)
                .select(["perf_max_delq", "count"])
                .group_by(["perf_max_delq"])
                .sum()
            )

            idxs = data.filter((pl.col("perf_max_delq") < self.max_delq))[
                "perf_max_delq"
            ].to_numpy()
            values = data.filter((pl.col("perf_max_delq") < self.max_delq))[
                "count"
            ].to_numpy()
            values_plus = data.filter((pl.col("perf_max_delq") >= self.max_delq))[
                "count"
            ].sum()

        return idxs, values, values_plus

    def _update_matrix(self, cycle, idxs, values, plus_values, rank: int = 1):
        """
        Updates the roll rate matrix given the indexes and their values.

        Parameters
        -------
        cycle: int,
               Delinquency cycle (e.g. 0, 1, 2, 3, ...).

        idxs: array-like,
              Indexes of the values in the roll_rate_matrix that are going to be modified.

        values: array-like,
                Values that are going to be inserted in the roll_rate_matrix.

        plus_values: int,
                     Value that is going to be added to the last index of a row, which indicates the largest delinquency
                     that we are taking into account.

        rank: int,
              Idnicator of the degree of granularity to compute at each update.
        """

        if cycle in [0, 1, 2]:
            self.roll_rate_matrix[cycle, idxs] += values
            self.roll_rate_matrix[cycle, self.max_delq] += plus_values
        if cycle == 3:
            self.roll_rate_matrix[cycle + rank - 1, idxs] += values
            self.roll_rate_matrix[cycle + rank - 1, self.max_delq] += plus_values
        if cycle == 4:
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

    def null_to_zero(self, data: pl.DataFrame):
        for column in data.columns[1:]:
            data = data.with_columns(
                pl.when(pl.col(column).is_null())
                .then(pl.lit(0))
                .otherwise(pl.col(column))
                .alias(column)
            )

        return data
