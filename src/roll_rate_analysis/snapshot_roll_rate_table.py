import polars as pl
import numpy as np
import polars.selectors as cs

class SnapshotRollRateTable:
    def __init__(self, snapshot_file: str, unique_key_col: str, delinquency_col: str, obs_files: list[str], perf_files: list[str], keep_cols: list[str] = None, max_delq: int = 6):
        
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.max_delq = max_delq
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

        self.roll_rate_matrix = np.zeros([self.max_delq + 1, self.max_delq + 1], dtype = np.int32)

    def compute(self):
        data = self.build().collect()
        for cycle in range(data['obs_max_delq'].min(), data['obs_max_delq'].max() + 1):
            self.roll_rate_matrix = self._n_cycle_performance(data, cycle = cycle)
        
        return pl.from_numpy(self.roll_rate_matrix, orient='row')

    def _n_cycle_performance(self, data: pl.DataFrame, cycle: int):
        tmp = (data.filter(pl.col('obs_max_delq') == cycle)
               .groupby(['obs_max_delq', 'perf_max_delq'])
               .count()
               .sort('perf_max_delq'))

        idxs = tmp.filter((pl.col('perf_max_delq') <= (self.max_delq - 1)))['perf_max_delq'].to_numpy()
        values = tmp.filter((pl.col('perf_max_delq') <= (self.max_delq - 1)))['count'].to_numpy()
        values_plus = tmp.filter((pl.col('perf_max_delq') > (self.max_delq - 1)))['count'].sum()

        if cycle >= self.max_delq:
            self.roll_rate_matrix[self.max_delq, idxs] += values
            self.roll_rate_matrix[self.max_delq, self.max_delq] += values_plus
        else:
            self.roll_rate_matrix[cycle, idxs] += values
            self.roll_rate_matrix[cycle, self.max_delq] += values_plus
        
        return self.roll_rate_matrix

    def build(self):
        self._build_obs_part()
        self._build_perf_part()

        result = self.obs_data.join(self.perf_data, how = 'left', on = self.unique_key_col, suffix = '_perf')
        result = result.select([self.unique_key_col, 'obs_max_delq', 'perf_max_delq'])

        return result

    def _build_obs_part(self):
        self.obs_data = self.main_file
        for item in self.obs_files.keys():
            self.obs_data = self.obs_data.join(self.obs_files[item], how = 'left', on = self.unique_key_col, suffix = '_' + item)

        self.obs_data = self.obs_data.with_columns(pl.max_horizontal(cs.starts_with(self.delinquency_col)).alias('obs_max_delq'))

    def _build_perf_part(self):
        self.perf_data = self.main_file
        for item in self.perf_files.keys():
            self.perf_data = self.perf_data.join(self.perf_files[item], how = 'left', on = self.unique_key_col, suffix = '_' + item)

        self.perf_data = self.perf_data.with_columns(pl.max_horizontal(cs.starts_with(self.delinquency_col)).alias('perf_max_delq'))

    def keep(self, cols_to_keep: list[str]):
        for item in self.obs_files.keys():
            self.obs_files[item] = self.obs_files[item].select(cols_to_keep)

        for item in self.perf_files.keys():
            self.perf_files[item] = self.perf_files[item].select(cols_to_keep)