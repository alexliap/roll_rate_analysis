import polars as pl
import numpy as np
import polars.selectors as cs

class SnapshotRollRateTable:
    def __init__(self, snapshot_file: str, unique_key_col: str, delinquency_col: str, obs_files: list[str], perf_files: list[str], keep_cols: list[str] = None):
        
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.main_file = pl.scan_csv(source=snapshot_file).select([self.unique_key_col])
        self.obs_files = dict()
        self.perf_files = dict()

        for file in obs_files:
            self.obs_files[file] = pl.scan_csv(file)
            if keep_cols is not None:
                self.obs_files[file] = self.obs_files[file].select(keep_cols)

        for file in perf_files:
            self.perf_files[file] = pl.scan_csv(file)
            if keep_cols is not None:
                self.perf_files[file] = self.perf_files[file].select(keep_cols)

        self.obs_data = None
        self.perf_data = None

    def keep(self, cols_to_keep: list[str]):
        for item in self.obs_files.keys():
            self.obs_files[item] = self.obs_files[item].select(cols_to_keep)

        for item in self.perf_files.keys():
            self.perf_files[item] = self.perf_files[item].select(cols_to_keep)

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
