import polars as pl
import numpy as np


class MOMRollRateTable:
    def __init__(self, unique_key_col: str, delinquency_col: str, df1: pl.DataFrame, df2: pl.DataFrame, max_delq: int = 6):
        
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.data = df1.join(df2, how = 'left', on = self.unique_key_col, suffix = '_secondary')
        self.max_delq = max_delq
        self.roll_rate_matrix = np.zeros([self.max_delq + 1, self.max_delq + 1], dtype = np.int32)

    def build(self):
        for cycle in range(self.data[self.delinquency_col].min(), self.data[self.delinquency_col].max() + 1):
            self.roll_rate_matrix = self._n_cycle_performance(self.data, cycle = cycle)
        
        return pl.from_numpy(self.roll_rate_matrix, orient='row')

    def _n_cycle_performance(self, data: pl.DataFrame, cycle: int):
        tmp = (data.filter(pl.col(self.delinquency_col) == cycle)
               .groupby([self.delinquency_col, self.delinquency_col + '_secondary'])
               .count()
               .sort(self.delinquency_col + '_secondary'))

        idxs = tmp.filter((pl.col(self.delinquency_col + '_secondary') <= (self.max_delq - 1)))[self.delinquency_col + '_secondary'].to_numpy()
        values = tmp.filter((pl.col(self.delinquency_col + '_secondary') <= (self.max_delq - 1)))['count'].to_numpy()
        values_plus = tmp.filter((pl.col(self.delinquency_col + '_secondary') > (self.max_delq - 1)))['count'].sum()

        if cycle >= self.max_delq:
            self.roll_rate_matrix[self.max_delq, idxs] += values
            self.roll_rate_matrix[self.max_delq, self.max_delq] += values_plus
        else:
            self.roll_rate_matrix[cycle, idxs] += values
            self.roll_rate_matrix[cycle, self.max_delq] += values_plus
        
        return self.roll_rate_matrix

    def get_roll_rates(self):

        return self.roll_rate_matrix
