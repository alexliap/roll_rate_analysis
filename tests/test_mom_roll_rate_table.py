import polars as pl
import numpy as np
from roll_rate_analysis.mom_roll_rate_table import MOMRollRateTable

def test_init():
    df1 = pl.scan_csv('test_datasets/test_sample_0.csv').collect()
    df2 = pl.scan_csv('test_datasets/test_sample_1.csv').collect()

    table = MOMRollRateTable(unique_key_col='ACCOUNT_ID', delinquency_col="DEBT_AGING", df1=df1, df2=df2)

    assert table.unique_key_col=='ACCOUNT_ID'
    assert table.delinquency_col=='DEBT_AGING'
    assert np.sum(table.roll_rate_matrix) == 0

def test_build():
    df1 = pl.scan_csv('test_datasets/test_sample_0.csv').collect()
    df2 = pl.scan_csv('test_datasets/test_sample_1.csv').collect()
    df = df1.join(df2, how = 'left', on = 'ACCOUNT_ID', suffix = '_secondary')
    schema_cols = ['up_to_date', '1_cycle_deliquent', '2_cycle_deliquent', '3_cycle_deliquent',
                   '4_cycle_deliquent', '5_cycle_deliquent', '6+_cycle_deliquent']
    roll_rates = np.zeros([len(schema_cols), len(schema_cols)], dtype = np.int32)

    for j in range(df['DEBT_AGING'].min(), df['DEBT_AGING'].max() + 1):
        tmp = (df.filter(pl.col('DEBT_AGING') == j)
               .groupby(['DEBT_AGING', 'DEBT_AGING_secondary'])
               .count()
               .sort('DEBT_AGING_secondary'))
        idxs = tmp.filter((pl.col('DEBT_AGING_secondary') <= 5))['DEBT_AGING_secondary'].to_numpy()
        values = tmp.filter((pl.col('DEBT_AGING_secondary') <= 5))['count'].to_numpy()
        values_6_plus = tmp.filter((pl.col('DEBT_AGING_secondary') > 5))['count'].sum()
        
        if j >= 6:
            roll_rates[6, idxs] += values
            roll_rates[6, 6] += values_6_plus
        else:
            roll_rates[j, idxs] += values
            roll_rates[j, 6] += values_6_plus

    table = MOMRollRateTable(unique_key_col='ACCOUNT_ID', delinquency_col="DEBT_AGING", df1=df1, df2=df2)
    roll_rate_matrix = table.build().to_numpy()

    assert np.sum(roll_rates == roll_rate_matrix) == len(schema_cols)**2
