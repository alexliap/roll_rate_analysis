### In this first example, we will explain the functionality of the MOMRollRateTable class

### MOMRollRateTable

In Application and Behavioural Scorecards, most of the time we don't have an exact definition on the bad customers.
So what we need to do is to decide who they are. Most of the time we decide that based on their delinquency status (e.g. 1-month delinquent, 2-month deliqnuent, etc) from month i to month i+1 (suppose September to October).

Firstly, let's see our data. In the tests folder there is a directory simulation_data. We pick 2 files representing month i and month i+1


```python
import polars as pl

data_i = pl.scan_csv("../tests/simulation_data/test_sample_0.csv").collect()
data_i_1 = pl.scan_csv("../tests/simulation_data/test_sample_1.csv").collect()
```


```python
data_i.head()
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 7)</small><table border="1" class="dataframe"><thead><tr><th>id</th><th>delq</th><th>bin_ind_1</th><th>Open</th><th>Active</th><th>Deactive</th><th>Closed</th></tr><tr><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>&quot;6C9EDD7A4D1CD3…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;D24F80FC2E5BE8…</td><td>2</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;255DEAC70523DC…</td><td>1</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;5413E691F93B09…</td><td>1</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;2E0DCCC622F818…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr></tbody></table></div>




```python
data_i_1.head()
```




<div><style>
.dataframe > thead > tr > th,
.dataframe > tbody > tr > td {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 7)</small><table border="1" class="dataframe"><thead><tr><th>id</th><th>delq</th><th>bin_ind_1</th><th>Open</th><th>Active</th><th>Deactive</th><th>Closed</th></tr><tr><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>&quot;98F343324796D9…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;4199ED33141ADC…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;BEE8FCE9BB7338…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;072661DCE708A2…</td><td>0</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr><tr><td>&quot;069EEA483CD204…</td><td>1</td><td>0</td><td>1</td><td>1</td><td>0</td><td>0</td></tr></tbody></table></div>



We can see that there are 7 columns:

**id**: Account id, the unique key of the dataset. \
**delq**: The delinquency of each account. \
**bin_ind_1**: A binary indicator. \
**Open, Active, Deactive, Closed**: Indicates if the account is open, active, deactive or closed in that month. \

The last 4 columsn are not of any use to us for this example.

So in order to get the roll rates from test_sample_0 to test_sample_1 we instanciate a MOMRllRateTable object and passing in the arguments needed, the unique key column of the 2 files **(should have the same name)**, the column which indicates the delinquency **(should have the same name)**, the paths to the 2 files (path_i for month i and path_i_1 for month i+1), the max deliqnuency we want to track and finally if we want to inlcude any binary indicators. \ \

Then we call mehtod build() on the object to calculates the roll rate table.


```python
from roll_rate_analysis import MOMRollRateTable

table = MOMRollRateTable(
    unique_key_col="id",
    delinquency_col="delq",
    path_i="../tests/simulation_data/test_sample_0.csv",
    path_i_1="../tests/simulation_data/test_sample_1.csv",
    max_delq=6,
)
```


```python
table.build()
```


```python
table.get_roll_rates()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0_cycle_deliqnuent</th>
      <th>1_cycle_deliqnuent</th>
      <th>2_cycle_deliqnuent</th>
      <th>3_cycle_deliqnuent</th>
      <th>4_cycle_deliqnuent</th>
      <th>5_cycle_deliqnuent</th>
      <th>6+_cycle_deliqnuent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0_cycle_deliqnuent</th>
      <td>29280</td>
      <td>6146</td>
      <td>5</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1_cycle_deliqnuent</th>
      <td>7759</td>
      <td>9593</td>
      <td>2347</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2_cycle_deliqnuent</th>
      <td>1103</td>
      <td>2313</td>
      <td>338</td>
      <td>787</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3_cycle_deliqnuent</th>
      <td>285</td>
      <td>341</td>
      <td>67</td>
      <td>34</td>
      <td>480</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4_cycle_deliqnuent</th>
      <td>56</td>
      <td>43</td>
      <td>4</td>
      <td>5</td>
      <td>30</td>
      <td>280</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5_cycle_deliqnuent</th>
      <td>26</td>
      <td>8</td>
      <td>0</td>
      <td>1</td>
      <td>3</td>
      <td>16</td>
      <td>182</td>
    </tr>
    <tr>
      <th>6+_cycle_deliqnuent</th>
      <td>25</td>
      <td>7</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>3</td>
      <td>342</td>
    </tr>
  </tbody>
</table>
</div>



So that does that table tell us? The sum of accounts in row **n** indicates the number of accounts that were **n** cycle delinquent in month i. The sum of accounts in column **k** indicates the number of accounts that were **k** cycle delinquent in month i+1. So the value at **(2,3)** shows that **2347** accounts that were 1 cycle delinquent on month i, are 2 cycle delinquent on month i+1, which means they didn't pay the installment in time.

Then we can call reduce to get a more high level view of that table like below.


```python
table.reduce()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>roll_down</th>
      <th>stable</th>
      <th>roll_up</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0_cycle_deliqnuent</th>
      <td>0.0</td>
      <td>82.6</td>
      <td>17.4</td>
    </tr>
    <tr>
      <th>1_cycle_deliqnuent</th>
      <td>39.4</td>
      <td>48.7</td>
      <td>11.9</td>
    </tr>
    <tr>
      <th>2_cycle_deliqnuent</th>
      <td>75.2</td>
      <td>7.4</td>
      <td>17.3</td>
    </tr>
    <tr>
      <th>3_cycle_deliqnuent</th>
      <td>57.4</td>
      <td>2.8</td>
      <td>39.8</td>
    </tr>
    <tr>
      <th>4_cycle_deliqnuent</th>
      <td>25.8</td>
      <td>7.2</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>5_cycle_deliqnuent</th>
      <td>16.1</td>
      <td>6.8</td>
      <td>77.1</td>
    </tr>
    <tr>
      <th>6+_cycle_deliqnuent</th>
      <td>9.8</td>
      <td>90.2</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



The table above gives us higher level info about the perventages of accounts in each bucket. \
**roll_down**: they paid their installment and at least one previous installment they owed \
**stable**: they just paid current installment (or remained in the 6+ deliqnuency bucket) \
**roll_up**: they didn't pay their current installment and their status chenged to worse

In most cases, the bucket where we see that the roll_up percentage is higher than 50% is the point were we decide **accounts from that bucket and below classify as bad accounts** (i.e. 4, 5, 6+ in our case). \ \

In general, we compute the roll rates for a larger period of time (e.g. a year) and sum them up and go on from that. But for the purpose of this example its not necessary.

# Miscellaneous

If we wanted to track delinquencies bigger than 6 then we could change the max_delq argument at initialization:


```python
table = MOMRollRateTable(
    unique_key_col="id",
    delinquency_col="delq",
    path_i="../tests/simulation_data/test_sample_0.csv",
    path_i_1="../tests/simulation_data/test_sample_1.csv",
    max_delq=7,
)
table.build()
table.get_roll_rates()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0_cycle_deliqnuent</th>
      <th>1_cycle_deliqnuent</th>
      <th>2_cycle_deliqnuent</th>
      <th>3_cycle_deliqnuent</th>
      <th>4_cycle_deliqnuent</th>
      <th>5_cycle_deliqnuent</th>
      <th>6_cycle_deliqnuent</th>
      <th>7+_cycle_deliqnuent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0_cycle_deliqnuent</th>
      <td>29280</td>
      <td>6146</td>
      <td>5</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1_cycle_deliqnuent</th>
      <td>7759</td>
      <td>9593</td>
      <td>2347</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2_cycle_deliqnuent</th>
      <td>1103</td>
      <td>2313</td>
      <td>338</td>
      <td>787</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3_cycle_deliqnuent</th>
      <td>285</td>
      <td>341</td>
      <td>67</td>
      <td>34</td>
      <td>480</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4_cycle_deliqnuent</th>
      <td>56</td>
      <td>43</td>
      <td>4</td>
      <td>5</td>
      <td>30</td>
      <td>280</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5_cycle_deliqnuent</th>
      <td>26</td>
      <td>8</td>
      <td>0</td>
      <td>1</td>
      <td>3</td>
      <td>16</td>
      <td>182</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6_cycle_deliqnuent</th>
      <td>11</td>
      <td>3</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>2</td>
      <td>6</td>
      <td>57</td>
    </tr>
    <tr>
      <th>7+_cycle_deliqnuent</th>
      <td>14</td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
      <td>278</td>
    </tr>
  </tbody>
</table>
</div>




```python
table.reduce()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>roll_down</th>
      <th>stable</th>
      <th>roll_up</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0_cycle_deliqnuent</th>
      <td>0.0</td>
      <td>82.6</td>
      <td>17.4</td>
    </tr>
    <tr>
      <th>1_cycle_deliqnuent</th>
      <td>39.4</td>
      <td>48.7</td>
      <td>11.9</td>
    </tr>
    <tr>
      <th>2_cycle_deliqnuent</th>
      <td>75.2</td>
      <td>7.4</td>
      <td>17.3</td>
    </tr>
    <tr>
      <th>3_cycle_deliqnuent</th>
      <td>57.4</td>
      <td>2.8</td>
      <td>39.8</td>
    </tr>
    <tr>
      <th>4_cycle_deliqnuent</th>
      <td>25.8</td>
      <td>7.2</td>
      <td>67.0</td>
    </tr>
    <tr>
      <th>5_cycle_deliqnuent</th>
      <td>16.1</td>
      <td>6.8</td>
      <td>77.1</td>
    </tr>
    <tr>
      <th>6_cycle_deliqnuent</th>
      <td>22.2</td>
      <td>7.4</td>
      <td>70.4</td>
    </tr>
    <tr>
      <th>7+_cycle_deliqnuent</th>
      <td>6.7</td>
      <td>93.3</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
