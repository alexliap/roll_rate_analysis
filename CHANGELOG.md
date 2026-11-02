# Changelog

All notable changes to this project are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

Major rewrite. Modernized packaging, polars-only output, significantly cleaner
internals.

### Breaking changes

| Area | Before | After |
| --- | --- | --- |
| Output type | `pandas.DataFrame` | `polars.DataFrame` with a `from_state` column |
| Method to get the matrix | `table.build(); table.get_roll_rates()` | `table.compute()` |
| Method to get the matrix (snapshot) | `table.compute(); table.get_roll_rates()` | `table.compute()` |
| Constructor positional arg | `path_i=`, `path_i_1=` | positional `month_i`, `month_i_plus_1` |
| Snapshot constructor names | `snapshot_file`, `obs_files`, `perf_files` | `snapshot`, `observation`, `performance` |
| Other constructor args | positional or keyword | **keyword-only** after the data inputs |
| pandas dependency | required | gone — depends only on `numpy` and `polars>=1.0` |
| Python support | 3.8 – 3.10 | **3.10+** |

#### Before

```python
from roll_rate_analysis import MOMRollRateTable

table = MOMRollRateTable(
    unique_key_col="id",
    delinquency_col="delq",
    path_i="jan.csv",
    path_i_1="feb.csv",
    max_delq=6,
)
table.build()
matrix = table.get_roll_rates()  # pandas.DataFrame, row labels in the index
```

#### After

```python
from roll_rate_analysis import MOMRollRateTable

table = MOMRollRateTable(
    "jan.csv",
    "feb.csv",
    unique_key_col="id",
    delinquency_col="delq",
    max_delq=6,
)
matrix = table.compute()  # polars.DataFrame, row labels in `from_state`
```

### Added

- Both classes now accept `pl.LazyFrame`, `pl.DataFrame`, or path-like inputs
  interchangeably — no need to write CSVs to disk first.
- Constructors do **no I/O**. Filesystem access is deferred to `compute()`.
- `compute()` and `reduce()` are idempotent; the matrix is cached and
  reaccessible via the `table.matrix` property.
- Hand-crafted snapshot correctness tests verifying cell-by-cell matrix
  entries and the detailed-mode `3x1` / `3x2+` granularity split (previously
  only an invariant was checked).

### Changed

- Snapshot `_update_matrix`'s cycle-branch chain
  (`cycle in [0,1,2] / == 3 / == 4 / == 5 / >= max`) replaced by a single
  `_row_index(cycle, rank)` helper.
- MOM's `case=1..4` magic-number dispatch replaced by four named accumulator
  methods (`_accumulate_delq_to_delq`, `_accumulate_delq_to_binary`,
  `_accumulate_binary_to_delq`, `_accumulate_binary_to_binary`).
- Both `reduce()` methods collapsed into a shared
  `_common.reduce_matrix(matrix, diag_cols)`. Removes the deprecated
  `np.matrix` usage and an ~80-line snapshot reduce.
- Packaging: `setup.py` → `pyproject.toml` (hatchling backend); `uv` for
  dependency management; `uv.lock` committed.
- Lint/format toolchain: black + isort + flake8 → **ruff**.
- Publish workflow uses `uv build` and **PyPI trusted publishing via OIDC**;
  `PYPI_API_TOKEN` is no longer used. Register the repository as a trusted
  publisher on PyPI before tagging this release.
- Docs workflow uses `uv sync --dev` + `uv run sphinx-build`.

### Fixed

- `__all__` in `__init__.py` was a tuple of class objects; now a tuple of
  strings.
- `binary_cols: list[str] = []` mutable default argument.

### Removed

- pandas dependency.
- Python 3.8 / 3.9 support.

### Migration checklist

1. `uv add roll-rate-analysis` or `pip install --upgrade roll-rate-analysis`.
2. Rename `path_i=` / `path_i_1=` to positional `month_i` / `month_i_plus_1`
   (or `snapshot` / `observation` / `performance` for the snapshot class).
3. Replace `table.build(); table.get_roll_rates()` with `table.compute()`.
4. If a downstream consumer needs a pandas DataFrame, call `.to_pandas()` on
   the returned polars frame.
5. Row labels live in the `from_state` column rather than the pandas index —
   adjust any code that filtered by row label.

[0.2.0]: https://github.com/alexliap/roll_rate_analysis/releases/tag/v0.2.0
