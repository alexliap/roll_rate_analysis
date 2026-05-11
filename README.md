# Roll Rate Analysis

![deploy on pypi](https://github.com/alexliap/roll_rate_analysis/actions/workflows/publish-package.yaml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/roll-rate-analysis?label=pypi%20package)
![Downloads](https://static.pepy.tech/badge/roll-rate-analysis)

Roll rate analysis is a credit-risk technique used to define the target variable when building Application or Behavioural scorecards. It's an iterative process — this package parametrises the moving parts so each iteration is a few lines of code rather than a fresh notebook.

The library has zero pandas dependency: inputs and outputs are [Polars](https://pola.rs/) frames.

## Installation

From PyPI:

```bash
uv add roll-rate-analysis        # uv projects
pip install roll-rate-analysis   # plain pip
```

Requires Python 3.10 or newer.

## What's in the box

Two classes, one method each:

| Class | Use case |
| --- | --- |
| `MOMRollRateTable` | Transition matrix between two consecutive months. |
| `SnapshotRollRateTable` | Transition matrix between an observation window and a performance window around a snapshot month. |

Both expose `compute()` (full transition matrix) and `reduce()` (roll_down / stable / roll_up summary). Both return polars `DataFrame`s whose first column (`from_state`) holds the row label.

## Quick start

```python
from roll_rate_analysis import MOMRollRateTable

table = MOMRollRateTable(
    "data/jan.csv",
    "data/feb.csv",
    unique_key_col="id",
    delinquency_col="delq",
    max_delq=6,
)

table.compute()    # polars.DataFrame, full transition matrix
table.reduce()     # polars.DataFrame, roll_down / stable / roll_up percentages
```

In-memory polars frames work too:

```python
import polars as pl
from roll_rate_analysis import SnapshotRollRateTable

snap = pl.read_csv("data/snap.csv")
obs = [pl.scan_csv(p) for p in ["data/obs1.csv", "data/obs2.csv"]]
perf = [pl.scan_csv(p) for p in ["data/perf1.csv", "data/perf2.csv"]]

table = SnapshotRollRateTable(
    snap, obs, perf,
    unique_key_col="id",
    delinquency_col="delq",
    detailed=True,
    granularity=2,
)
table.compute()
```

See the notebooks under [`examples/`](examples/) for end-to-end walkthroughs.

## Development

This project uses [uv](https://docs.astral.sh/uv/). Clone and bootstrap with:

```bash
git clone https://github.com/alexliap/roll_rate_analysis.git
cd roll_rate_analysis
uv sync --dev
```

Run the test suite, linter, and formatter:

```bash
uv run pytest
uv run ruff check .
uv run ruff format .
```

Pre-commit hooks (ruff + standard checks) keep the tree clean:

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

## License

MIT — see [LICENSE](LICENSE).
