"""Shared helpers for roll rate computation.

The matrix layout used by both classes is row=primary state, column=secondary
state. Each row has a single "diagonal" column where the account did not change
state (stable). Cells to the left of the diagonal are roll_down (state
improved), cells to the right are roll_up (state worsened). ``reduce_matrix``
collapses each row into those three buckets using a per-row diagonal index,
which is the only piece of metadata that differs between MOM (square) and
Snapshot (rectangular with extra rows in detailed mode).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

LABEL_COL = "from_state"

LazySource = pl.LazyFrame | pl.DataFrame | str | Path


def load_lazy(source: LazySource) -> pl.LazyFrame:
    """Return a polars LazyFrame regardless of input type."""
    if isinstance(source, pl.LazyFrame):
        return source
    if isinstance(source, pl.DataFrame):
        return source.lazy()
    if isinstance(source, str | Path):
        return pl.scan_csv(source)
    raise TypeError(
        f"Unsupported input type {type(source).__name__}; "
        "expected polars LazyFrame, DataFrame, or a file path."
    )


def cycle_row_tags(max_delq: int) -> list[str]:
    """Generate the canonical row/column tags ``0_cycle_delinquent`` … ``N+_cycle_delinquent``."""
    return [f"{i}_cycle_delinquent" for i in range(max_delq)] + [f"{max_delq}+_cycle_delinquent"]


def labeled_matrix(
    counts: np.ndarray,
    row_tags: list[str],
    column_tags: list[str],
) -> pl.DataFrame:
    """Wrap a 2D numpy count matrix in a polars DataFrame with labeled axes.

    The first column is named ``from_state`` and holds the row tags; the
    remaining columns are named after ``column_tags`` and hold the integer counts.
    """
    if counts.shape != (len(row_tags), len(column_tags)):
        raise ValueError(
            f"counts shape {counts.shape} does not match ({len(row_tags)}, {len(column_tags)})."
        )
    data: dict[str, list | np.ndarray] = {LABEL_COL: row_tags}
    for j, tag in enumerate(column_tags):
        data[tag] = counts[:, j]
    return pl.DataFrame(data)


def write_capped_counts(
    counts: np.ndarray,
    row: int,
    grouped: pl.DataFrame,
    value_col: str,
    max_delq: int,
) -> None:
    """Write per-secondary-value counts into ``counts[row]``, capping at ``max_delq``.

    ``grouped`` must be a polars DataFrame with two columns: ``value_col`` (the
    secondary delinquency value) and ``len`` (occurrence count). Values strictly
    greater than ``max_delq - 1`` are summed into ``counts[row, max_delq]``.
    """
    if grouped.height == 0:
        return
    below = grouped.filter(pl.col(value_col) <= max_delq - 1)
    above = grouped.filter(pl.col(value_col) > max_delq - 1)
    if below.height:
        counts[row, below[value_col].to_numpy()] += below["len"].to_numpy()
    if above.height:
        counts[row, max_delq] += int(above["len"].sum())


def reduce_matrix(
    matrix: pl.DataFrame,
    diag_cols: list[int],
    percentages: bool = True,
) -> pl.DataFrame:
    """Collapse a roll-rate matrix into roll_down / stable / roll_up per row.

    ``matrix`` is expected in the format produced by :func:`labeled_matrix`:
    a polars DataFrame with a ``from_state`` label column followed by numeric
    transition-count columns.

    ``diag_cols[i]`` gives, for row ``i``, the index (among the numeric
    columns, 0-based) where "stable" lives. Cells left of that index sum to
    ``roll_down``; the diagonal itself is ``stable``; cells right of it sum to
    ``roll_up``.
    """
    if LABEL_COL not in matrix.columns:
        raise ValueError(f"matrix must contain a '{LABEL_COL}' label column.")
    value_cols = [c for c in matrix.columns if c != LABEL_COL]
    values = matrix.select(value_cols).to_numpy()
    n_rows = values.shape[0]
    if len(diag_cols) != n_rows:
        raise ValueError(f"diag_cols has length {len(diag_cols)} but matrix has {n_rows} rows.")

    buckets = np.zeros((n_rows, 3), dtype=np.float64)
    for i, d in enumerate(diag_cols):
        row = values[i]
        buckets[i, 0] = row[:d].sum()
        buckets[i, 1] = row[d]
        buckets[i, 2] = row[d + 1 :].sum()

    if percentages:
        totals = buckets.sum(axis=1, keepdims=True)
        with np.errstate(invalid="ignore", divide="ignore"):
            buckets = np.where(totals > 0, 100 * buckets / totals, 0.0)
        buckets = np.round(buckets, 1)
        return pl.DataFrame(
            {
                LABEL_COL: matrix[LABEL_COL],
                "roll_down": buckets[:, 0],
                "stable": buckets[:, 1],
                "roll_up": buckets[:, 2],
            }
        )
    buckets_int = buckets.astype(np.int64)
    return pl.DataFrame(
        {
            LABEL_COL: matrix[LABEL_COL],
            "roll_down": buckets_int[:, 0],
            "stable": buckets_int[:, 1],
            "roll_up": buckets_int[:, 2],
        }
    )
