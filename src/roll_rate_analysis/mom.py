"""Month-over-month roll rate table."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import polars as pl
import polars.selectors as cs

from ._common import (
    LazySource,
    cycle_row_tags,
    labeled_matrix,
    load_lazy,
    reduce_matrix,
    write_capped_counts,
)

_MERGED = "_rr_merged_bin"
_MERGED_SECONDARY = _MERGED + "_secondary"


class MOMRollRateTable:
    """Month-over-month roll rate table for two consecutive monthly snapshots.

    Parameters
    ----------
    month_i:
        Data for month ``i``. Accepts a polars ``LazyFrame``/``DataFrame`` or a
        path/string pointing to a CSV file.
    month_i_plus_1:
        Data for month ``i+1``. Same supported types as ``month_i``.
    unique_key_col:
        Name of the account identifier column. Must exist in both inputs.
    delinquency_col:
        Name of the delinquency column (integer months past due). Must exist in
        both inputs.
    max_delq:
        Largest delinquency level kept as its own row/column. Anything above
        rolls into the ``N+`` bucket.
    binary_cols:
        Optional binary indicator columns to append to the matrix. Listed in
        descending priority — the first entry wins ties. Each indicator gets
        one extra row and column.

    Use
    ---
    >>> table = MOMRollRateTable(
    ...     "jan.csv", "feb.csv",
    ...     unique_key_col="id", delinquency_col="delq", max_delq=6,
    ... )
    >>> matrix = table.compute()      # polars.DataFrame, the full transition matrix
    >>> reduced = table.reduce()      # polars.DataFrame, roll_down / stable / roll_up

    ``compute`` and ``reduce`` are idempotent; the matrix is cached after the
    first call. Both return polars ``DataFrame``s whose first column
    (``from_state``) holds the row label.
    """

    def __init__(
        self,
        month_i: LazySource,
        month_i_plus_1: LazySource,
        *,
        unique_key_col: str,
        delinquency_col: str,
        max_delq: int = 6,
        binary_cols: Sequence[str] = (),
    ) -> None:
        if max_delq < 1:
            raise ValueError("max_delq must be >= 1.")
        binary_cols = tuple(binary_cols)
        if delinquency_col in binary_cols or unique_key_col in binary_cols:
            raise ValueError("binary_cols must not include the unique_key or delinquency columns.")

        self._month_i_source = month_i
        self._month_i_plus_1_source = month_i_plus_1
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.max_delq = max_delq
        self.binary_cols = binary_cols

        self.tags = cycle_row_tags(max_delq) + list(reversed(binary_cols))
        self._matrix: pl.DataFrame | None = None

    @property
    def matrix(self) -> pl.DataFrame:
        """Return the cached transition matrix, computing it on first access."""
        if self._matrix is None:
            self.compute()
        assert self._matrix is not None
        return self._matrix

    def compute(self) -> pl.DataFrame:
        """Compute the transition matrix and return it as a polars DataFrame."""
        n = self.max_delq + 1 + len(self.binary_cols)
        counts = np.zeros((n, n), dtype=np.int64)
        data = self._joined_frame()

        if self.binary_cols:
            self._accumulate_delq_to_delq(counts, data, exclude_binary=True)
            self._accumulate_delq_to_binary(counts, data)
            self._accumulate_binary_to_delq(counts, data)
            self._accumulate_binary_to_binary(counts, data)
        else:
            self._accumulate_delq_to_delq(counts, data, exclude_binary=False)

        self._matrix = labeled_matrix(counts, self.tags, self.tags)
        return self._matrix

    def reduce(self, percentages: bool = True) -> pl.DataFrame:
        """Return roll_down / stable / roll_up per row, in percentages or counts."""
        diag_cols = list(range(len(self.tags)))
        return reduce_matrix(self.matrix, diag_cols, percentages=percentages)

    # ----- pipeline -------------------------------------------------------

    def _joined_frame(self) -> pl.DataFrame:
        """Load both months, project the relevant columns, optionally merge binaries, and join."""
        select_cols = [self.unique_key_col, self.delinquency_col, *self.binary_cols]
        left = load_lazy(self._month_i_source).select(select_cols)
        right = load_lazy(self._month_i_plus_1_source).select(select_cols)

        if self.binary_cols:
            left = self._merge_binary_cols(left)
            right = self._merge_binary_cols(right)

        return left.join(right, how="left", on=self.unique_key_col, suffix="_secondary").collect()

    def _merge_binary_cols(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Collapse the binary indicator columns into one priority-valued column.

        Priority is encoded as ``len(binary_cols), len(binary_cols)-1, …, 1`` so
        ``binary_cols[0]`` (highest priority) gets the largest value. When more
        than one indicator is set on the same row, ``max_horizontal`` keeps the
        winner.
        """
        n = len(self.binary_cols)
        for idx, col in enumerate(self.binary_cols):
            priority = n - idx
            frame = frame.with_columns(
                pl.when(pl.col(col) == 1)
                .then(pl.lit(priority))
                .otherwise(pl.col(col))
                .alias(f"{col}__priority")
            )
        return frame.with_columns(
            pl.max_horizontal(cs.ends_with("__priority")).alias(_MERGED)
        ).drop(cs.ends_with("__priority"))

    # ----- accumulation per case ------------------------------------------

    def _accumulate_delq_to_delq(
        self,
        counts: np.ndarray,
        data: pl.DataFrame,
        *,
        exclude_binary: bool,
    ) -> None:
        """Accounts that had a normal delinquency status in both months."""
        if exclude_binary:
            data = data.filter((pl.col(_MERGED) == 0) & (pl.col(_MERGED_SECONDARY) == 0))
        secondary = f"{self.delinquency_col}_secondary"
        for cycle in self._observed_cycles(data, self.delinquency_col):
            grouped = self._group_counts(data, self.delinquency_col, cycle, secondary)
            write_capped_counts(
                counts, min(cycle, self.max_delq), grouped, secondary, self.max_delq
            )

    def _accumulate_delq_to_binary(self, counts: np.ndarray, data: pl.DataFrame) -> None:
        """Accounts that moved from a delinquency state into a binary indicator."""
        data = data.filter((pl.col(_MERGED) == 0) & (pl.col(_MERGED_SECONDARY) > 0))
        for cycle in self._observed_cycles(data, self.delinquency_col):
            grouped = self._group_counts(data, self.delinquency_col, cycle, _MERGED_SECONDARY)
            self._write_binary_secondary(counts, min(cycle, self.max_delq), grouped)

    def _accumulate_binary_to_delq(self, counts: np.ndarray, data: pl.DataFrame) -> None:
        """Accounts whose binary indicator was set in month i but had a delq state in i+1."""
        data = data.filter((pl.col(_MERGED) > 0) & (pl.col(_MERGED_SECONDARY) == 0))
        secondary = f"{self.delinquency_col}_secondary"
        for priority in self._observed_cycles(data, _MERGED):
            grouped = self._group_counts(data, _MERGED, priority, secondary)
            write_capped_counts(counts, self.max_delq + priority, grouped, secondary, self.max_delq)

    def _accumulate_binary_to_binary(self, counts: np.ndarray, data: pl.DataFrame) -> None:
        """Accounts whose binary indicator was set in both months."""
        data = data.filter((pl.col(_MERGED) > 0) & (pl.col(_MERGED_SECONDARY) > 0))
        for priority in self._observed_cycles(data, _MERGED):
            grouped = self._group_counts(data, _MERGED, priority, _MERGED_SECONDARY)
            self._write_binary_secondary(counts, self.max_delq + priority, grouped)

    # ----- low-level helpers ---------------------------------------------

    @staticmethod
    def _observed_cycles(data: pl.DataFrame, col: str) -> range:
        if data.height == 0:
            return range(0)
        return range(int(data[col].min()), int(data[col].max()) + 1)

    @staticmethod
    def _group_counts(
        data: pl.DataFrame, primary: str, primary_value: int, secondary: str
    ) -> pl.DataFrame:
        return (
            data.filter(pl.col(primary) == primary_value)
            .group_by([primary, secondary])
            .len()
            .sort(secondary)
        )

    def _write_binary_secondary(self, counts: np.ndarray, row: int, grouped: pl.DataFrame) -> None:
        """Apply counts where the secondary axis is a binary-priority column.

        Each priority ``k`` maps directly to column ``max_delq + k`` with no
        capping (unlike the delinquency axis, the priority values are exact).
        """
        if grouped.height == 0:
            return
        cols = grouped[_MERGED_SECONDARY].to_numpy() + self.max_delq
        counts[row, cols] += grouped["len"].to_numpy()


__all__ = ("MOMRollRateTable",)
