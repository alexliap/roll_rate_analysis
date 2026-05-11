"""Snapshot roll rate table over observation and performance windows."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

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

_OBS_MAX = "obs_max_delq"
_PERF_MAX = "perf_max_delq"


@dataclass(frozen=True)
class _RowSpec:
    label: str
    level: int


class SnapshotRollRateTable:
    """Roll rate table for a snapshot month with observation and performance windows.

    For every account in the snapshot, the observation window is reduced to its
    maximum delinquency across the supplied observation files, and similarly for
    the performance window. The resulting transition matrix has rows indexed by
    the observation max-delinquency and columns indexed by the performance
    max-delinquency.

    Parameters
    ----------
    snapshot:
        Data for the snapshot month (defines the account universe). Accepts a
        polars ``LazyFrame``/``DataFrame`` or a path/string pointing to a CSV.
    observation:
        Sequence of frames or paths forming the observation window.
    performance:
        Sequence of frames or paths forming the performance window.
    unique_key_col:
        Name of the account identifier column. Must exist in every input.
    delinquency_col:
        Name of the delinquency column. Must exist in every observation and
        performance frame.
    max_delq:
        Largest delinquency level kept as its own row/column. Anything above
        rolls into the ``N+`` bucket.
    detailed:
        Split delinquency levels 3 and 4 into ``granularity`` sub-rows showing
        how many times the account hit that level during the observation window.
    granularity:
        Number of sub-rows per detailed level. Must be ≥ 2 when ``detailed``.
    keep_cols:
        Optional column whitelist applied to each observation/performance frame
        before joining (memory optimisation). Must include ``delinquency_col``.

    Use
    ---
    >>> table = SnapshotRollRateTable(
    ...     "snap.csv",
    ...     ["obs1.csv", "obs2.csv"],
    ...     ["perf1.csv", "perf2.csv"],
    ...     unique_key_col="id",
    ...     delinquency_col="delq",
    ...     detailed=True,
    ...     granularity=2,
    ... )
    >>> matrix = table.compute()      # polars.DataFrame, the full transition matrix
    >>> reduced = table.reduce()      # polars.DataFrame, roll_down / stable / roll_up

    ``compute`` and ``reduce`` are idempotent; the matrix is cached after the
    first call.
    """

    def __init__(
        self,
        snapshot: LazySource,
        observation: Sequence[LazySource],
        performance: Sequence[LazySource],
        *,
        unique_key_col: str,
        delinquency_col: str,
        max_delq: int = 6,
        detailed: bool = False,
        granularity: int = 1,
        keep_cols: Sequence[str] | None = None,
    ) -> None:
        if max_delq < 1:
            raise ValueError("max_delq must be >= 1.")
        if granularity < 1:
            raise ValueError("granularity must be >= 1.")
        if detailed and granularity < 2:
            raise ValueError("granularity must be >= 2 when detailed=True.")

        observation = list(observation)
        performance = list(performance)
        if not observation:
            raise ValueError("at least one observation frame is required.")
        if not performance:
            raise ValueError("at least one performance frame is required.")

        if keep_cols is not None:
            keep_cols = tuple(keep_cols)
            if delinquency_col not in keep_cols:
                raise ValueError(
                    "keep_cols must include the delinquency_col so that it survives projection."
                )

        self._snapshot_source = snapshot
        self._observation_sources = observation
        self._performance_sources = performance
        self.unique_key_col = unique_key_col
        self.delinquency_col = delinquency_col
        self.max_delq = max_delq
        self.detailed = detailed
        self.granularity = granularity if detailed else 1
        self.keep_cols = keep_cols

        self._row_specs = self._build_row_specs()
        self.row_tags = [s.label for s in self._row_specs]
        self.column_tags = cycle_row_tags(max_delq)
        self._matrix: pl.DataFrame | None = None

    @property
    def extra_rows(self) -> int:
        """Number of additional rows beyond ``max_delq + 1`` due to detailed mode."""
        return 2 * (self.granularity - 1) if self.detailed else 0

    @property
    def matrix(self) -> pl.DataFrame:
        """Return the cached transition matrix, computing it on first access."""
        if self._matrix is None:
            self.compute()
        assert self._matrix is not None
        return self._matrix

    def compute(self) -> pl.DataFrame:
        """Compute the transition matrix and return it as a polars DataFrame."""
        n_rows = self.max_delq + 1 + self.extra_rows
        n_cols = self.max_delq + 1
        counts = np.zeros((n_rows, n_cols), dtype=np.int64)

        data = self._build_joined().collect()
        if data.height > 0:
            cycles = range(int(data[_OBS_MAX].min()), int(data[_OBS_MAX].max()) + 1)
            for cycle in cycles:
                self._accumulate_cycle(counts, data, cycle)

        self._matrix = labeled_matrix(counts, self.row_tags, self.column_tags)
        return self._matrix

    def reduce(self, percentages: bool = True) -> pl.DataFrame:
        """Return roll_down / stable / roll_up per row, in percentages or counts."""
        diag_cols = [spec.level for spec in self._row_specs]
        return reduce_matrix(self.matrix, diag_cols, percentages=percentages)

    # ----- row layout -----------------------------------------------------

    def _build_row_specs(self) -> list[_RowSpec]:
        specs: list[_RowSpec] = []
        for i in range(self.max_delq):
            if self.detailed and i in (3, 4):
                for j in range(1, self.granularity):
                    specs.append(_RowSpec(f"{i}x{j}_cycle_delinquent", i))
                specs.append(_RowSpec(f"{i}x{self.granularity}+_cycle_delinquent", i))
            else:
                specs.append(_RowSpec(f"{i}_cycle_delinquent", i))
        specs.append(_RowSpec(f"{self.max_delq}+_cycle_delinquent", self.max_delq))
        return specs

    def _row_index(self, cycle: int, rank: int = 1) -> int:
        """Return the matrix row index for ``(cycle, rank)``.

        ``rank`` is only meaningful when ``detailed`` is on and ``cycle`` is 3 or 4.
        """
        if cycle >= self.max_delq:
            return self.max_delq + self.extra_rows
        if self.detailed and cycle in (3, 4):
            base = 3 if cycle == 3 else 3 + self.granularity
            return base + rank - 1
        if self.detailed and cycle >= 5:
            return cycle + self.extra_rows
        return cycle

    # ----- pipeline -------------------------------------------------------

    def _build_joined(self) -> pl.LazyFrame:
        """Build the merged frame of ``(unique_key, obs_max_delq, perf_max_delq, …)``."""
        snapshot = load_lazy(self._snapshot_source).select([self.unique_key_col])
        obs = self._build_window(snapshot, self._observation_sources, _OBS_MAX, "obs")
        if self.detailed:
            obs = obs.with_columns(
                [
                    pl.sum_horizontal(cs.starts_with(self.delinquency_col) == 3).alias(
                        "obs_times_3_cycle"
                    ),
                    pl.sum_horizontal(cs.starts_with(self.delinquency_col) == 4).alias(
                        "obs_times_4_cycle"
                    ),
                ]
            )
        perf = self._build_window(snapshot, self._performance_sources, _PERF_MAX, "perf")

        joined = obs.join(perf, how="left", on=self.unique_key_col, suffix="_perfwin")
        keep = [self.unique_key_col, _OBS_MAX, _PERF_MAX]
        if self.detailed:
            keep = [
                self.unique_key_col,
                _OBS_MAX,
                "obs_times_3_cycle",
                "obs_times_4_cycle",
                _PERF_MAX,
            ]
        return joined.select(keep)

    def _build_window(
        self,
        snapshot: pl.LazyFrame,
        sources: Sequence[LazySource],
        max_alias: str,
        suffix_tag: str,
    ) -> pl.LazyFrame:
        """Join each window file into ``snapshot`` and reduce to one max-delq column."""
        result = snapshot
        for i, src in enumerate(sources):
            frame = load_lazy(src)
            if self.keep_cols is not None:
                frame = frame.select([self.unique_key_col, *self.keep_cols])
            result = result.join(
                frame,
                how="left",
                on=self.unique_key_col,
                suffix=f"_{suffix_tag}{i}",
            )
        return result.with_columns(
            pl.max_horizontal(cs.starts_with(self.delinquency_col)).alias(max_alias)
        )

    # ----- accumulation per cycle ----------------------------------------

    def _accumulate_cycle(self, counts: np.ndarray, data: pl.DataFrame, cycle: int) -> None:
        rows = data.filter(pl.col(_OBS_MAX) == cycle)
        if rows.height == 0:
            return

        if self.detailed and cycle in (3, 4):
            self._accumulate_detailed(counts, rows, cycle)
            return

        grouped = rows.group_by([_OBS_MAX, _PERF_MAX]).len().sort(_PERF_MAX)
        write_capped_counts(counts, self._row_index(cycle), grouped, _PERF_MAX, self.max_delq)

    def _accumulate_detailed(self, counts: np.ndarray, rows: pl.DataFrame, cycle: int) -> None:
        times_col = f"obs_times_{cycle}_cycle"
        grouped = (
            rows.filter(pl.col(times_col) >= 1)
            .group_by([times_col, _PERF_MAX])
            .len()
            .sort([times_col, _PERF_MAX])
        )
        for rank in range(1, self.granularity + 1):
            if rank < self.granularity:
                sub = grouped.filter(pl.col(times_col) == rank)
            else:
                sub = (
                    grouped.filter(pl.col(times_col) >= rank)
                    .group_by(_PERF_MAX)
                    .agg(pl.col("len").sum())
                    .sort(_PERF_MAX)
                )
            row_idx = self._row_index(cycle, rank)
            write_capped_counts(counts, row_idx, sub, _PERF_MAX, self.max_delq)


__all__ = ("SnapshotRollRateTable",)
