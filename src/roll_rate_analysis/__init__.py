"""Roll rate analysis for credit risk scorecards."""

from importlib.metadata import PackageNotFoundError, version

from .mom import MOMRollRateTable
from .snapshot import SnapshotRollRateTable

try:
    __version__ = version("roll-rate-analysis")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = ("MOMRollRateTable", "SnapshotRollRateTable", "__version__")
