"""Pure pandas logic used by the Dagster assets.

Keeping the data transformations in plain functions (no Dagster imports) makes
them easy to unit test and to reuse from notebooks.
"""

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

SIGNAL_COLUMNS = ("vehicle_speed_kmph", "engine_speed_rpm")


def load_telematics(csv_path: str | Path) -> pd.DataFrame:
    """Read a telematics CSV and return it sorted by a parsed timestamp column."""
    df = pd.read_csv(csv_path, index_col=0)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def interpolate_vehicle_speed(df: pd.DataFrame) -> pd.DataFrame:
    """Fill the gaps in vehicle speed by interpolating over time.

    Vehicle speed is reported every 5 seconds while engine speed is reported
    every second, so most vehicle speed rows are NaN. ``method="time"`` weights
    the interpolation by the actual time between samples, which matters when
    the reporting interval is irregular. Leading/trailing NaNs are left as-is.
    """
    out = df.set_index("timestamp")
    out["vehicle_speed_kmph"] = out["vehicle_speed_kmph"].interpolate(
        method="time", limit_area="inside"
    )
    return out.reset_index()[df.columns]


def compute_distributions(
    df: pd.DataFrame, columns: Sequence[str] = SIGNAL_COLUMNS
) -> pd.DataFrame:
    """Summary statistics (count, mean, std, quartiles, ...) for each column."""
    return df[list(columns)].describe()


def compute_correlations(
    df: pd.DataFrame, columns: Sequence[str] = SIGNAL_COLUMNS
) -> pd.DataFrame:
    """Pairwise Pearson correlation matrix between the columns."""
    return df[list(columns)].corr()
