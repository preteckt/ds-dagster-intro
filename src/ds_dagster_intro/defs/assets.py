"""Dagster assets for the telematics pipeline.

The DAG is:

    preprocessed_telematics  -->  telematics_summary

Dagster infers the dependency from the parameter name: ``telematics_summary``
takes an argument called ``preprocessed_telematics``, so it runs after that
asset and receives its output.
"""

from pathlib import Path

import dagster as dg
import pandas as pd

from ds_dagster_intro.defs.processing import (
    compute_correlations,
    compute_distributions,
    interpolate_vehicle_speed,
    load_telematics,
)

# src/ds_dagster_intro/defs/assets.py -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]


class TelematicsConfig(dg.Config):
    """Run configuration, editable from the Launchpad in the Dagster UI."""

    # Relative paths are resolved against the project root.
    csv_path: str = "data/example_telematics_data.csv"


@dg.asset(group_name="telematics")
def preprocessed_telematics(
    context: dg.AssetExecutionContext, config: TelematicsConfig
) -> pd.DataFrame:
    """Raw telematics data with gaps in vehicle speed interpolated."""
    raw = load_telematics(PROJECT_ROOT / config.csv_path)
    df = interpolate_vehicle_speed(raw)

    # Metadata shows up on the asset page in the UI.
    context.add_output_metadata(
        {
            "source": config.csv_path,
            "num_rows": len(df),
            "vehicle_speed_nans_before": int(raw["vehicle_speed_kmph"].isna().sum()),
            "vehicle_speed_nans_after": int(df["vehicle_speed_kmph"].isna().sum()),
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
        }
    )
    return df


@dg.asset(group_name="telematics")
def telematics_summary(
    context: dg.AssetExecutionContext, preprocessed_telematics: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Distributions of and correlations between vehicle and engine speed."""
    distributions = compute_distributions(preprocessed_telematics)
    correlations = compute_correlations(preprocessed_telematics)

    context.add_output_metadata(
        {
            "distributions": dg.MetadataValue.md(distributions.to_markdown()),
            "correlations": dg.MetadataValue.md(correlations.to_markdown()),
            "speed_rpm_correlation": float(
                correlations.loc["vehicle_speed_kmph", "engine_speed_rpm"]
            ),
        }
    )
    return {"distributions": distributions, "correlations": correlations}
