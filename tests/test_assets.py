import unittest

import dagster as dg
import numpy as np
import pandas as pd

from ds_dagster_intro.defs.assets import preprocessed_telematics, telematics_summary
from ds_dagster_intro.defs.processing import (
    compute_correlations,
    compute_distributions,
    interpolate_vehicle_speed,
)


def make_frame(vehicle_speed, engine_speed=None):
    """Build a small 1 Hz telematics frame for tests."""
    n = len(vehicle_speed)
    if engine_speed is None:
        engine_speed = [700.0] * n
    return pd.DataFrame(
        {
            "serial": ["test-serial"] * n,
            "timestamp": pd.date_range("2026-01-01", periods=n, freq="1s", tz="UTC"),
            "vehicle_speed_kmph": vehicle_speed,
            "engine_speed_rpm": engine_speed,
        }
    )


class TestInterpolateVehicleSpeed(unittest.TestCase):
    def test_interpolate_fills_gaps_linearly(self):
        df = make_frame([0.0, np.nan, np.nan, np.nan, np.nan, 10.0])

        result = interpolate_vehicle_speed(df)

        self.assertEqual(
            result["vehicle_speed_kmph"].tolist(), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0]
        )

    def test_interpolate_preserves_reported_values_and_shape(self):
        df = make_frame([5.0, np.nan, 7.0, np.nan, 3.0])

        result = interpolate_vehicle_speed(df)

        self.assertEqual(result.shape, df.shape)
        self.assertEqual(list(result.columns), list(df.columns))
        reported = df["vehicle_speed_kmph"].notna()
        self.assertEqual(
            result.loc[reported, "vehicle_speed_kmph"].tolist(), [5.0, 7.0, 3.0]
        )


class TestSummary(unittest.TestCase):
    def test_summary_distributions_and_correlations(self):
        speed = np.arange(10, dtype=float)
        df = make_frame(speed, engine_speed=700.0 + 50.0 * speed)

        distributions = compute_distributions(df)
        correlations = compute_correlations(df)

        self.assertIn("count", distributions.index)
        self.assertIn("mean", distributions.index)
        self.assertAlmostEqual(distributions.loc["mean", "vehicle_speed_kmph"], 4.5)
        self.assertAlmostEqual(
            correlations.loc["vehicle_speed_kmph", "engine_speed_rpm"], 1.0
        )


class TestPipeline(unittest.TestCase):
    def test_assets_materialize_end_to_end(self):
        result = dg.materialize([preprocessed_telematics, telematics_summary])
        self.assertTrue(result.success)

        df = result.output_for_node("preprocessed_telematics")
        self.assertEqual(
            list(df.columns),
            ["serial", "timestamp", "vehicle_speed_kmph", "engine_speed_rpm"],
        )
        # Only leading/trailing gaps may remain after interpolation.
        speed = df["vehicle_speed_kmph"]
        inside = speed.loc[speed.first_valid_index() : speed.last_valid_index()]
        self.assertEqual(inside.isna().sum(), 0)

        summary = result.output_for_node("telematics_summary")
        signals = ["vehicle_speed_kmph", "engine_speed_rpm"]
        self.assertEqual(list(summary["distributions"].columns), signals)
        self.assertEqual(summary["correlations"].shape, (2, 2))
        self.assertEqual(list(summary["correlations"].columns), signals)


if __name__ == "__main__":
    unittest.main()
