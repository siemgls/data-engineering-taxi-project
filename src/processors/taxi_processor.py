import pandas as pd
import numpy as np

class TaxiProcessor:

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove required columns
        columns_to_remove = ["VendorID", "store_and_fwd_flag", "RatecodeID"]
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])

        # Trip duration in minutes
        df["trip_duration_minutes"] = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60

        # Average speed, only if duration > 0
        df["average_speed_mph"] = np.where(
            df["trip_duration_minutes"] > 0,
            df["trip_distance"] / (df["trip_duration_minutes"] / 60),
            np.nan
        )

        # Pickup year/month
        df["pickup_year"] = df["tpep_pickup_datetime"].dt.year
        df["pickup_month"] = df["tpep_pickup_datetime"].dt.month

        # Revenue per mile, only if distance > 0
        df["revenue_per_mile"] = np.where(
            df["trip_distance"] > 0,
            df["total_amount"] / df["trip_distance"],
            np.nan
        )

        # Distance category
        df["trip_distance_category"] = pd.cut(
            df["trip_distance"],
            bins=[-float("inf"), 2, 10, float("inf")],
            labels=["Short", "Medium", "Long"],
            right=False
        )

        # Fare category
        df["fare_category"] = pd.cut(
            df["fare_amount"],
            bins=[-float("inf"), 20, 50, float("inf")],
            labels=["Low", "Medium", "High"],
            right=False
        )

        # Time of day
        hour = df["tpep_pickup_datetime"].dt.hour

        df["trip_time_of_day"] = pd.cut(
            hour,
            bins=[-1, 5, 11, 17, 23],
            labels=["Night", "Morning", "Afternoon", "Evening"]
        )

        return df