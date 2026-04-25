import pandas as pd
import numpy as np


class BackupValidator:

    def validate(self, df: pd.DataFrame):
        errors = []

        required_new_columns = [
            "trip_duration_minutes",
            "average_speed_mph",
            "pickup_year",
            "pickup_month",
            "revenue_per_mile",
            "trip_distance_category",
            "fare_category",
            "trip_time_of_day"
        ]

        for col in required_new_columns:
            if col not in df.columns:
                errors.append(f"Missing processed column: {col}")

        negative_duration = (df["trip_duration_minutes"] < 0).sum()
        if negative_duration > 0:
            errors.append(f"{negative_duration} rows have negative trip duration")

        infinite_speed = np.isinf(df["average_speed_mph"]).sum()
        if infinite_speed > 0:
            errors.append(f"{infinite_speed} rows have infinite average speed")

        infinite_revenue = np.isinf(df["revenue_per_mile"]).sum()
        if infinite_revenue > 0:
            errors.append(f"{infinite_revenue} rows have infinite revenue per mile")

        if errors:
            print("BACKUP VALIDATION ERRORS:")
            for error in errors:
                print("-", error)
        else:
            print("Backup validation passed!")

        return df