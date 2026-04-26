import pandas as pd
import numpy as np


class BackupValidator:

    def validate(self, df: pd.DataFrame):
        errors = []
        bad_rows = pd.DataFrame()

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

        # Check if processor created all required columns
        for col in required_new_columns:
            if col not in df.columns:
                errors.append(f"Missing processed column: {col}")

        if errors:
            print("BACKUP VALIDATION ERRORS:")
            for error in errors:
                print("-", error)

            return df, errors, bad_rows

        # Negative duration
        negative_duration = df[df["trip_duration_minutes"] < 0]
        if len(negative_duration) > 0:
            errors.append(f"{len(negative_duration)} rows have negative trip duration")
            bad_rows = pd.concat([bad_rows, negative_duration])

        # Infinite speed
        infinite_speed = df[np.isinf(df["average_speed_mph"])]
        if len(infinite_speed) > 0:
            errors.append(f"{len(infinite_speed)} rows have infinite average speed")
            bad_rows = pd.concat([bad_rows, infinite_speed])

        # Infinite revenue per mile
        infinite_revenue = df[np.isinf(df["revenue_per_mile"])]
        if len(infinite_revenue) > 0:
            errors.append(f"{len(infinite_revenue)} rows have infinite revenue per mile")
            bad_rows = pd.concat([bad_rows, infinite_revenue])

        # Missing category values
        missing_distance_category = df[df["trip_distance_category"].isna()]
        if len(missing_distance_category) > 0:
            errors.append(f"{len(missing_distance_category)} rows have missing trip distance category")
            bad_rows = pd.concat([bad_rows, missing_distance_category])

        missing_fare_category = df[df["fare_category"].isna()]
        if len(missing_fare_category) > 0:
            errors.append(f"{len(missing_fare_category)} rows have missing fare category")
            bad_rows = pd.concat([bad_rows, missing_fare_category])

        missing_time_of_day = df[df["trip_time_of_day"].isna()]
        if len(missing_time_of_day) > 0:
            errors.append(f"{len(missing_time_of_day)} rows have missing trip time of day")
            bad_rows = pd.concat([bad_rows, missing_time_of_day])

        if errors:
            print("BACKUP VALIDATION ERRORS:")
            for error in errors:
                print("-", error)
        else:
            print("Backup validation passed!")

        bad_rows = bad_rows.drop_duplicates()

        return df, errors, bad_rows