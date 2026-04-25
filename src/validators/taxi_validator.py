import pandas as pd


class TaxiValidator:

    def validate(self, df: pd.DataFrame):
        errors = []

        # 1. Required columns
        required_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "total_amount"
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")

        # 2. Null check (only required columns)
        for col in required_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                errors.append(f"{col} has {null_count} null values")

        # 3. Date logic
        bad_dates = df[df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]]
        if len(bad_dates) > 0:
            errors.append(f"{len(bad_dates)} rows have dropoff before pickup")

        # 4. Negative values
        numeric_checks = ["trip_distance", "fare_amount", "total_amount"]

        for col in numeric_checks:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                errors.append(f"{col} has {negative_count} negative values")

        # 5. Passenger count check
        invalid_passengers = (df["passenger_count"] < 0).sum()
        if invalid_passengers > 0:
            errors.append(f"{invalid_passengers} invalid passenger counts")

        # Result
        if errors:
            print("VALIDATION ERRORS:")
            for e in errors:
                print("-", e)

        else:
            print("Validation passed!")

        return df