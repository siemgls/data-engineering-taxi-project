import pandas as pd


class TaxiValidator:

    def validate(self, df: pd.DataFrame):
        errors = []
        bad_rows = pd.DataFrame()

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

        # Check required columns
        for col in required_columns:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")

        # Stop if required columns are missing
        if errors:
            print("VALIDATION ERRORS:")
            for error in errors:
                print("-", error)

            return df, errors, bad_rows

        # Null checks
        for col in required_columns:
            null_rows = df[df[col].isna()]
            if len(null_rows) > 0:
                errors.append(f"{col} has {len(null_rows)} null values")
                bad_rows = pd.concat([bad_rows, null_rows])

        # Date logic
        bad_dates = df[df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]]
        if len(bad_dates) > 0:
            errors.append(f"{len(bad_dates)} rows have dropoff before pickup")
            bad_rows = pd.concat([bad_rows, bad_dates])

        # Negative trip distance
        negative_distance = df[df["trip_distance"] < 0]
        if len(negative_distance) > 0:
            errors.append(f"trip_distance has {len(negative_distance)} negative values")
            bad_rows = pd.concat([bad_rows, negative_distance])

        # Negative fare amount
        negative_fare = df[df["fare_amount"] < 0]
        if len(negative_fare) > 0:
            errors.append(f"fare_amount has {len(negative_fare)} negative values")
            bad_rows = pd.concat([bad_rows, negative_fare])

        # Negative total amount
        negative_total = df[df["total_amount"] < 0]
        if len(negative_total) > 0:
            errors.append(f"total_amount has {len(negative_total)} negative values")
            bad_rows = pd.concat([bad_rows, negative_total])

        # Invalid passenger count
        invalid_passenger_count = df[df["passenger_count"] < 0]
        if len(invalid_passenger_count) > 0:
            errors.append(f"passenger_count has {len(invalid_passenger_count)} invalid values")
            bad_rows = pd.concat([bad_rows, invalid_passenger_count])

        # Print result
        if errors:
            print("VALIDATION ERRORS:")
            for error in errors:
                print("-", error)
        else:
            print("Raw validation passed!")

        bad_rows = bad_rows.drop_duplicates()

        return df, errors, bad_rows