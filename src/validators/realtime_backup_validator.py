import pandas as pd


class RealtimeBackupValidator:

    def validate(self, df: pd.DataFrame):
        errors = []
        bad_rows = pd.DataFrame()

        required_columns = [
            "order_total",
            "shipping_delay_days",
            "order_value_category",
            "processed_at"
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"Missing processed column: {col}")

        if errors:
            return df, errors, bad_rows

        invalid_total = df[df["order_total"].notna() & (df["order_total"] < 0)]
        if len(invalid_total) > 0:
            errors.append(f"{len(invalid_total)} rows have negative order_total")
            bad_rows = pd.concat([bad_rows, invalid_total])

        invalid_delay = df[df["shipping_delay_days"].notna() & (df["shipping_delay_days"] < 0)]
        if len(invalid_delay) > 0:
            errors.append(f"{len(invalid_delay)} rows have negative shipping_delay_days")
            bad_rows = pd.concat([bad_rows, invalid_delay])

        missing_category = df[df["order_total"].notna() & df["order_value_category"].isna()]
        if len(missing_category) > 0:
            errors.append(f"{len(missing_category)} rows have missing order_value_category")
            bad_rows = pd.concat([bad_rows, missing_category])

        if errors:
            print("REALTIME BACKUP VALIDATION ERRORS:")
            for error in errors:
                print("-", error)
        else:
            print("Realtime backup validation passed!")

        return df, errors, bad_rows.drop_duplicates()