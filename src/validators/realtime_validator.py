import pandas as pd


class RealtimeValidator:

    def validate(self, df: pd.DataFrame):
        errors = []
        bad_rows = pd.DataFrame()

        required_columns = [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "ship_date",
            "quantity",
            "unit_price",
            "discount",
            "payment_method",
            "country",
            "city",
            "status"
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"Missing column: {col}")

        if errors:
            return df, errors, bad_rows

        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

        for col in required_columns:
            invalid = df[df[col].isna()]
            if len(invalid) > 0:
                errors.append(f"{col} has {len(invalid)} missing values")
                bad_rows = pd.concat([bad_rows, invalid])

        for col in ["customer_id", "country", "city"]:
            invalid = df[df[col].astype(str).str.strip() == ""]
            if len(invalid) > 0:
                errors.append(f"{col} has {len(invalid)} empty values")
                bad_rows = pd.concat([bad_rows, invalid])

        duplicates = df[df.duplicated(subset=["order_id"], keep=False)]
        if len(duplicates) > 0:
            errors.append(f"{len(duplicates)} rows have duplicate order_id values")
            bad_rows = pd.concat([bad_rows, duplicates])

        invalid_quantity = df[df["quantity"] <= 0]
        if len(invalid_quantity) > 0:
            errors.append(f"{len(invalid_quantity)} rows have quantity <= 0")
            bad_rows = pd.concat([bad_rows, invalid_quantity])

        invalid_price = df[df["unit_price"] < 0]
        if len(invalid_price) > 0:
            errors.append(f"{len(invalid_price)} rows have unit_price < 0")
            bad_rows = pd.concat([bad_rows, invalid_price])

        invalid_discount = df[(df["discount"] < 0) | (df["discount"] > 1)]
        if len(invalid_discount) > 0:
            errors.append(f"{len(invalid_discount)} rows have discount outside 0-1")
            bad_rows = pd.concat([bad_rows, invalid_discount])

        invalid_dates = df[df["ship_date"] < df["order_date"]]
        if len(invalid_dates) > 0:
            errors.append(f"{len(invalid_dates)} rows have ship_date before order_date")
            bad_rows = pd.concat([bad_rows, invalid_dates])

        allowed_payments = ["credit_card", "debit_card", "paypal", "cash"]
        invalid_payment = df[~df["payment_method"].isin(allowed_payments)]
        if len(invalid_payment) > 0:
            errors.append(f"{len(invalid_payment)} rows have invalid payment_method")
            bad_rows = pd.concat([bad_rows, invalid_payment])

        allowed_statuses = ["completed", "cancelled", "pending", "returned"]
        invalid_status = df[~df["status"].isin(allowed_statuses)]
        if len(invalid_status) > 0:
            errors.append(f"{len(invalid_status)} rows have invalid status")
            bad_rows = pd.concat([bad_rows, invalid_status])

        if errors:
            print("REALTIME VALIDATION ERRORS:")
            for error in errors:
                print("-", error)
        else:
            print("Realtime validation passed!")

        return df, errors, bad_rows.drop_duplicates()