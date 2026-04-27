import pandas as pd
import numpy as np


class RealtimeProcessor:

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove duplicate orders
        df = df.drop_duplicates(subset=["order_id"], keep="first")

        # Extra column 1
        df["order_total"] = np.where(
            (df["quantity"] > 0) &
            (df["unit_price"] >= 0) &
            (df["discount"].between(0, 1)),
            df["quantity"] * df["unit_price"] * (1 - df["discount"]),
            np.nan
        )

        # Extra column 2
        df["shipping_delay_days"] = (
            df["ship_date"] - df["order_date"]
        ).dt.days

        # Extra column 3
        df["order_value_category"] = pd.cut(
            df["order_total"],
            bins=[-float("inf"), 25, 100, float("inf")],
            labels=["Low", "Medium", "High"]
        )

        # Extra column 4
        df["processed_at"] = pd.Timestamp.now()

        return df