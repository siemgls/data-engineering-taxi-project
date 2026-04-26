import os
import pandas as pd
from datetime import datetime


class ErrorLogger:
    def __init__(self, base_dir="data/error"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path(self, prefix: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.base_dir, f"{prefix}_{ts}.csv")

    def log_messages(self, messages: list[str], prefix: str = "errors"):
        if not messages:
            return None
        path = self._path(prefix)
        df = pd.DataFrame({"error": messages})
        df.to_csv(path, index=False)
        print(f"Error log written to {path}")
        return path

    def log_rows(self, df: pd.DataFrame, prefix: str = "bad_rows"):
        if df is None or len(df) == 0:
            return None
        path = self._path(prefix)
        df.to_csv(path, index=False)
        print(f"Bad rows written to {path}")
        return path