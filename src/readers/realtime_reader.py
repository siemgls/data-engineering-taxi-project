import pandas as pd


class RealtimeReader:

    def read(self, file_path: str) -> pd.DataFrame:
        if file_path.endswith(".csv"):
            return pd.read_csv(file_path)

        if file_path.endswith(".xlsx"):
            return pd.read_excel(file_path)

        raise ValueError(f"Unsupported file type: {file_path}")