import sys
import os
import time
import pandas as pd

sys.path.insert(0, os.path.abspath("."))

from src.validators.taxi_validator import TaxiValidator
from src.processors.taxi_processor import TaxiProcessor
from src.validators.backup_validator import BackupValidator
from src.writers.local_writer import LocalWriter
from src.utils.logger import ErrorLogger


WATCH_FOLDER = "data/input/realtime"
OUTPUT_FOLDER = "data/output/realtime"


def process_file(file_path):
    print(f"Processing new file: {file_path}")

    df = pd.read_csv(file_path)

    # Convert CSV string columns to datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"],
        errors="coerce"
    )

    validator = TaxiValidator()
    processor = TaxiProcessor()
    backup_validator = BackupValidator()
    writer = LocalWriter()
    logger = ErrorLogger()

    df, errors, bad_rows = validator.validate(df)
    logger.log_messages(errors, "realtime_raw_errors")
    logger.log_rows(bad_rows, "realtime_raw_bad_rows")

    processed_df = processor.process(df)

    processed_df, errors, bad_rows = backup_validator.validate(processed_df)
    logger.log_messages(errors, "realtime_backup_errors")
    logger.log_rows(bad_rows, "realtime_backup_bad_rows")

    filename = os.path.basename(file_path)
    output_path = os.path.join(OUTPUT_FOLDER, f"processed_{filename}")

    writer.write_csv(processed_df, output_path)


def watch_folder():
    print("Watching folder for new files...")

    os.makedirs(WATCH_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    processed_files = set()

    while True:
        files = os.listdir(WATCH_FOLDER)

        for file in files:
            file_path = os.path.join(WATCH_FOLDER, file)

            if os.path.isfile(file_path) and file not in processed_files:
                try:
                    process_file(file_path)
                    processed_files.add(file)
                except Exception as e:
                    print(f"Error processing {file}: {e}")

        time.sleep(5)


if __name__ == "__main__":
    watch_folder()