import os
import sys
import pandas as pd

# Make src importable
sys.path.insert(0, os.path.abspath("."))

from src.validators.taxi_validator import TaxiValidator
from src.processors.taxi_processor import TaxiProcessor
from src.validators.backup_validator import BackupValidator
from src.writers.local_writer import LocalWriter
from src.utils.logger import ErrorLogger


INPUT_PATH = "data/input/yellow_tripdata_2026-01.parquet"
OUTPUT_PATH = "data/output/processed_taxi_2026_01.parquet"


def main():
    print("=== START TAXI PIPELINE ===")

    # 1. Read
    print("Reading data...")
    df = pd.read_parquet(INPUT_PATH)

    # 2. Raw validation
    print("Validating raw data...")
    validator = TaxiValidator()
    df, raw_errors, raw_bad_rows = validator.validate(df)

    # 3. Log raw validation errors
    logger = ErrorLogger()
    logger.log_messages(raw_errors, "raw_validation_errors")
    logger.log_rows(raw_bad_rows, "raw_bad_rows")

    # 4. Process
    print("Processing data...")
    processor = TaxiProcessor()
    processed_df = processor.process(df)

    # 5. Backup validation
    print("Validating processed data...")
    backup_validator = BackupValidator()
    processed_df, backup_errors, backup_bad_rows = backup_validator.validate(processed_df)

    # 6. Log backup validation errors
    logger.log_messages(backup_errors, "backup_validation_errors")
    logger.log_rows(backup_bad_rows, "backup_bad_rows")

    # 7. Write output
    print("Writing output...")
    writer = LocalWriter()
    writer.write_parquet(processed_df, OUTPUT_PATH)

    print("=== PIPELINE FINISHED ===")


if __name__ == "__main__":
    main()