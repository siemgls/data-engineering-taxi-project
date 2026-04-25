import pandas as pd
import sys
import os

# make src importable
sys.path.insert(0, os.path.abspath("."))

from src.validators.taxi_validator import TaxiValidator
from src.processors.taxi_processor import TaxiProcessor
from src.validators.backup_validator import BackupValidator
from src.writers.local_writer import LocalWriter


INPUT_PATH = "data/input/yellow_tripdata_2026-01.parquet"
OUTPUT_PATH = "data/output/processed_taxi_2026_01.parquet"


def main():
    print("=== START TAXI PIPELINE ===")

    # 1. Read
    print("Reading data...")
    df = pd.read_parquet(INPUT_PATH)

    # 2. Validate (raw)
    print("Validating raw data...")
    validator = TaxiValidator()
    df = validator.validate(df)

    # 3. Process
    print("Processing data...")
    processor = TaxiProcessor()
    processed_df = processor.process(df)

    # 4. Backup validation
    print("Validating processed data...")
    backup_validator = BackupValidator()
    processed_df = backup_validator.validate(processed_df)

    # 5. Write
    print("Writing output...")
    writer = LocalWriter()
    writer.write_parquet(processed_df, OUTPUT_PATH)

    print("=== PIPELINE FINISHED ===")


if __name__ == "__main__":
    main()