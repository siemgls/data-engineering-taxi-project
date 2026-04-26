import os
import sys
import pandas as pd

# Make src importable
sys.path.insert(0, os.path.abspath("."))

from dotenv import load_dotenv
from src.validators.taxi_validator import TaxiValidator
from src.processors.taxi_processor import TaxiProcessor
from src.validators.backup_validator import BackupValidator
from src.writers.local_writer import LocalWriter
from src.writers.azure_blob_writer import AzureBlobWriter
from src.utils.logger import ErrorLogger


INPUT_PATH = "data/input/yellow_tripdata_2026-01.parquet"
OUTPUT_PATH = "data/output/processed_taxi_2026_01.parquet"


def main():
    print("=== START TAXI PIPELINE ===")

    # Load environment variables
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

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

    # 7. Write output locally
    print("Writing output locally...")
    writer = LocalWriter()
    writer.write_parquet(processed_df, OUTPUT_PATH)

    # 8. Upload to Azure
    print("Uploading to Azure Blob Storage...")
    azure_writer = AzureBlobWriter(connection_string, container_name)

    azure_writer.upload_file(
        OUTPUT_PATH,
        "processed/processed_taxi_2026_01.parquet"
    )

    print("=== PIPELINE FINISHED ===")


if __name__ == "__main__":
    main()