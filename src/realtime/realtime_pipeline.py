import os
import sys
import time
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath("."))

from src.readers.realtime_reader import RealtimeReader
from src.validators.realtime_validator import RealtimeValidator
from src.processors.realtime_processor import RealtimeProcessor
from src.validators.realtime_backup_validator import RealtimeBackupValidator
from src.writers.local_writer import LocalWriter
from src.writers.azure_blob_writer import AzureBlobWriter
from src.utils.logger import ErrorLogger


WATCH_FOLDER = "data/input/realtime"
OUTPUT_FOLDER = "data/output/realtime"
ARCHIVE_FOLDER = "data/archive/realtime"


def process_file(file_path: str):
    print(f"Processing new file: {file_path}")

    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")

    reader = RealtimeReader()
    validator = RealtimeValidator()
    processor = RealtimeProcessor()
    backup_validator = RealtimeBackupValidator()
    local_writer = LocalWriter()
    azure_writer = AzureBlobWriter(connection_string, container_name)
    logger = ErrorLogger()

    df = reader.read(file_path)

    df, errors, bad_rows = validator.validate(df)
    logger.log_messages(errors, "realtime_raw_errors")
    logger.log_rows(bad_rows, "realtime_raw_bad_rows")

    processed_df = processor.process(df)

    processed_df, backup_errors, backup_bad_rows = backup_validator.validate(processed_df)
    logger.log_messages(backup_errors, "realtime_backup_errors")
    logger.log_rows(backup_bad_rows, "realtime_backup_bad_rows")

    filename = os.path.basename(file_path)
    output_filename = f"processed_{filename}"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

    local_writer.write_csv(processed_df, output_path)

    azure_writer.upload_file(
        output_path,
        f"realtime/{output_filename}"
    )

    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
    archived_path = os.path.join(ARCHIVE_FOLDER, filename)

    if os.path.exists(archived_path):
        os.remove(archived_path)

    os.replace(file_path, archived_path)

    print(f"Archived input file to {archived_path}")


def process_once():
    print("Checking realtime input folder once...")

    os.makedirs(WATCH_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

    files = os.listdir(WATCH_FOLDER)

    processed_count = 0

    for file in files:
        file_path = os.path.join(WATCH_FOLDER, file)

        if not os.path.isfile(file_path):
            continue

        if not file.endswith((".csv", ".xlsx")):
            continue

        try:
            process_file(file_path)
            processed_count += 1
        except Exception as e:
            print(f"Error processing {file}: {e}")

    print(f"Realtime one-time check finished. Files processed: {processed_count}")


def watch_folder():
    print("Watching folder for new files...")

    os.makedirs(WATCH_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

    processed_files = set()

    while True:
        files = os.listdir(WATCH_FOLDER)

        for file in files:
            file_path = os.path.join(WATCH_FOLDER, file)

            if not os.path.isfile(file_path):
                continue

            if file in processed_files:
                continue

            if not file.endswith((".csv", ".xlsx")):
                continue

            try:
                process_file(file_path)
                processed_files.add(file)
            except Exception as e:
                print(f"Error processing {file}: {e}")

        time.sleep(5)


if __name__ == "__main__":
    process_once()