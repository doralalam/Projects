import os
import shutil
import logging

RAW_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/icici_prudential"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/icici"
LOG_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/logs/separator_logs"

files_copied = 0
files_skipped = 0
error_count = 0


def setup_logging():

    os.makedirs(LOG_PATH, exist_ok=True)

    log_file = os.path.join(LOG_PATH, "icici_separator.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def sync_files():

    global files_copied, files_skipped, error_count

    for root, _, files in os.walk(RAW_PATH):

        relative_path = os.path.relpath(root, RAW_PATH)
        target_dir = os.path.join(OUTPUT_PATH, relative_path)

        os.makedirs(target_dir, exist_ok=True)

        for file in files:

            if not file.endswith((".xls", ".xlsx")):
                continue

            source_file = os.path.join(root, file)
            target_file = os.path.join(target_dir, file)

            try:

                if os.path.exists(target_file):

                    files_skipped += 1
                    logging.info(f"Skipped (exists) -> {target_file}")
                    continue

                shutil.copy2(source_file, target_file)

                files_copied += 1

                logging.info(f"Copied -> {target_file}")

            except Exception as e:

                logging.error(f"Failed -> {source_file}")
                logging.error(str(e))

                error_count += 1


def main():

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    setup_logging()

    logging.info("Starting ICICI sync")

    sync_files()

    print("\nExecution Summary")
    print("------------------")
    print(f"Files copied: {files_copied}")
    print(f"Files skipped: {files_skipped}")
    print(f"Errors: {error_count}")

    if error_count > 0:

        logging.warning("Task partially completed. Please resolve errors.")

        print("\nTask partially completed. Check logs.")

    else:

        logging.info("Task completed successfully.")

        print("\nTask completed successfully.")


if __name__ == "__main__":
    main()