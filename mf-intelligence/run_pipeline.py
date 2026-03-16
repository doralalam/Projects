import subprocess
import logging
import time
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")
LOG_DIR = BASE_DIR / "logs"
PIPELINE_LOG = LOG_DIR / "pipeline.log"

LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(PIPELINE_LOG),
        logging.StreamHandler()
    ]
)

STANDARDIZERS = [
    "standardizers/edelweiss_standardizer.py",
    "standardizers/hdfc_standardizer.py",
    "standardizers/icici_standardizer.py",
    "standardizers/invesco_standardizer.py",
    "standardizers/motilal_standardizer.py",
    "standardizers/nippon_standardizer.py",
    "standardizers/quant_standardizer.py",
    "standardizers/sbi_standardizer.py"
]

BUILDERS = [
    "builders/all_amc_builder.py",
    "builders/all_amc_clean_builder.py"
]

UPLOAD_SCRIPT = [
    "utils/upload_parquet_to_pg.py"
]


def run_script(script_path):

    full_path = BASE_DIR / script_path

    start = time.time()

    logging.info(f"Starting -> {script_path}")

    result = subprocess.run(
        ["python", str(full_path)],
        cwd=BASE_DIR
    )

    runtime = round(time.time() - start, 2)

    if result.returncode != 0:
        logging.error(f"FAILED -> {script_path} | Runtime: {runtime}s")
        raise RuntimeError(f"Pipeline stopped due to failure in {script_path}")

    logging.info(f"Completed -> {script_path} | Runtime: {runtime}s")


def run_stage(stage_name, scripts):

    logging.info(f"\n===== {stage_name} START =====")

    for script in scripts:
        run_script(script)

    logging.info(f"===== {stage_name} COMPLETE =====\n")


def main():

    pipeline_start = time.time()

    logging.info("========== PIPELINE STARTED ==========")

    run_stage("STANDARDIZERS", STANDARDIZERS)

    run_stage("BUILDERS", BUILDERS)

    run_stage("POSTGRESQL UPLOAD", UPLOAD_SCRIPT)

    total_runtime = round(time.time() - pipeline_start, 2)

    logging.info(f"========== PIPELINE FINISHED ==========")
    logging.info(f"Total Runtime: {total_runtime} seconds")


if __name__ == "__main__":
    main()