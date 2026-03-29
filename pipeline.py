import argparse
import logging
import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src import data_cleaning
from src import rfm
from src import cohort
from src import market_basket
from src import clv

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


STEPS = {
    "clean":   ("Data Cleaning",        data_cleaning.run),
    "rfm":     ("RFM Segmentation",     rfm.run),
    "cohort":  ("Cohort Analysis",      cohort.run),
    "basket":  ("Market Basket Mining", market_basket.run),
    "clv":     ("CLV Prediction",       clv.run),
}


def run_step(name: str) -> bool:
    label, fn = STEPS[name]
    logger.info(f"Starting: {label}")
    try:
        fn()
        logger.info(f" Completed: {label}")
        return True
    except Exception as e:
        logger.error(f" FAILED: {label} — {e}", exc_info=True)
        return False


def run_pipeline(step: str = None):
    start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Consumer360 Pipeline — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    steps_to_run = [step] if step else list(STEPS.keys())
    results = {}

    for s in steps_to_run:
        if s not in STEPS:
            logger.warning(f"Unknown step '{s}'. Valid steps: {list(STEPS.keys())}")
            continue
        results[s] = run_step(s)

    elapsed = (datetime.now() - start).seconds
    passed = sum(results.values())
    failed = len(results) - passed

    logger.info("=" * 60)
    logger.info(f"Pipeline complete in {elapsed}s — {passed} passed, {failed} failed")
    logger.info("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consumer360 weekly pipeline")
    parser.add_argument("--step", choices=list(STEPS.keys()), help="Run a single step only")
    args = parser.parse_args()
    run_pipeline(step=args.step)
