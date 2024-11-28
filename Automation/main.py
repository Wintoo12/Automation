import os
import re
import time
import subprocess
import random
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler

# Initialize logger at the module level
logger = logging.getLogger('script_runner')


def setup_logging():
    """
    Configure logging with both file and console output

    Returns:
        logging.Logger: Configured logger
    """
    # Ensure the logger is not re-adding handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # File Handler with rotation
    file_handler = RotatingFileHandler(
        'script_runner.log',
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s: %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def parse_repetitions(script_path):
    """
    Extract number of repetitions from the script filename

    Args:
        script_path (str): Path to the script

    Returns:
        int: Number of times to repeat the script (default 1 if not found)
    """
    # Extract filename from path
    filename = os.path.basename(script_path)

    # Use regex to find a number at the end of the filename (before .py)
    match = re.search(r'-(\d+)\.py$', filename)

    if match:
        repetitions = int(match.group(1))
        logger.info(f"Script {filename} will be repeated {repetitions} times")
        return repetitions

    logger.warning(f"No repetition number found for {filename}. Defaulting to 1.")
    return 1


def validate_script(script_path):
    """
    Validate script path exists and is readable

    Args:
        script_path (str): Path to the script

    Returns:
        bool: True if script is valid, False otherwise
    """
    if not os.path.exists(script_path):
        logger.error(f"Script does not exist: {script_path}")
        return False

    if not os.path.isfile(script_path):
        logger.error(f"Not a file: {script_path}")
        return False

    if not os.access(script_path, os.R_OK):
        logger.error(f"Script is not readable: {script_path}")
        return False

    return True


def run_script(script_path):
    """
    Run a Python script multiple times based on filename

    Args:
        script_path (str): Path to the script to run

    Returns:
        bool: True if all runs succeeded, False if any run failed
    """
    # Validate script first
    if not validate_script(script_path):
        return False

    # Parse number of repetitions
    max_repetitions = parse_repetitions(script_path)

    for attempt in range(max_repetitions):
        delay = get_random_delay()
        logger.info(f"Attempt {attempt + 1}/{max_repetitions} for {script_path}: Delay {delay:.2f}s")

        time.sleep(delay)

        try:
            result = subprocess.run(
                ["python", script_path],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Successfully completed {script_path} (Attempt {attempt + 1})")

        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing {script_path} (Attempt {attempt + 1}/{max_repetitions}):")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")

            # If any attempt fails, return False
            return False

    # If all attempts succeed
    return True


def get_random_delay(min_delay=3, max_delay=10):
    """
    Generate a random delay with configurable range

    Args:
        min_delay (float): Minimum delay in seconds
        max_delay (float): Maximum delay in seconds

    Returns:
        float: Random delay between min and max
    """
    return random.uniform(min_delay, max_delay)


def main():
    # Setup logging
    setup_logging()

    # Define scripts to run
    scripts = [
        "Automation-Survey/Automation/BSME-2-M-105.py",
        "Automation-Survey/Automation/BSA-3-M-20.py",
        "Automation-Survey/Automation/BSED-2-F-20.py",
        "Automation-Survey/Automation/BSED-3-M-5.py",
        "Automation-Survey/Automation/BSED-3-F-40.py"
    ]

    # Use a pool of workers to execute scripts in parallel
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Dictionary to track script execution status
        script_futures = {
            executor.submit(run_script, script): script
            for script in scripts
        }

        # Track successful and failed scripts
        successful_scripts = []
        failed_scripts = []

        # Process completed futures
        for future in as_completed(script_futures):
            script = script_futures[future]
            try:
                result = future.result()
                if result:
                    successful_scripts.append(script)
                else:
                    failed_scripts.append(script)
            except Exception as e:
                logger.error(f"Unexpected error with {script}: {e}")
                failed_scripts.append(script)

        # Log summary
        logger.info("Execution Summary:")
        logger.info(f"Successful Scripts: {successful_scripts}")
        logger.info(f"Failed Scripts: {failed_scripts}")


if __name__ == "__main__":
    main()