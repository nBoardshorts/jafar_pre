# helpers/logging_helper.py

import os
import datetime
import inspect
from loguru import logger

from gitignore.config import LOG_DIR

# Purpose:
# 1. Configure and set up logging for individual scripts
#
# Criteria:
# 1. Automatically create log directories if they don't exist
# 2. Log messages in a separate file for each script and date
# 3. Rotate log files when they reach 50 MB in size
# 4. Provide a function to log exceptions with traceback information

"""
USAGE:
1. To set up logging in a script, follow these instructions:

    Import the configure_logging function from logging_helper.py:

    from logging_helper import configure_logging

2. Call the configure_logging() function at the beginning of the script:

    configure_logging()

3. Use the logger from Loguru for logging messages:

    from loguru import logger

    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")

4. To log exceptions with traceback information, import the log_exception function from logging_helper.py and use it in an except block:

    from logging_helper import log_exception

    try:
        # Some code that raises an exception
    except Exception as e:
        log_exception(e)
    That's it! Now you have logging set up for your script.

"""

logger = logger

def configure_logging():
    """
    Configures logging for the calling script.

    Usage:
    Call this function at the beginning of any script where logging is needed. Log messages
    will be saved to a file in the LOG_DIR/database directory with the format {date}_{script_name}.log.
    Log files will be rotated when they reach 50 MB in size.
    """
    log_subdir = "database"
    log_dir = os.path.join(LOG_DIR, log_subdir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    calling_script = inspect.stack()[1].filename
    script_name = os.path.basename(calling_script).split('.')[0]

    log_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    log_file_path = os.path.join(log_dir, f"{log_date}_{script_name}.log")

    logger.add(log_file_path, level="INFO", format="{time} - {level} - {message}", rotation="50 MB")
    logger.info("Logging configured.")

def log_exception(exception: Exception):
    """
    Logs an exception with traceback information.

    Usage:
    Call this function in an except block to log the exception and its traceback.

    Example:
    try:
        # Some code that raises an exception
    except Exception as e:
        log_exception(e)
    """
    logger.exception(exception)
