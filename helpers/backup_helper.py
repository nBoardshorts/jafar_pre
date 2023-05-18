# helpers/backup_helper.py

import os
import shutil
import datetime

def create_backup(source_file, backup_dir, backup_prefix):
    """
    Creates a backup file by copying the given source_file to the backup_dir, with a filename
    that includes the current timestamp and the backup_prefix.

    Args:
        source_file (str): The path to the source file to be backed up.
        backup_dir (str): The directory where the backup file should be saved.
        backup_prefix (str): A prefix for the backup file name.

    Returns:
        str: The path to the created backup file.
    """
    current_time = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{backup_prefix}_backup_{current_time}.sql"
    backup_path = os.path.join(backup_dir, backup_file)

    shutil.copy(source_file, backup_path)
    return backup_path

