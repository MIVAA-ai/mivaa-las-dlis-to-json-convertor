from pathlib import Path
import time
import os
from dlisio import dlis
from worker.tasks import convert_to_json_task
from .crawlerconfig import CRAWLER_CONFIG
from utils.IdentifyWellLogFormat import IdentifyWellLogFormat
from mappings.WellLogsFormat import WellLogFormat
import traceback
from datetime import datetime
from utils.logger import Logger

watcher_logger = Logger.get_logger("watcher.log")

def poll_folder():
    """
    Poll the uploads folder for new .las and .dlis files, dynamically detect file formats,
    and trigger the appropriate Celery tasks.
    """
    upload_folder = Path(CRAWLER_CONFIG["UPLOAD_FOLDER"])
    processed_folder = Path(CRAWLER_CONFIG["PROCESSED_FOLDER"])

    watcher_logger.info(f"Polling folder: {upload_folder} for new LAS and DLIS files...")
    seen_files = set()

    while True:
        try:
            # Get all files in the upload folder
            current_files = {f for f in upload_folder.iterdir() if f.is_file()}

            # Detect new files
            new_files = current_files - seen_files
            for file in new_files:
                #instantiating a logger for each file
                log_filename = f'{os.path.basename(str(file))}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
                file_logger = Logger.get_logger(log_filename)

                file_logger.info(f"New file detected: {file}")

                try:
                    # Wait for the file to stabilize
                    if not _wait_for_file_complete(filepath=file, log_filename=log_filename):
                        file_logger.info(f"File not ready: {file}")
                        continue

                    # Identify the file format
                    file_format = IdentifyWellLogFormat.GetFormat(file)

                    if file_format == WellLogFormat.LAS:
                        file_logger.info(f"Identified as LAS: {file}")

                        result = convert_to_json_task.delay(
                            filepath=str(file),
                            log_filename=log_filename,
                            output_folder=str(processed_folder),
                            file_format=WellLogFormat.LAS.value
                        )

                        file_logger.info(f"Task submitted for LAS file {file}, Task ID: {result}")

                    elif file_format == WellLogFormat.DLIS:
                        file_logger.info(f"Identified as DLIS: {file}. Extracting logical files for scanning")

                        logical_files = dlis.load(file)
                        file_logger.info(f"Loaded {len(logical_files)} logical files from DLIS {file}")

                        for logical_file in logical_files:

                            try:
                                logical_file_id = str(logical_file.fileheader.id)
                            except Exception as e:
                                file_logger.error(f"Error accessing logical file header in {file}: {e}")
                                continue  # Skip this logical file but continue processing others

                            result = convert_to_json_task.delay(
                                filepath=str(file),
                                log_filename=log_filename,
                                output_folder=str(processed_folder),
                                file_format=WellLogFormat.DLIS.value,
                                logical_file_id=logical_file_id
                            )
                            file_logger.info(
                                f"Task submitted for logical file {logical_file.fileheader.id} in DLIS file {file}, Task ID: {result}")
                    else:
                        file_logger.warning(f"Unknown format: {file}")

                except Exception as e:
                    file_logger.error(f"Error processing file {file}: {e}")
                    file_logger.debug(traceback.format_exc())

                finally:
                    # Always add the file to the seen_files set, even if an error occurs
                    seen_files.add(file)

            time.sleep(5)  # Poll every 5 seconds
        except Exception as e:
            watcher_logger.error(f"Critical error during polling: {e}")
            watcher_logger.debug(traceback.format_exc())

def _wait_for_file_complete(filepath, log_filename, stabilization_time=10, check_interval=5, abandonment_time=1800):
    """
    Wait until the file size stabilizes and is not modified for a certain duration.
    Detect abandoned copy operations after prolonged inactivity.

    :param filepath: Path of the file to check
    :param stabilization_time: Time (in seconds) with no modifications before considering the file ready
    :param check_interval: Interval (in seconds) between file size checks
    :param abandonment_time: Maximum time (in seconds) with no activity before considering the file abandoned
    :return: True if the file is ready for processing, False if the copy operation is abandoned
    """
    file_logger = Logger.get_logger(log_filename=log_filename)


    file_logger.info(f"Waiting for file to complete: {filepath}")
    last_size = -1  # Track the last observed file size
    last_activity_time = time.time()  # Track the last time the file size changed

    while True:
        try:
            # Ensure the file is accessible
            if not os.access(filepath, os.R_OK):
                file_logger.info(f"File {filepath} is not accessible yet.")
                time.sleep(check_interval)
                continue

            # Get current file size and modification time
            current_size = os.path.getsize(filepath)
            current_modified_time = os.stat(filepath).st_mtime

            # Detect incremental size changes
            if last_size >= 0:
                increment = current_size - last_size
                if increment > 0:
                    file_logger.info(f"Copied: +{increment} bytes | Total: {current_size} bytes.")
                    last_activity_time = time.time()  # Update activity timer
                else:
                    # Check for abandonment if no size change
                    if (time.time() - last_activity_time) > abandonment_time:
                        file_logger.error(f"File copy abandoned after {abandonment_time} seconds of inactivity: {filepath}")
                        return False
            else:
                file_logger.info(f"Current file size: {current_size} bytes.")

            # Check if the file has stabilized
            if current_size == last_size and (time.time() - current_modified_time) >= stabilization_time:
                file_logger.info(f"File stabilized: {filepath} with size {current_size} bytes.")
                return True

            # Update last observed file size
            last_size = current_size

        except (OSError, PermissionError) as e:
            file_logger.error(f"Error accessing file {filepath}: {e}")

        time.sleep(check_interval)