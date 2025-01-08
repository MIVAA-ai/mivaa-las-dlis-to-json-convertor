# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
# from worker.tasks import convert_las_to_json_task
# from .crawlerconfig import CRAWLER_CONFIG
# from pathlib import Path
# import time
# import os
# import logging
#
#
# class NewFileHandler(FileSystemEventHandler):
#     """
#     Event handler for detecting new .las files and triggering Celery tasks.
#     """
#
#     def on_created(self, event):
#         """
#         Triggered when a new file is created in the monitored folder.
#         """
#         if event.is_directory or not event.src_path.endswith(".las"):
#             return
#
#         filepath = Path(event.src_path)
#         print(f"New file detected: {filepath}")
#
#         # Wait for the file to stabilize
#         if self._wait_for_file_complete(filepath):
#             print(f"File ready for processing: {filepath}")
#             result = convert_las_to_json_task.delay(str(filepath), str(CRAWLER_CONFIG["PROCESSED_FOLDER"]))
#             print(f"Task submitted for {filepath}, Task ID: {result.id}")
#         else:
#             print(f"File not ready: {filepath}")
#
#     def _wait_for_file_complete(self, filepath, stabilization_time=10, check_interval=5, abandonment_time=1800):
#         """
#         Wait indefinitely until the file size stabilizes and is not modified for a certain duration.
#         Detect abandoned copy operations after prolonged inactivity.
#
#         :param filepath: Path of the file to check
#         :param stabilization_time: Time (in seconds) with no modifications before considering the file ready
#         :param check_interval: Interval (in seconds) between file size checks
#         :param abandonment_time: Maximum time (in seconds) with no activity before considering the file abandoned
#         :return: True if the file is ready for processing, False if the copy operation is abandoned
#         """
#         print(f"Waiting for file to complete: {filepath}")
#         last_size = -1  # Track the last observed file size
#         last_activity_time = time.time()  # Track the last time the file size changed
#
#         while True:
#             try:
#                 # Ensure the file is accessible
#                 if not os.access(filepath, os.R_OK):
#                     print(f"File {filepath} is not accessible yet.")
#                     time.sleep(check_interval)
#                     continue
#
#                 # Get current file size and modification time
#                 current_size = os.path.getsize(filepath)
#                 current_modified_time = os.stat(filepath).st_mtime
#
#                 # Detect incremental size changes
#                 if last_size >= 0:
#                     increment = current_size - last_size
#                     if increment > 0:
#                         print(f"Copied: +{increment} bytes | Total: {current_size} bytes.")
#                         last_activity_time = time.time()  # Update activity timer
#                     else:
#                         # Check for abandonment if no size change
#                         if (time.time() - last_activity_time) > abandonment_time:
#                             print(f"File copy abandoned after {abandonment_time} seconds of inactivity: {filepath}")
#                             return False
#                 else:
#                     print(f"Current file size: {current_size} bytes.")
#
#                 # Check if the file has stabilized
#                 if current_size == last_size and (time.time() - current_modified_time) >= stabilization_time:
#                     print(f"File stabilized: {filepath} with size {current_size} bytes.")
#                     return True
#
#                 # Update last observed file size
#                 last_size = current_size
#
#             except (OSError, PermissionError) as e:
#                 print(f"Error accessing file {filepath}: {e}")
#
#             time.sleep(check_interval)
#
# def watch_folder():
#     """
#     Monitor the uploads folder for new files.
#     """
#     upload_folder = CRAWLER_CONFIG["UPLOAD_FOLDER"]
#     print(f"Watching folder: {upload_folder} for new .las files...")
#
#     event_handler = NewFileHandler()
#     observer = Observer()
#     observer.schedule(event_handler, path=str(upload_folder), recursive=False)
#
#     observer.start()
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()

from pathlib import Path
import time
import os
import logging
from worker.tasks import convert_las_to_json_task
from .crawlerconfig import CRAWLER_CONFIG

def poll_folder():
    """
    Poll the uploads folder for new .las files and trigger Celery tasks.
    """
    upload_folder = Path(CRAWLER_CONFIG["UPLOAD_FOLDER"])
    processed_folder = Path(CRAWLER_CONFIG["PROCESSED_FOLDER"])

    print(f"Polling folder: {upload_folder} for new .las files...")
    seen_files = set()

    while True:
        try:
            # Get all .las files in the folder
            current_files = {f for f in upload_folder.iterdir() if f.is_file() and f.suffix == ".las"}

            # Detect new files
            new_files = current_files - seen_files
            for file in new_files:
                print(f"New file detected: {file}")

                # Wait for the file to stabilize
                if _wait_for_file_complete(file):
                    print(f"File ready for processing: {file}")
                    result = convert_las_to_json_task.delay(str(file), str(processed_folder))
                    print(f"Task submitted for {file}, Task ID: {result.id}")
                else:
                    print(f"File not ready: {file}")

            # Update seen files
            seen_files = current_files

        except Exception as e:
            print(f"Error during polling: {e}")

        time.sleep(5)  # Poll every 5 seconds

def _wait_for_file_complete(filepath, stabilization_time=10, check_interval=5, abandonment_time=1800):
    """
    Wait until the file size stabilizes and is not modified for a certain duration.
    Detect abandoned copy operations after prolonged inactivity.

    :param filepath: Path of the file to check
    :param stabilization_time: Time (in seconds) with no modifications before considering the file ready
    :param check_interval: Interval (in seconds) between file size checks
    :param abandonment_time: Maximum time (in seconds) with no activity before considering the file abandoned
    :return: True if the file is ready for processing, False if the copy operation is abandoned
    """
    print(f"Waiting for file to complete: {filepath}")
    last_size = -1  # Track the last observed file size
    last_activity_time = time.time()  # Track the last time the file size changed

    while True:
        try:
            # Ensure the file is accessible
            if not os.access(filepath, os.R_OK):
                print(f"File {filepath} is not accessible yet.")
                time.sleep(check_interval)
                continue

            # Get current file size and modification time
            current_size = os.path.getsize(filepath)
            current_modified_time = os.stat(filepath).st_mtime

            # Detect incremental size changes
            if last_size >= 0:
                increment = current_size - last_size
                if increment > 0:
                    print(f"Copied: +{increment} bytes | Total: {current_size} bytes.")
                    last_activity_time = time.time()  # Update activity timer
                else:
                    # Check for abandonment if no size change
                    if (time.time() - last_activity_time) > abandonment_time:
                        print(f"File copy abandoned after {abandonment_time} seconds of inactivity: {filepath}")
                        return False
            else:
                print(f"Current file size: {current_size} bytes.")

            # Check if the file has stabilized
            if current_size == last_size and (time.time() - current_modified_time) >= stabilization_time:
                print(f"File stabilized: {filepath} with size {current_size} bytes.")
                return True

            # Update last observed file size
            last_size = current_size

        except (OSError, PermissionError) as e:
            print(f"Error accessing file {filepath}: {e}")

        time.sleep(check_interval)
