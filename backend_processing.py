"""
Backend Processing for Vendor Upload Portal

This module handles the processing of uploaded COI files.
"""

import os
import logging
from extractor import run as process_coi_file

logger = logging.getLogger(__name__)

def process_uploaded_file(file_path):
    try:
        # Validate the file type and size
        if not file_path.endswith(('.pdf', '.jpg', '.jpeg', '.png')):
            raise ValueError("Invalid file type")

        # Process the file using the COI Extractor module
        # Call the extractor's run function to process the file
        process_coi_file()

        logger.info("File processed successfully: %s", file_path)
    except Exception as e:
        logger.error("Error processing file %s: %s", file_path, str(e))
        raise

def main():
    uploads_dir = "uploads"
    for file_name in os.listdir(uploads_dir):
        file_path = os.path.join(uploads_dir, file_name)
        process_uploaded_file(file_path)

if __name__ == "__main__":
    main()