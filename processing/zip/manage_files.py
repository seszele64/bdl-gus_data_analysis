import sys  # for printing progress
import zipfile
import os
from paths import paths

# --------------------------------- zip files -------------------------------- #


class ZipFileManager:
    def __init__(self, zip_path, output_path = None):
        self.zip_path = zip_path
        self.output_path = output_path
    
    def set_output_path(self, output_path):
        self.output_path = output_path

    # unpack selected zip file to selected folder

    def extract_with_progress(self):
        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            total_size = sum(file_info.file_size for file_info in zip_ref.infolist())
            extracted_size = 0

            for file_info in zip_ref.infolist():
                extracted_size += file_info.file_size
                extracted_percent = (extracted_size / total_size) * 100
                sys.stdout.write('\rExtracting: {:.2f}%'.format(extracted_percent))
                sys.stdout.flush()

                zip_ref.extract(file_info, self.output_path)

            sys.stdout.write('\rExtraction Complete!     \n')
            sys.stdout.flush()

# ------------------------------------- < ------------------------------------ #