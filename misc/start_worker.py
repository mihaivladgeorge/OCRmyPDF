#!/usr/bin/env python3

import ocrmypdf
import sys
from database import *


PIECES_DIRECTORY = os.getenv('OCR_PIECES_DIRECTORY', '/pieces')
PIECES_OUT_DIRECTORY = os.getenv('OCR_PIECES_OUT_DIRECTORY', "/pieces_out")
OUTPUT_DIRECTORY = os.getenv('OCR_OUTPUT_DIRECTORY', '/output')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
file_id = sys.argv[1]


def execute_ocrmypdf():
    file_path = PIECES_DIRECTORY
    output_path = PIECES_OUT_DIRECTORY

    print(f'Attempting to OCRmyPDF to: {output_path}')
    exit_code = ocrmypdf.ocr(
        input_file=os.path.join(file_path, sys.argv[1]) + ".pdf",
        output_file=os.path.join(output_path, sys.argv[1]) + ".pdf",
    )
    if exit_code == 0:
        print('OCR is done')
        update_status("done", file_id)

    else:
        print('OCR failed')
        update_status("failed", file_id)


def main():
    execute_ocrmypdf()


if __name__ == "__main__":
    main()
