#!/usr/bin/env python3

from __future__ import annotations

import time
from PyPDF2 import PdfReader, PdfWriter, PdfMerger
import math
import uuid
from database import *
from argo import *


INPUT_DIRECTORY = os.getenv('OCR_INPUT_DIRECTORY', '/input')
PIECES_DIRECTORY = os.getenv('OCR_PIECES_DIRECTORY', '/pieces')
PIECES_OUT_DIRECTORY = os.getenv('OCR_PIECES_OUT_DIRECTORY', "/pieces_out")
OUTPUT_DIRECTORY = os.getenv('OCR_OUTPUT_DIRECTORY', '/output')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
MAX_CPU = int(os.getenv('MAX_CPU'))
ARGO_URL = "http://argo-workflows-server.default.svc.cluster.local:2746/api/v1/workflows/?listOptions.labelSelector=workflows.argoproj.io/phase%20in%20(Running)"
ARGO_SUBMIT_URL = "http://argo-workflows-server.default.svc.cluster.local:2746/api/v1/workflows/default/submit"


def get_and_split_new_pdfs():
    file_path = INPUT_DIRECTORY
    pieces_path = PIECES_DIRECTORY
    total_pages = 0
    total_pdfs = 0
    print("Checking for new pdfs...")
    for file in os.listdir(file_path):
        if file.endswith(".pdf") or file.endswith(".PDF"):
            print(f"New pdf file found: {file}")
            pdf = PdfReader(open(os.path.join(file_path, file), "rb"))
            total_pages += len(pdf.pages)
            total_pdfs += 1
    split_pages = math.ceil(total_pages/MAX_CPU)
    if total_pages > 0:
        print(f"Number of pdfs to be processed now is {total_pdfs}")
        print(f"Total current page number is {total_pages}, splitting pdfs in pieces of {split_pages}...")
    else:
        print("No new pdfs found...")
    for file in os.listdir(file_path):
        if file.endswith(".pdf") or file.endswith(".PDF"):
            print(f"Splitting pdf {file}...")
            file_id = uuid.uuid4()
            pdf = PdfReader(open(os.path.join(file_path, file), "rb"))
            add_master_job(file_id, file, math.ceil(len(pdf.pages)/split_pages), "pending")
            for piece in range(math.ceil(len(pdf.pages)/split_pages)):
                writer = PdfWriter()
                for page in range(split_pages):
                    if piece * split_pages + page <= len(pdf.pages) - 1:
                        writer.add_page(pdf.pages[piece * split_pages + page])
                piece_path = os.path.join(pieces_path, str(file_id)) + str(piece) + ".pdf"
                add_piece_job(str(file_id) + str(piece), "pending")
                with open(piece_path, "wb") as stream:
                    writer.write(stream)
            print("Splitting done!")
    if total_pages > 0:
        print("Removing input files...")
        for file in os.listdir(file_path):
            os.remove(os.path.join(file_path, file))


def rebuild_pdf(file_id, file_name):
    input_path = PIECES_OUT_DIRECTORY
    output_path = OUTPUT_DIRECTORY
    merge = PdfMerger()

    print("Merging files...")
    pieces = [pdf for pdf in os.listdir(input_path) if pdf.startswith(file_id)]
    pieces = sorted(pieces, key=lambda x: int(x[-5]))

    for file in pieces:
        merge.append(os.path.join(input_path, file))
    print("Should be merged...Writing to disk...")
    merge.write(os.path.join(output_path, file_name))
    merge.close()


def process_jobs():
    pending_jobs = get_piece_jobs("pending")
    for job in pending_jobs:
        wf_count = get_argo_workflow_count()
        if wf_count is None or wf_count < MAX_CPU:
            print(f"Starting workflow for job {job}")
            start_workflow(job)
            update_status("processing", job)
        else:
            print("The load is maximum, not processing any more pdfs at the moment...")


def check_master_jobs(status):
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM master WHERE status = %s", [status, ])
        pending_jobs = cursor.fetchall()

        for item in pending_jobs:
            cursor.execute("SELECT COUNT(*) FROM pieces WHERE piece_id LIKE %s AND status = 'done'", [str(item[0]) + "%"])
            finished_pieces = cursor.fetchone()
            finished_pieces = int(finished_pieces[0])
            print(finished_pieces)
            expected_pieces = int(item[2])
            if finished_pieces == expected_pieces:
                print(f"OCR done for file {str(item[1])}... Rebuilding pdf...")
                cursor.execute("UPDATE master SET status = 'done' WHERE master_id = %s", [str(item[0])])
                conn.commit()
                rebuild_pdf(str(item[0]), str(item[1]))
            else:
                print(f"Still waiting for workers for file {str(item[1])}, only {str(finished_pieces)}/{str(expected_pieces)} jobs finished...")
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def routine():
    # Check for new pdfs, split them and send job info to db
    get_and_split_new_pdfs()
    # Check db for pending jobs, send to argo workflows
    process_jobs()
    # Check master job table, if pending check job pieces -> rebuild pdf and mark as done
    check_master_jobs("pending")


def main():
    print(
        f"Starting OCRmyPDF watcher with config:\n"
        f"Maximum number of vCPUs used: {MAX_CPU}\n"
        f"Input Directory: {INPUT_DIRECTORY}\n"
        f"Pieces Directory: {PIECES_DIRECTORY}\n"
        f"Output Directory: {OUTPUT_DIRECTORY}\n"
    )
    connect_db()
    add_tables()
    while True:
        t0 = time.monotonic()
        print("Starting loop...")
        routine()
        print(f"Sleeping for {7.0 - (time.monotonic() - t0)} seconds ...")
        time.sleep(7.0 - (time.monotonic() - t0))


if __name__ == "__main__":
    main()
