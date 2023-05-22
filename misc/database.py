#!/usr/bin/env python3

import os
import psycopg2
import psycopg2.extras

POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')


# Used for worker pod
def update_status(status, file_id):
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()

        cursor.execute("UPDATE pieces SET status = %s WHERE piece_id = %s", (status, file_id))
        conn.commit()

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# User for watcher pod
def connect_db():
    conn = None
    try:
        print("Connecting to postgres db...")
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()
        psycopg2.extras.register_uuid()

        print('DB version:')
        cursor.execute('SELECT version()')

        db_v = cursor.fetchone()
        print(db_v)
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def add_tables():
    commands = (
        """
        CREATE TABLE master (
            master_id VARCHAR(128) PRIMARY KEY,
            pdf_name VARCHAR(255) NOT NULL,
            page_number INTEGER NOT NULL,
            status VARCHAR(21) NOT NULL
            )
        """,
        """
        CREATE TABLE pieces (
            piece_id VARCHAR(256) PRIMARY KEY,
            status VARCHAR(20) NOT NULL
            )
        """
    )
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()

        for c in commands:
            cursor.execute(c)

        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def add_master_job(file_id, name, page_number, status):
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()

        cursor.execute("INSERT INTO master (master_id, pdf_name, page_number, status) VALUES(%s, %s, %s, %s)", (file_id, name, page_number, status))
        conn.commit()

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def add_piece_job(piece_id, status):
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()

        cursor.execute("INSERT INTO pieces (piece_id, status) VALUES(%s, %s)", (piece_id, status))
        conn.commit()

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_piece_jobs(status):
    conn = None
    try:
        conn = psycopg2.connect(
            host="postgres.default.svc.cluster.local",
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD)
        cursor = conn.cursor()
        cursor.execute("SELECT piece_id FROM pieces WHERE status = %s", [status, ])
        pending_jobs = cursor.fetchall()

        if status == "pending":
            print("Pieces waiting to be processed:")
        else:
            print("Processing pieces:")

        jobs = []
        for item in pending_jobs:
            print(str(item[0]))
            jobs.append(str(item[0]))

        cursor.close()

        return jobs
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
