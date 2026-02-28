from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterable, List, Sequence

import mysql.connector
from mysql.connector import MySQLConnection

from src.config import Settings
from src.models import CompanyRecord


@contextmanager
def db_connection(settings: Settings):
    connection = mysql.connector.connect(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_name,
    )
    try:
        yield connection
    finally:
        connection.close()


def ensure_schema_changes(connection: MySQLConnection, db_name: str) -> None:
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'masters'
        """,
        (db_name,),
    )
    existing = {row["COLUMN_NAME"] for row in cursor.fetchall()}
    required_sql = {
        "duplicate_status": "ADD COLUMN duplicate_status ENUM('primary','duplicate','probable') DEFAULT NULL",
        "duplicate_of": "ADD COLUMN duplicate_of INT NULL",
        "duplicate_score": "ADD COLUMN duplicate_score DECIMAL(5,3) NULL",
        "ai_decision": "ADD COLUMN ai_decision TEXT NULL",
        "record_type": "ADD COLUMN record_type ENUM('new','old') DEFAULT 'old'",
    }

    alter_parts = [sql for col, sql in required_sql.items() if col not in existing]
    if alter_parts:
        alter_stmt = f"ALTER TABLE {db_name}.masters\n" + ",\n".join(alter_parts)
        cursor.execute(alter_stmt)
        connection.commit()
    cursor.close()


def fetch_companies(connection: MySQLConnection) -> List[CompanyRecord]:
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, label, type
        FROM masters
        WHERE type = 'Company' AND label IS NOT NULL AND TRIM(label) <> ''
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return [CompanyRecord(id=row["id"], label=row["label"], type=row["type"]) for row in rows]


def fetch_company_columns(connection: MySQLConnection, db_name: str) -> List[str]:
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'masters'
        """,
        (db_name,),
    )
    cols = [r["COLUMN_NAME"] for r in cursor.fetchall()]
    cursor.close()
    return cols


def set_old_record_type(connection: MySQLConnection, ids: Sequence[int]) -> None:
    if not ids:
        return
    cursor = connection.cursor()
    placeholders = ",".join(["%s"] * len(ids))
    cursor.execute(
        f"UPDATE masters SET record_type='old' WHERE id IN ({placeholders})",
        tuple(ids),
    )
    cursor.close()


def update_master_row(
    connection: MySQLConnection,
    record_id: int,
    duplicate_status: str,
    duplicate_of: int | None,
    duplicate_score: float | None,
    ai_decision: str | None,
) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        UPDATE masters
        SET duplicate_status = %s,
            duplicate_of = %s,
            duplicate_score = %s,
            ai_decision = %s
        WHERE id = %s
        """,
        (duplicate_status, duplicate_of, duplicate_score, ai_decision, record_id),
    )
    cursor.close()


def insert_new_primary_record(
    connection: MySQLConnection,
    db_name: str,
    canonical_name: str,
) -> int | None:
    cols = fetch_company_columns(connection, db_name)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = 'masters'
          AND IS_NULLABLE = 'NO'
          AND COLUMN_DEFAULT IS NULL
          AND EXTRA NOT LIKE '%auto_increment%'
        """,
        (db_name,),
    )
    mandatory = {row["COLUMN_NAME"] for row in cursor.fetchall()}
    cursor.close()

    supported = set(cols)
    if "type" in mandatory and "label" in mandatory:
        pass
    elif mandatory - {"type", "label"}:
        return None

    insert_cols = [c for c in ["label", "type", "duplicate_status", "record_type"] if c in supported]
    values: List[object] = []
    for col in insert_cols:
        if col == "label":
            values.append(canonical_name)
        elif col == "type":
            values.append("Company")
        elif col == "duplicate_status":
            values.append("primary")
        elif col == "record_type":
            values.append("new")

    if not insert_cols:
        return None

    placeholders = ",".join(["%s"] * len(insert_cols))
    sql = f"INSERT INTO masters ({','.join(insert_cols)}) VALUES ({placeholders})"
    cursor = connection.cursor()
    cursor.execute(sql, tuple(values))
    new_id = cursor.lastrowid
    cursor.close()
    return int(new_id)


def create_run_audit_table(connection: MySQLConnection, db_name: str) -> None:
    cursor = connection.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.dedupe_run_audit (
            run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mode ENUM('dry_run','apply') NOT NULL,
            total_company_rows INT NOT NULL,
            duplicate_groups INT NOT NULL,
            duplicate_rows INT NOT NULL,
            probable_rows INT NOT NULL,
            notes TEXT NULL
        )
        """
    )
    connection.commit()
    cursor.close()


def insert_run_audit(
    connection: MySQLConnection,
    mode: str,
    total_company_rows: int,
    duplicate_groups: int,
    duplicate_rows: int,
    probable_rows: int,
    notes: str,
) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO dedupe_run_audit
            (mode, total_company_rows, duplicate_groups, duplicate_rows, probable_rows, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (mode, total_company_rows, duplicate_groups, duplicate_rows, probable_rows, notes),
    )
    cursor.close()
