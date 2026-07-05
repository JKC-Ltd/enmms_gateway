import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
import sys


datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

CLOUD_HOST = "srv2208.hstgr.io"
CLOUD_USER = "u565803524_dev_uratex"
CLOUD_PASSWORD = "m1&XJPSH"
CLOUD_DATABASE = "u565803524_dev_uratex"


def cloud_database():
    try:
        cloud_connection = mysql.connector.connect(
            host=CLOUD_HOST,
            user=CLOUD_USER,
            password=CLOUD_PASSWORD,
            database=CLOUD_DATABASE,
            connection_timeout=5,   # give up connecting after 5 seconds
            read_timeout=5,   # give up waiting for query response after 5 seconds
            write_timeout=5,   # give up waiting to send data after 5 seconds
        )
        if cloud_connection.is_connected():
            return cloud_connection
        else:
            return None

    except Error as cloud_error:
        print(f"Cloud database interrupt at {datetime_now}")
        print(f"Cloud Connection failed: {cloud_error}")
        return None


def local_database():
    try:
        local_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="0SmartPower0",
            database="uratex",
        )
        if local_connection.is_connected():
            return local_connection

    except Error as local_error:
        print(f"Local database interrupt at {datetime_now}")
        print(f"Local Connection failed: {local_error}")
        return None


def ensure_connected(conn):
    """
    Check if a connection is alive using ping.
    attempts=1, delay=0 means fail fast — no retrying.
    Returns None if connection is dead so the caller can handle it.
    """
    if conn is None:
        return None
    try:
        conn.ping(reconnect=False, attempts=1, delay=0)
        return conn
    except Error:
        print("Connection lost and could not reconnect.")
        return None


BATCH_SIZE = 500


def sync(gateway_id, from_conn, to_conn, fromCloudToLocal=True):
    """
    Replay queued offline rows from `from_conn` into `to_conn`.

    Processes up to BATCH_SIZE rows per call so the gateway is never
    blocked replaying a large backlog before reading meters.  Rows are
    executed in a single transaction and deleted in one bulk DELETE on
    success, falling back to row-by-row on a bulk failure so a single
    bad query doesn't block the rest.
    """
    if from_conn is None or to_conn is None:
        return

    from_conn = ensure_connected(from_conn)
    if from_conn is None:
        return

    try:
        from_cursor = from_conn.cursor(dictionary=True)
        from_sql = ("SELECT * FROM sensor_offlines "
                    "WHERE gateway_id = %s ORDER BY id LIMIT %s")
        from_cursor.execute(from_sql, (gateway_id, BATCH_SIZE))
        from_result = from_cursor.fetchall()
        from_cursor.close()
    except Error as e:
        print(f"sync() failed to fetch offline rows: {e}")
        return

    if not from_result:
        return

    to_conn = ensure_connected(to_conn)
    if to_conn is None:
        print("sync() skipped — destination connection unavailable.")
        return

    print(
        f"Syncing {len(from_result)} offline rows (batch size: {BATCH_SIZE})...")

    to_cursor = to_conn.cursor()

    succeeded_ids = []
    failed_ids = []

    try:
        # Execute all rows in one transaction
        for row in from_result:
            try:
                to_cursor.execute(row["query"])
                succeeded_ids.append(row["id"])
            except Error as row_error:
                print(f"Row {row['id']} INVALID — skipping: {row_error}")
                failed_ids.append(row["id"])

        to_conn.commit()
        print(
            f"Batch committed: {len(succeeded_ids)} succeeded, {len(failed_ids)} failed.")

    except Error as batch_error:
        print(f"Batch commit failed: {batch_error}")
        try:
            to_conn.rollback()
        except Exception:
            pass
        return

    finally:
        to_cursor.close()

    # Bulk-delete all successfully synced rows in one query
    if succeeded_ids:
        from_conn = ensure_connected(from_conn)
        if from_conn:
            try:
                del_cursor = from_conn.cursor()
                placeholders = ", ".join(["%s"] * len(succeeded_ids))
                del_cursor.execute(
                    f"DELETE FROM `sensor_offlines` WHERE id IN ({placeholders})",
                    succeeded_ids
                )
                from_conn.commit()
                del_cursor.close()
                print(
                    f"Cleared {len(succeeded_ids)} synced rows from offline queue.")
            except Error as del_error:
                print(f"Failed to delete synced rows: {del_error}")

    if failed_ids:
        print(
            f"{len(failed_ids)} rows left in offline queue (invalid queries): {failed_ids}")


# insert into `sensor_registers`
#     (`id`,`sensor_type_id`, `sensor_model_id`, `sensor_reg_address`, `updated_at`, `created_at`)
# values (1, 1, 1, '0, 6, 12, 18, 342', '2025-02-16 16:15:35', '2025-02-16 16:15:35')
# ON DUPLICATE KEY UPDATE
# `sensor_type_id` = VALUES(`sensor_type_id`),
# `sensor_model_id` = VALUES(`sensor_model_id`),
# `sensor_reg_address` = VALUES(`sensor_reg_address`),
# `updated_at` = VALUES(`updated_at`),
# `created_at` = VALUES(`created_at`)
