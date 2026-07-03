import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
import sys


datetime_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def cloud_database():
    try:
        cloud_connection = mysql.connector.connect(
            host="srv2208.hstgr.io",
            # user="u565803524_siix",
            # password="|C9leEeiFQ4",
            # database="u565803524_siix"
            user="u565803524_gateway_test",
            password="GatewayTest0123",
            database="u565803524_gateway_test"
        )
        if cloud_connection.is_connected():
            return cloud_connection
        else:
            return False

    except Error as cloud_error:
        print(f"Cloud database interupt at {datetime_now}")
        print(f"Cloud Connection failed: {cloud_error}")
        return False


def local_database():
    try:
        local_database = mysql.connector.connect(
            host="localhost",
            user="root",
            password="0SmartPower0",
            database="enmms"
        )
        if local_database.is_connected():
            return local_database

    except Error as local_error:
        print(f"Local database interupt at {datetime_now}")
        print(f"Local Connection failed: {local_error}")
        return False


def ensure_connected(conn):
    if conn is None:
        return None

    try:
        conn.ping(reconnect=True, attempts=3, delay=2)
        return conn
    except Exception as e:
        print(f"Database unavailable: {e}")
        try:
            conn.close()
        except Exception:
            pass

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
    ensure_connected(from_conn)
    from_cursor = from_conn.cursor(dictionary=True)
    from_sql = ("SELECT * FROM sensor_offlines "
                "WHERE gateway_id = %s ORDER BY id LIMIT %s")
    from_cursor.execute(from_sql, (gateway_id, BATCH_SIZE))
    from_result = from_cursor.fetchall()
    from_cursor.close()

    if not from_result:
        return

    print(
        f"Syncing {len(from_result)} offline rows (batch size: {BATCH_SIZE})...")

    ensure_connected(to_conn)
    to_cursor = to_conn.cursor()

    succeeded_ids = []
    failed_ids = []

    try:
        # Execute all rows in one transaction
        for row in from_result:
            try:
                to_cursor.execute(row["query"])
                succeeded_ids.append(row["id"])
            except mysql.connector.Error as row_error:
                print(f"Row {row['id']} INVALID — skipping: {row_error}")
                failed_ids.append(row["id"])

        to_conn.commit()
        print(
            f"Batch committed: {len(succeeded_ids)} succeeded, {len(failed_ids)} failed.")

    except mysql.connector.Error as batch_error:
        print(f"Batch commit failed: {batch_error}")
        to_conn.rollback()
        # Nothing was committed — don't delete anything
        to_cursor.close()
        return

    finally:
        to_cursor.close()

    # Bulk-delete all successfully synced rows in one query
    if succeeded_ids:
        ensure_connected(from_conn)
        del_cursor = from_conn.cursor()
        placeholders = ", ".join(["%s"] * len(succeeded_ids))
        del_cursor.execute(
            f"DELETE FROM `sensor_offlines` WHERE id IN ({placeholders})",
            succeeded_ids
        )
        from_conn.commit()
        del_cursor.close()
        print(f"Cleared {len(succeeded_ids)} synced rows from offline queue.")

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


def check_connection(conn):
    returnData = False
    print("Checking Cloud Connection...")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sensor_logs LIMIT 1")
        cursor.fetchone()
        cursor.close()
        print("Cloud Active.ss")
        returnData = True
        return returnData

    except Exception:
        print("Cloud Inactive.sss")
        return returnData
