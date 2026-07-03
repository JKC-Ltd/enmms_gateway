import mysql.connector
import db_connections
import gateway_config
import time
from datetime import datetime
import sys

date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# DECLARING ID's
gateway_id = gateway_config.gateway_id
gateway_code = gateway_config.gateway_code


def insert_sensor_logs(meter_id, slave_address, column_parameter="", values="",
                       cloud_conn=None, local_conn=None):
    cloud_cursor = None
    local_cursor = None

    try:
        column_parameter = ", ".join([col.strip()
                                     for col in column_parameter.split(',')])
        placeholders = ", ".join(["%s"] * len(values))
        sql = f"INSERT INTO sensor_logs ({column_parameter}) VALUES ({placeholders})"

        # Try to ensure cloud connection if one was provided
        cloud_conn_checked = None
        if cloud_conn:
            cloud_conn_checked = db_connections.ensure_connected(cloud_conn)

        if cloud_conn_checked:
            cloud_cursor = cloud_conn_checked.cursor()
            try:
                cloud_cursor.execute(sql, values)
                cloud_conn_checked.commit()
                if cloud_cursor.rowcount > 0:
                    print("INSERTED TO CLOUD SUCCESSFULLY")
                else:
                    print("FAILED TO INSERT INTO CLOUD")
            except mysql.connector.Error as e:
                print(f"Cloud insert failed: {e}")
                # fall through to queueing below
                cloud_cursor.close()
                cloud_cursor = None

        # If cloud not available, queue the materialized SQL in sensor_offlines
        if not cloud_conn_checked:
            # Ensure local connection (try provided local_conn, else create a new one)
            local_conn_checked = None
            if local_conn:
                local_conn_checked = db_connections.ensure_connected(
                    local_conn)
            if not local_conn_checked:
                local_conn_checked = db_connections.local_database()

            if not local_conn_checked:
                print("Local DB unavailable — cannot queue offline row")
            else:
                local_cursor = local_conn_checked.cursor()
                # materialize the SQL for offline replay. Keep original behaviour
                try:
                    # Quote string values so the materialized SQL is valid for replay
                    safe_values = []
                    for v in values:
                        if isinstance(v, str):
                            safe_values.append(
                                "'" + v.replace("'", "''") + "'")
                        else:
                            safe_values.append(v)
                    materialized_sql = sql % tuple(safe_values)
                except Exception:
                    # Fallback: naive join with commas (best-effort)
                    materialized_sql = sql + ' -- materialization failed'

                offline_sql = "INSERT INTO sensor_offlines (query, gateway_id) VALUES (%s, %s)"
                local_cursor.execute(
                    offline_sql, (materialized_sql, gateway_id))
                local_conn_checked.commit()
                print("QUEUED ROW TO sensor_offlines")

        else:
            # Cloud succeeded; also write a local copy if possible
            local_conn_checked = None
            if local_conn:
                local_conn_checked = db_connections.ensure_connected(
                    local_conn)
            if not local_conn_checked:
                local_conn_checked = db_connections.local_database()

            if local_conn_checked:
                local_cursor = local_conn_checked.cursor()
                try:
                    local_cursor.execute(sql, values)
                    local_conn_checked.commit()
                    if local_cursor.rowcount > 0:
                        print("INSERTED TO LOCAL SUCCESSFULLY")
                    else:
                        print("FAILED TO INSERT INTO LOCAL")
                except mysql.connector.Error as e:
                    print(f"Local insert failed after cloud write: {e}")

    except mysql.connector.Error as error_message:
        print(f"Error: {error_message}")
        try:
            if local_conn:
                local_conn.rollback()
        except Exception:
            pass
    finally:
        if cloud_cursor:
            try:
                cloud_cursor.close()
            except Exception:
                pass
        if local_cursor:
            try:
                local_cursor.close()
            except Exception:
                pass
