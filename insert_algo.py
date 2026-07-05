import mysql.connector
import db_connections
import gateway_config
import time 
from datetime import datetime
import sys

date_now   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# DECLARING ID's
gateway_id   = gateway_config.gateway_id
gateway_code = gateway_config.gateway_code

def insert_sensor_logs(meter_id, slave_address, column_parameter="", values="",
                       cloud_conn=None, local_conn=None):
    """
    Insert a sensor reading into cloud and local databases.

    Returns True if cloud insert succeeded, False if it failed or was unavailable.
    The caller (index.py) uses this return value to reset cloud_conn to None
    so the next cycle attempts to reconnect via cloud_database() with proper timeouts.
    """
    cloud_cursor = None
    local_cursor = None

    column_parameter = ", ".join([col.strip() for col in column_parameter.split(',')])
    placeholders     = ", ".join(["%s"] * len(values))
    sql              = f"INSERT INTO sensor_logs ({column_parameter}) VALUES ({placeholders})"

    # --- Cloud insert ---
    cloud_ok = False
    if cloud_conn:
        # Check connection is alive — returns None if dead (internet dropped)
        live_cloud = db_connections.ensure_connected(cloud_conn)
        if live_cloud is None:
            print("Cloud connection lost — will queue offline and retry next cycle.")
        else:
            try:
                cloud_cursor = live_cloud.cursor()
                cloud_cursor.execute(sql, values)
                live_cloud.commit()
                if cloud_cursor.rowcount > 0:
                    print("INSERTED TO CLOUD SUCCESSFULLY")
                    cloud_ok = True
                else:
                    print("FAILED TO INSERT INTO CLOUD")
            except mysql.connector.Error as cloud_error:
                print(f"Cloud insert failed: {cloud_error}")
                try:
                    live_cloud.rollback()
                except Exception:
                    pass
            finally:
                if cloud_cursor:
                    cloud_cursor.close()
                    cloud_cursor = None

    # --- Local insert or offline queue ---
    try:
        db_connections.ensure_connected(local_conn)
        local_cursor = local_conn.cursor()

        if not cloud_ok:
            # Cloud unavailable or failed — queue for deferred sync
            materialized_sql = sql % tuple(
                f"'{v}'" if isinstance(v, str) else str(v) for v in values
            )
            offline_sql = "INSERT INTO sensor_offlines (query, gateway_id) VALUES (%s, %s)"
            local_cursor.execute(offline_sql, (materialized_sql, gateway_id))
            local_conn.commit()
            print("Cloud unavailable — queued to sensor_offlines")
        else:
            local_cursor.execute(sql, values)
            local_conn.commit()
            if local_cursor.rowcount > 0:
                print("INSERTED TO LOCAL SUCCESSFULLY")
            else:
                print("FAILED TO INSERT INTO LOCAL")

    except mysql.connector.Error as local_error:
        print(f"Local insert failed: {local_error}")
        try:
            local_conn.rollback()
        except Exception:
            pass
    finally:
        if local_cursor:
            local_cursor.close()

    return cloud_ok
