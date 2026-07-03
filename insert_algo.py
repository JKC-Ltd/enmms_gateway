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

        if cloud_conn:
            cloud_conn = db_connections.ensure_connected(cloud_conn)
            cloud_cursor = cloud_conn.cursor()
            cloud_cursor.execute(sql, values)
            cloud_conn.commit()
            if cloud_cursor.rowcount > 0:
                print("INSERTED TO CLOUD SUCCESSFULLY")
            else:
                print("FAILED TO INSERT INTO CLOUD")

        if not cloud_conn:
            # Cloud unavailable — queue for deferred sync
            local_conn = db_connections.ensure_connected(local_conn)
            local_cursor = local_conn.cursor()
            materialized_sql = sql % values
            offline_sql = "INSERT INTO sensor_offlines (query, gateway_id) VALUES (%s, %s)"
            local_cursor.execute(offline_sql, (materialized_sql, gateway_id))
            local_conn.commit()
        else:
            local_conn = db_connections.ensure_connected(local_conn)
            local_cursor = local_conn.cursor()
            local_cursor.execute(sql, values)
            local_conn.commit()
            if local_cursor.rowcount > 0:
                print("INSERTED TO LOCAL SUCCESSFULLY")
            else:
                print("FAILED TO INSERT INTO LOCAL")

    except mysql.connector.Error as error_message:
        print(f"Error: {error_message}")
        local_conn.rollback()
    finally:
        if cloud_cursor:
            cloud_cursor.close()
        if local_cursor:
            local_cursor.close()
