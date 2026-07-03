
import db_connections
import mysql.connector
import time 
from datetime import datetime
import sys

# ── Gateway Identity ──────────────────────────────────────────────────────────
gateway_id      = 2
gateway_code    = "GAT-02"

# ── Company / Deployment ─────────────────────────────────────────────────────
# COMPANY_CODE becomes the second segment in every MQTT topic:
#   enmms/{COMPANY_CODE}/{gateway_code}/sensor/{sensor_id}
COMPANY_CODE     = "siix"

# "cloud"  — gateway publishes directly to a cloud MQTT broker (Mode A)
# "local"  — gateway publishes to a local Mosquitto broker (Mode B/C)
DEPLOYMENT_MODE  = "local"

# ── MQTT Broker ───────────────────────────────────────────────────────────────
MQTT_HOST        = "localhost"   # broker hostname or IP
MQTT_PORT        = 1883
MQTT_USER        = "enmms"       # set to None to disable authentication
MQTT_PASSWORD    = "0smartpower0"            # fill with your broker password

# ── Polling interval ─────────────────────────────────────────────────────────
# Controls how often the gateway reads each meter.
# Change freely — the MQTT + DB Writer architecture handles any value.
#   300 = 5 minutes (current default)
#    60 = 1 minute
#    10 = 10 seconds
#     1 = 1 second
POLL_INTERVAL_SECONDS = 300

# ── Migration safety flag ─────────────────────────────────────────────────────
# True  → gateway still writes directly to cloud MySQL (Phase 2 safety net).
# False → cloud writes go exclusively through the DB Writer service (Phase 3+).
# Switch to False only after verifying the DB Writer is inserting correctly.
CLOUD_DIRECT_WRITE = True


def get_metter_ids(local_conn):
    meters_result   = []
    query           = local_conn.cursor(dictionary=True)

    sql = """SELECT sensors.id AS id, slave_address, sensor_reg_address,
                    sensor_type_parameter, sensor_models.id AS sensor_model_id
             FROM sensors
             LEFT JOIN sensor_models ON sensors.sensor_model_id = sensor_models.id
             LEFT JOIN sensor_types ON sensor_models.sensor_type_id = sensor_types.id
             WHERE sensors.gateway_id = %s"""
    query.execute(sql, (gateway_id,))

    results     = query.fetchall()
    query.close()

    for row in results:
        exploded_reg_address    = [int(value) for value in row['sensor_reg_address'].split(',')]
        exploded_parameter      = [str(value) for value in row['sensor_type_parameter'].split(',')]
        data      = {   'id':row['id'] , 
                        'sensor_model_id': row['sensor_model_id'],
                        'slave_address': row['slave_address'], 
                        'register_address': exploded_reg_address, 
                        'parameter': exploded_parameter
                    }
        meters_result.append(data)

    return meters_result

    
# THIS CODE UNDER IS MORE LIKELY THE IMPLODE IN PHP
# column_parameter = ", ".join(register_address["parameter"])
