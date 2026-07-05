
import db_connections
import mysql.connector
import time 
from datetime import datetime
import sys

# DECLARING ID's
gateway_id      = 1
gateway_code    = "GAT-01"


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
