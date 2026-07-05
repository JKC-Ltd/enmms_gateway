from pymodbus.client import ModbusSerialClient
import db_connections
import gateway_config
import insert_algo
import time
from datetime import datetime
import sys

gateway_id   = gateway_config.gateway_id
gateway_code = gateway_config.gateway_code

# DECLARING MODBUS CLIENT
client = ModbusSerialClient(
    port='/dev/ttyUSB0',
    baudrate=9600,
    stopbits=1,
    parity="N",
    bytesize=8,
    timeout=2
)

# Open connections once at startup — reused for the entire lifetime of the process
cloud_conn = db_connections.cloud_database()
if not cloud_conn:
    print("Cloud database unreachable at startup. Running in offline mode.")

local_conn = db_connections.local_database()
if not local_conn:
    print("Local database unreachable. Cannot continue.")
    sys.exit(1)

try:
    while True:
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Retry cloud connection if it was never established or dropped
        if not cloud_conn:
            print(f"[{date_now}] Attempting to reconnect to cloud database...")
            cloud_conn = db_connections.cloud_database()
            if cloud_conn:
                print(f"[{date_now}] Cloud connection re-established.")
            else:
                print(f"[{date_now}] Cloud still unreachable. Running in offline mode.")

        try:
            # Sync offline queue before polling meters
            # Pass cloud_conn by reference — sync() handles None gracefully
            db_connections.sync(gateway_id, from_conn=cloud_conn, to_conn=local_conn, fromCloudToLocal=True)
            db_connections.sync(gateway_id, from_conn=local_conn, to_conn=cloud_conn, fromCloudToLocal=False)

            # Fetch meter configuration (uses the already-open local connection)
            meter_results = gateway_config.get_metter_ids(local_conn)

            for meter_result in meter_results:
                model_id           = meter_result['sensor_model_id']
                meter_id           = meter_result['id']
                slave_address      = int(meter_result['slave_address'])
                columns            = ["gateway_id", "sensor_id"] + meter_result['parameter'] + ['datetime_created']
                register_addresses = meter_result['register_address']
                column_parameter   = ', '.join(columns)
                meter_value_temp   = ()

                # Connect once per meter (not once per register)
                if client.connect():
                    try:
                        for register_address in register_addresses:
                            response = client.read_holding_registers(
                                address=int(register_address), count=2, device_id=slave_address
                            )

                            if not response.isError():
                                sensor_value     = float("%.2f" % client.convert_from_registers(
                                    response.registers, data_type=client.DATATYPE.FLOAT32
                                ))
                                meter_value_temp = meter_value_temp + (sensor_value,)
                            else:
                                print("Error Reading Register")
                    finally:
                        client.close()
                else:
                    print("Unable to connect to the Modbus Server.")

                meter_value_temp = tuple(map(float, meter_value_temp))
                meter_value_temp = meter_value_temp + (date_now,)
                meter_value      = (gateway_id, meter_id) + meter_value_temp

                # insert_sensor_logs returns True if cloud insert succeeded, False if it fell back to offline
                cloud_ok = insert_algo.insert_sensor_logs(
                    meter_id, slave_address, column_parameter, meter_value,
                    cloud_conn=cloud_conn, local_conn=local_conn
                )

                # If cloud insert failed, mark cloud_conn as None so next cycle retries
                if not cloud_ok:
                    cloud_conn = None

        except Exception as e:
            print(f"[{date_now}] Cycle error: {e}")
            # Do not exit — log and continue to next cycle

        print(f"[{date_now}] Cycle complete. Sleeping...")
        time.sleep(10)

finally:
    # Reached only on KeyboardInterrupt or fatal crash
    print("Gateway shutting down. Closing connections...")
    if cloud_conn:
        cloud_conn.close()
    local_conn.close()
