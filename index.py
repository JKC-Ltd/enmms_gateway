from pymodbus.client import ModbusSerialClient
import db_connections
import gateway_config
import insert_algo
import mqtt_publisher
import time
from datetime import datetime
import sys

# ── Modbus client (initialised once; reconnects per meter per loop) ───────────
client = ModbusSerialClient(
    port='/dev/ttyUSB0',
    baudrate=9600,
    stopbits=1,
    parity="N",
    bytesize=8,
    timeout=2
)

# ── Local DB connection (always required) ─────────────────────────────────────
local_conn = db_connections.local_database()
if not local_conn:
    print("Local database unreachable. Cannot continue.")
    sys.exit(1)

# ── Cloud DB connection (only opened when CLOUD_DIRECT_WRITE is True) ─────────
cloud_conn = None
if gateway_config.CLOUD_DIRECT_WRITE:
    cloud_conn = db_connections.cloud_database()
    if not cloud_conn:
        print("Cloud database unreachable at startup. Running without direct cloud write.")

# ── MQTT publisher (connects asynchronously; queues locally when offline) ─────
publisher = mqtt_publisher.MQTTPublisher()
publisher.start()

print("Gateway started. Poll interval: {} seconds.".format(
    gateway_config.POLL_INTERVAL_SECONDS
))

try:
    while True:
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── Reconnect DB connections if they dropped ──────────────────────────
        db_connections.ensure_connected(local_conn)
        if gateway_config.CLOUD_DIRECT_WRITE and cloud_conn:
            db_connections.ensure_connected(cloud_conn)

        # ── Sync offline queues before reading meters ─────────────────────────
        if gateway_config.CLOUD_DIRECT_WRITE and cloud_conn:
            db_connections.sync(
                gateway_config.gateway_id,
                from_conn=cloud_conn, to_conn=local_conn, fromCloudToLocal=True
            )
            db_connections.sync(
                gateway_config.gateway_id,
                from_conn=local_conn, to_conn=cloud_conn, fromCloudToLocal=False
            )

        # ── Refresh meter configuration from local DB ─────────────────────────
        meter_results = gateway_config.get_metter_ids(local_conn)

        for meter_result in meter_results:
            model_id           = meter_result['sensor_model_id']
            meter_id           = meter_result['id']
            slave_address      = int(meter_result['slave_address'])
            columns            = (["gateway_id", "sensor_id"]
                                   + meter_result['parameter']
                                   + ['datetime_created'])
            register_addresses = meter_result['register_address']
            column_parameter   = ', '.join(columns)
            meter_value_temp   = ()

            if client.connect():
                try:
                    for register_address in register_addresses:
                        if model_id == 1:
                            response = client.read_holding_registers(
                                address=int(register_address), count=2, slave=slave_address
                            )
                        else:
                            response = client.read_input_registers(
                                address=int(register_address), count=2, slave=slave_address
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
            meter_value      = (gateway_config.gateway_id, meter_id) + meter_value_temp

            # ── Write to local (always) and optionally to cloud MySQL ─────────
            insert_algo.insert_sensor_logs(
                meter_id, slave_address, column_parameter, meter_value,
                cloud_conn=cloud_conn, local_conn=local_conn
            )

            # ── Publish to MQTT broker (falls back to outbox if offline) ──────
            payload = dict(zip(columns, meter_value))
            topic   = "enmms/{}/{}/sensor/{}".format(
                gateway_config.COMPANY_CODE,
                gateway_config.gateway_code,
                meter_id
            )
            publisher.publish(topic, payload)

        time.sleep(gateway_config.POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print("Gateway stopped by user.")
finally:
    publisher.stop()
    if cloud_conn:
        cloud_conn.close()
    local_conn.close()
    print("Gateway shutdown complete.")
