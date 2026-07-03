from pymodbus.client import ModbusSerialClient
import db_connections
import gateway_config
import insert_algo
import time
from datetime import datetime
import sys

# ------------------------------------------------------------------
# Gateway Information
# ------------------------------------------------------------------
gateway_id = gateway_config.gateway_id
gateway_code = gateway_config.gateway_code

# ------------------------------------------------------------------
# Create Modbus Client ONCE
# ------------------------------------------------------------------
client = ModbusSerialClient(
    port='/dev/ttyUSB0',
    baudrate=9600,
    stopbits=1,
    parity="N",
    bytesize=8,
    timeout=2
)

# ------------------------------------------------------------------
# Open Database Connections ONCE
# ------------------------------------------------------------------
cloud_conn = db_connections.cloud_database()
if not cloud_conn:
    print("Cloud database unreachable. Running in offline mode.")

local_conn = db_connections.local_database()
if not local_conn:
    print("Local database unreachable.")
    sys.exit(1)

try:

    while True:
        print("Testing connections...")
        # ----------------------------------------------------------
        # Current Timestamp
        # ----------------------------------------------------------
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ----------------------------------------------------------
        # Reconnect Database if Needed
        # ----------------------------------------------------------
        if cloud_conn:
            print("Nag True sa Connection...")
            cloud_conn = db_connections.ensure_connected(cloud_conn)
        else:
            print("FALSE Connection...")
            # Attempt to open a new cloud connection if previously None/False
            cloud_conn = db_connections.cloud_database() or None

        if local_conn:
            local_conn = db_connections.ensure_connected(local_conn)
        else:
            local_conn = db_connections.local_database() or None

        print("------------------INDEX------------------")
        print(f"Cloud_Conn:{cloud_conn}")
        print(f"Local_Conn:{local_conn}")
        # ----------------------------------------------------------
        # Synchronize Database
        # ----------------------------------------------------------
        try:
            if cloud_conn and local_conn:
                db_connections.sync(
                    gateway_id,
                    from_conn=cloud_conn,
                    to_conn=local_conn,
                    fromCloudToLocal=True
                )

                db_connections.sync(
                    gateway_id,
                    from_conn=local_conn,
                    to_conn=cloud_conn,
                    fromCloudToLocal=False
                )
            else:
                if not cloud_conn:
                    print("Cloud DB unavailable — skipping sync to cloud/local.")
                if not local_conn:
                    print("Local DB unavailable — skipping sync to local.")

        except Exception as e:
            print(f"Sync Error : {e}")

        # ----------------------------------------------------------
        # Get Meter Configuration
        # ----------------------------------------------------------
        # meter_results = gateway_config.get_metter_ids(local_conn)

        meter_results = [
            {
                'id': 1,
                'sensor_model_id': 2,
                'slave_address': 5,
                'register_address': [200, 202, 204, 6, 8, 10, 52, 56, 342],
                'parameter': ['voltage_ab', 'voltage_bc', 'voltage_ca', 'current_a', 'current_b', 'current_c', 'real_power', 'apparent_power', 'energy']}]

        # ----------------------------------------------------------
        # Connect to Modbus ONLY ONCE
        # ----------------------------------------------------------
        if not client.connected:
            if not client.connect():
                print("Unable to connect to Modbus.")
                time.sleep(5)
                continue

        # ----------------------------------------------------------
        # Read Every Meter
        # ----------------------------------------------------------
        for meter_result in meter_results:

            model_id = meter_result['sensor_model_id']
            meter_id = meter_result['id']
            slave_address = int(meter_result['slave_address'])

            columns = (
                ["gateway_id", "sensor_id"]
                + meter_result['parameter']
                + ["datetime_created"]
            )

            register_addresses = meter_result['register_address']
            column_parameter = ", ".join(columns)

            meter_value_temp = ()

            try:

                for register_address in register_addresses:

                    if model_id == 1:
                        response = client.read_holding_registers(
                            address=int(register_address),
                            count=2,
                            slave=slave_address
                        )
                    else:
                        response = client.read_input_registers(
                            address=int(register_address),
                            count=2,
                            device_id=slave_address
                        )

                    if response.isError():
                        print(
                            f"Meter {meter_id} Register {register_address} Error"
                        )
                        continue

                    sensor_value = float("%.2f" % client.convert_from_registers(
                        response.registers,
                        data_type=client.DATATYPE.FLOAT32
                    ))

                    meter_value_temp += (sensor_value,)

            except Exception as e:
                print(f"Meter {meter_id} Error : {e}")
                continue

            meter_value_temp = tuple(map(float, meter_value_temp))
            datetime_created = date_now if cloud_conn else f"'{date_now}'"
            meter_value_temp += (datetime_created,)

            meter_value = (
                gateway_id,
                meter_id
            ) + meter_value_temp

            try:

                insert_algo.insert_sensor_logs(
                    meter_id,
                    slave_address,
                    column_parameter,
                    meter_value,
                    cloud_conn=cloud_conn,
                    local_conn=local_conn
                )

            except Exception as e:
                print(f"Insert Error : {e}")

        print(f"[{date_now}] Polling Finished.")

        # ----------------------------------------------------------
        # Wait 5 Seconds
        # ----------------------------------------------------------
        time.sleep(5)

except KeyboardInterrupt:
    print("Program stopped by user.")

finally:

    print("Closing connections...")

    try:
        client.close()
    except:
        pass

    try:
        if cloud_conn and cloud_conn.is_connected():
            cloud_conn.close()
    except:
        pass

    try:
        if local_conn and local_conn.is_connected():
            local_conn.close()
    except:
        pass
