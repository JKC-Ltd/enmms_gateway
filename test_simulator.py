"""
Gateway Simulator — ENMMS local test tool

Publishes fake sensor readings to the MQTT broker on a configurable interval,
simulating what index.py does with real Modbus meters.

Use this on your development machine to test the full pipeline:
  MQTT broker → DB Writer → MySQL
  MQTT broker → Reverb Forwarder → Laravel → WebSocket → Browser

Usage:
    pip install paho-mqtt
    python3 test_simulator.py

    # Or override defaults:
    MQTT_HOST=localhost MQTT_PORT=1883 POLL_INTERVAL=5 python3 test_simulator.py
"""

import json
import os
import random
import time
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    raise SystemExit("paho-mqtt required: pip install paho-mqtt")

# ── Config (override via environment variables) ───────────────────────────────
MQTT_HOST      = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT      = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER      = os.getenv("MQTT_USER", "enmms")
MQTT_PASSWORD  = os.getenv("MQTT_PASSWORD", "")        # same as gateway_config.py
COMPANY_CODE   = os.getenv("COMPANY_CODE", "siix")
GATEWAY_CODE   = os.getenv("GATEWAY_CODE", "GAT-02")
GATEWAY_ID     = int(os.getenv("GATEWAY_ID", 2))
POLL_INTERVAL  = float(os.getenv("POLL_INTERVAL", 10))  # seconds between readings

# ── Simulated sensors ─────────────────────────────────────────────────────────
# Mirrors the sensor IDs used in the dashboard JS (15-19).
# Add or remove entries to match your local database.
SIMULATED_SENSORS = [
    {"sensor_id": 15},
    {"sensor_id": 16},
    {"sensor_id": 17},
    {"sensor_id": 18},
    {"sensor_id": 19},
]

# ── Base values — readings drift realistically each cycle ─────────────────────
_state = {s["sensor_id"]: {"energy": 1000.0 + random.uniform(0, 500)} for s in SIMULATED_SENSORS}


def _make_reading(sensor_id: int) -> dict:
    """Generate a plausible sensor reading that drifts over time."""
    st = _state[sensor_id]
    delta_energy = random.uniform(0.01, 0.2)
    st["energy"] += delta_energy

    voltage = random.uniform(218.0, 222.0)
    current = random.uniform(10.0, 15.0)
    power   = round(voltage * current * random.uniform(0.85, 0.95), 2)

    return {
        "gateway_id":      GATEWAY_ID,
        "sensor_id":       sensor_id,
        "energy":          round(st["energy"], 2),
        "voltage_ab":      round(voltage, 2),
        "voltage_bc":      round(voltage + random.uniform(-1, 1), 2),
        "voltage_ca":      round(voltage + random.uniform(-1, 1), 2),
        "current_a":       round(current, 2),
        "current_b":       round(current + random.uniform(-0.5, 0.5), 2),
        "current_c":       round(current + random.uniform(-0.5, 0.5), 2),
        "real_power":      power,
        "apparent_power":  round(power / random.uniform(0.88, 0.95), 2),
        "datetime_created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[simulator] Connected to broker at {}:{}.".format(MQTT_HOST, MQTT_PORT))
        print("[simulator] Publishing a reading every {} second(s) for sensors: {}".format(
            POLL_INTERVAL, [s["sensor_id"] for s in SIMULATED_SENSORS]
        ))
        print("[simulator] Press Ctrl+C to stop.\n")
    else:
        raise SystemExit("[simulator] Broker refused connection (rc={}). "
                         "Check host/port/credentials.".format(rc))


def main():
    client = mqtt.Client(client_id="enmms-simulator", clean_session=True)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect

    print("[simulator] Connecting to {}:{} ...".format(MQTT_HOST, MQTT_PORT))
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    # Brief wait for the on_connect callback to fire
    time.sleep(1)

    try:
        while True:
            for sensor in SIMULATED_SENSORS:
                sensor_id = sensor["sensor_id"]
                reading   = _make_reading(sensor_id)
                topic     = "enmms/{}/{}/sensor/{}".format(
                    COMPANY_CODE, GATEWAY_CODE, sensor_id
                )
                payload   = json.dumps(reading)
                result    = client.publish(topic, payload, qos=1)
                status    = "OK" if result.rc == mqtt.MQTT_ERR_SUCCESS else "FAIL rc={}".format(result.rc)
                print("[simulator] {} → {}  energy={:.2f} kWh  power={:.0f} W".format(
                    status, topic, reading["energy"], reading["real_power"]
                ))

            print()
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[simulator] Stopped.")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
