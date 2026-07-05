# Migrating index.py: Cron One-Shot → Persistent Loop + systemd

---

## 1. Overview

### The Problem with Cron

When `index.py` is scheduled via cron, it runs as a **one-shot script**: it opens database connections, does its work, closes the connections, and exits. The next time cron fires, it starts fresh and opens new connections again.

```
CRON (one-shot per run):

  [open] → [work] → [close] → [exit]
  [open] → [work] → [close] → [exit]
  [open] → [work] → [close] → [exit]
  ...
  12 open/close cycles every hour (at 5-minute intervals)
```

### The Persistent Loop

A persistent loop opens connections **once at startup** and reuses them for the entire lifetime of the process. The loop sleeps between cycles instead of exiting.

```
PERSISTENT LOOP:

  [open once]
      ↓
  [work] → [sleep 5 min] → [work] → [sleep 5 min] → [work] → ...
      ↓
  [close on shutdown]
  1 connection for the entire lifetime
```

### Why This Matters: Hostinger max_connections_per_hour = 150

Hostinger shared hosting enforces a hard limit of **150 new connections per hour** per database user. Every time `mysql.connector.connect()` is called, it counts as one new connection — regardless of how long it stays open.

**Connection math:**

| Setup | Gateways | Connections/run | Runs/hour | Total/hour |
|---|---|---|---|---|
| Cron every 5 min | 7 (SIIX) | 1 | 12 | **84/hour** |
| Cron every 5 min | 22 (Uratext) | 1 | 12 | **264/hour** ❌ |
| Persistent loop | 7 (SIIX) | — | — | **7 total** ✅ |
| Persistent loop | 22 (Uratext) | — | — | **22 total** ✅ |

At 22 gateways, cron exceeds the Hostinger limit every hour. The persistent loop keeps the total connection count equal to the number of gateways — and those connections are never closed unless the internet drops.

---

## 2. Code Changes Required

### 2a. db_connections.py — Improve `ensure_connected()`

**Current version:**

```python
def ensure_connected(conn):
    if conn and not conn.is_connected():
        print("Connection lost. Reconnecting...")
        conn.reconnect(attempts=3, delay=2)
    return conn
```

**Improved version:**

```python
def ensure_connected(conn):
    if conn is None:
        return conn
    try:
        conn.ping(reconnect=True, attempts=3, delay=2)
    except mysql.connector.Error:
        print("Connection lost and could not reconnect.")
    return conn
```

**Why `ping(reconnect=True)` is better than `is_connected()`:**

`is_connected()` only checks the client-side socket state — it reads a local flag without sending any data to the server. If the internet drops and the TCP connection silently dies (no RST packet, which is common on mobile/LTE links), `is_connected()` still returns `True` because the socket *looks* alive from the client's perspective.

`ping()` sends an actual packet to the MySQL server and waits for a response. If the connection is dead — whether due to a dropped internet link, a server timeout, or a firewall killing idle connections — `ping()` detects it immediately. With `reconnect=True`, it also attempts to establish a new connection in the same call, making recovery automatic.

---

### 2b. index.py — Wrap in Persistent Loop

**Current index.py (summarized):**

```python
date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # set once at startup — will be stale next run

cloud_conn = db_connections.cloud_database()
local_conn = db_connections.local_database()

try:
    sync(...)
    poll meters...
    insert...
finally:
    cloud_conn.close()
    local_conn.close()
# script exits — cron fires again in 5 minutes and opens new connections
```

**New index.py (persistent loop):**

```python
from pymodbus.client import ModbusSerialClient
import db_connections
import gateway_config
import insert_algo
import time
from datetime import datetime
import sys

gateway_id   = gateway_config.gateway_id
gateway_code = gateway_config.gateway_code

client = ModbusSerialClient(
    port='/dev/ttyUSB0',
    baudrate=9600,
    stopbits=1,
    parity="N",
    bytesize=8,
    timeout=2
)

# Open connections once at startup
cloud_conn = db_connections.cloud_database()
if not cloud_conn:
    print("Cloud database unreachable at startup. Running in offline mode.")

local_conn = db_connections.local_database()
if not local_conn:
    print("Local database unreachable. Cannot continue.")
    sys.exit(1)

try:
    while True:
        date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # refreshed each cycle

        # Retry cloud connection if it was never established or fully dropped
        if not cloud_conn:
            cloud_conn = db_connections.cloud_database()
            if cloud_conn:
                print(f"Cloud connection re-established at {date_now}")

        try:
            # Sync offline queue before polling meters
            db_connections.sync(gateway_id, from_conn=cloud_conn, to_conn=local_conn, fromCloudToLocal=True)
            db_connections.sync(gateway_id, from_conn=local_conn, to_conn=cloud_conn, fromCloudToLocal=False)

            # Fetch meter configuration
            meter_results = gateway_config.get_metter_ids(local_conn)

            for meter_result in meter_results:
                model_id           = meter_result['sensor_model_id']
                meter_id           = meter_result['id']
                slave_address      = int(meter_result['slave_address'])
                columns            = ["gateway_id", "sensor_id"] + meter_result['parameter'] + ['datetime_created']
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
                meter_value      = (gateway_id, meter_id) + meter_value_temp

                insert_algo.insert_sensor_logs(
                    meter_id, slave_address, column_parameter, meter_value,
                    cloud_conn=cloud_conn, local_conn=local_conn
                )

        except Exception as e:
            print(f"[{date_now}] Cycle error: {e}")
            # Do not exit — log the error and continue to next cycle

        print(f"[{date_now}] Cycle complete. Sleeping 5 minutes...")
        time.sleep(300)

finally:
    # Reached only on KeyboardInterrupt or fatal crash
    print("Gateway shutting down. Closing connections...")
    if cloud_conn:
        cloud_conn.close()
    local_conn.close()
```

**Key changes from the old version:**

1. **`date_now` moved inside the loop** — the old code set `date_now` once at startup, so every record inserted during the run would have the same stale timestamp. Now it's refreshed at the top of each 5-minute cycle.

2. **`while True` wraps all the work** — instead of running once and exiting, the script loops forever. The cron schedule is replaced by `time.sleep(300)` at the bottom of the loop.

3. **`if not cloud_conn` retry block** — if the cloud connection was never established at startup (no internet at boot), or if `insert_algo` set it to a falsy state after a failure, this block attempts to reconnect at the start of each cycle without restarting the process.

4. **`time.sleep(300)` replaces cron** — the 5-minute interval is now controlled inside the script. Remove the cron entry after deploying (see Section 3).

5. **Inner `try/except Exception`** — catches any error that occurs during a cycle (bad Modbus read, query failure, transient network error) and logs it without killing the process. The next cycle starts normally after the sleep.

6. **Outer `try/finally`** — still closes both connections cleanly when the process is stopped via `systemctl stop`, `KeyboardInterrupt`, or a fatal crash that reaches the top level.

---

## 3. Removing the Cron Job

If you previously scheduled `index.py` with cron, you must remove that entry before deploying the persistent loop. Running both simultaneously will create multiple concurrent processes, each holding their own database connections — the opposite of what this migration is for.

```bash
# View current crontab to confirm the entry exists
crontab -l

# Open crontab for editing
crontab -e
# Find the line that runs index.py (e.g. */5 * * * * python3 /home/pi/enmms_gateway/index.py)
# Delete that line, then save and exit

# Verify the entry is gone
crontab -l
```

> **Warning:** If you keep both the cron job and the systemd service running at the same time, you will have multiple `index.py` processes running simultaneously. Each process opens its own database connections, multiplying your connection count and defeating the entire purpose of this migration.

---

## 4. Setting Up systemd

systemd replaces cron's job of launching the script. Unlike cron, systemd manages the process for its entire lifetime and handles:

- **Auto-start on boot** — the service starts automatically after a power outage once the device comes back online
- **Auto-restart on crash** — if `index.py` exits unexpectedly (unhandled exception, segfault, OOM kill), systemd restarts it after a short delay
- **Centralised logging** — all `print()` output is captured and viewable with `journalctl`

### Step 1: Create the service file

```bash
sudo nano /etc/systemd/system/enmms-gateway.service
```

Paste the following content. **Update `ExecStart` and `WorkingDirectory` to match the actual path on your device.**

```ini
[Unit]
Description=ENMMS Gateway Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /home/pi/enmms_gateway/index.py
WorkingDirectory=/home/pi/enmms_gateway
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**What each directive does:**

| Directive | Purpose |
|---|---|
| `After=network.target` | Waits for the network interface to be up before starting — essential because the script connects to the cloud database at startup |
| `Restart=always` | Restarts the service on any exit: crash, unhandled exception, or even a clean exit (e.g. `sys.exit()`) |
| `RestartSec=10` | Waits 10 seconds before restarting — prevents a rapid restart loop if the script crashes immediately (e.g. database unreachable at boot) |
| `StandardOutput=journal` | Sends all stdout output (your `print()` calls) to the systemd journal, viewable with `journalctl` |
| `StandardError=journal` | Sends stderr (Python tracebacks) to the same journal |

### Step 2: Enable and start the service

```bash
# Reload systemd to pick up the new service file
sudo systemctl daemon-reload

# Enable the service to auto-start on boot
sudo systemctl enable enmms-gateway

# Start the service now (without rebooting)
sudo systemctl start enmms-gateway

# Confirm it's running
sudo systemctl status enmms-gateway
```

A healthy status output looks like this:

```
● enmms-gateway.service - ENMMS Gateway Service
     Loaded: loaded (/etc/systemd/system/enmms-gateway.service; enabled)
     Active: active (running) since Mon 2025-01-01 08:00:00 PST; 2min ago
   Main PID: 1234 (python3)
```

- `enabled` confirms it will auto-start on boot
- `active (running)` confirms the process is currently alive

### Step 3: Useful commands

```bash
# Watch live logs — streams print() output from index.py in real time
journalctl -u enmms-gateway -f

# View the last 100 lines of logs
journalctl -u enmms-gateway -n 100

# Stop the service (triggers the finally: block in index.py — closes connections cleanly)
sudo systemctl stop enmms-gateway

# Restart the service after a code change
sudo systemctl restart enmms-gateway

# Disable auto-start on boot (does not stop the currently running service)
sudo systemctl disable enmms-gateway
```

---

## 5. Offline / Online Behavior

The persistent loop is designed to survive internet outages without any manual intervention or process restart.

### What happens when internet drops

```
Cycle 1: cloud_conn ALIVE  → sync ✅ → meters ✅ → insert cloud ✅ → insert local ✅
Cycle 2: cloud_conn ALIVE  → sync ✅ → meters ✅ → insert cloud ✅ → insert local ✅

--- INTERNET DROPS ---

Cycle 3: cloud_conn STALE
  → ping() detects dead connection → reconnect fails (no internet)
  → sync skipped
  → meters ✅  (Modbus is local over RS-485, completely unaffected)
  → insert cloud FAILS → falls back to sensor_offlines ✅
  → insert local ✅
  → sleep 5 min

Cycle 4, 5, 6...  same as Cycle 3

--- INTERNET RESTORED ---

Cycle N: cloud_conn STALE
  → ping(reconnect=True) detects dead → reconnect SUCCEEDS ✅
  → sync drains sensor_offlines in batches of 500 ✅
  → meters ✅
  → insert cloud ✅  (back to normal)
```

### Scenario matrix

| Scenario | Gateway restarts? | Recovers automatically? | Data lost? |
|---|---|---|---|
| Normal operation | No | N/A | No |
| Internet drops mid-run | No | Yes — next cycle retries | No — stored in sensor_offlines |
| Internet restored | No | Yes — ping() reconnects | No — queue drained by sync() |
| Script crashes | No | Yes — systemd restarts in 10s | No |
| Power outage | Yes (on restore) | Yes — systemd starts on boot | No |
| Starts with no internet | No | Yes — retries cloud_database() each cycle | No |

---

## 6. Connection Count Comparison

| Setup | SIIX (7 gateways) | Uratext (22 gateways) | 30 gateways | Hostinger limit |
|---|---|---|---|---|
| Cron every 5 min (old code) | 84+/hour | 264+/hour ❌ | 360+/hour ❌ | 150/hour |
| Cron every 5 min (fixed code) | 84/hour | 264/hour ❌ | 360/hour ❌ | 150/hour |
| Persistent loop + systemd | 7 total | 22 total ✅ | 30 total ✅ | 150/hour |

With the persistent loop, `max_connections_per_hour` is essentially a non-issue. Connections are opened once and reused indefinitely. The only time a new connection is opened is when the internet drops and then restores — which is a rare event, not something that happens 12 times per hour. Even with 30 gateways and occasional reconnects, the connection count stays well within Hostinger's limit.
