"""
MQTT Publisher — ENMMS Gateway v2

Maintains one persistent TCP connection to the MQTT broker.
Falls back to the local mqtt_outbox table when the broker is unreachable.
A background thread drains the outbox when connectivity is restored.

Usage:
    import mqtt_publisher
    publisher = mqtt_publisher.MQTTPublisher()
    publisher.start()                             # call once at startup
    publisher.publish("enmms/siix/GAT-01/sensor/7", {"sensor_id": 7, ...})
    publisher.stop()                              # call on shutdown
"""

import json
import time
import threading

try:
    import paho.mqtt.client as mqtt
    _PAHO_AVAILABLE = True
except ImportError:
    _PAHO_AVAILABLE = False
    print("[mqtt_publisher] WARNING: paho-mqtt not installed. "
          "All readings will be queued in mqtt_outbox.")

import gateway_config
import db_connections


class MQTTPublisher:
    """Persistent MQTT publisher with local outbox fallback."""

    DRAIN_INTERVAL = 30    # seconds between outbox drain attempts
    DRAIN_BATCH    = 100   # max messages per drain cycle

    def __init__(self):
        self._connected    = False
        self._client       = None
        self._lock         = threading.Lock()
        self._drain_thread = None
        self._running      = False
        self._local_conn   = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        """Initialize MQTT client and start the background outbox drain thread."""
        self._running    = True
        self._local_conn = db_connections.local_database()
        self._ensure_outbox_table()

        if not _PAHO_AVAILABLE:
            print("[mqtt_publisher] paho-mqtt unavailable. Queuing all readings.")
        else:
            self._client = mqtt.Client(
                client_id="enmms-{}-{}".format(
                    gateway_config.COMPANY_CODE,
                    gateway_config.gateway_code
                ),
                clean_session=False
            )

            if gateway_config.MQTT_USER:
                self._client.username_pw_set(
                    gateway_config.MQTT_USER,
                    gateway_config.MQTT_PASSWORD
                )

            self._client.on_connect    = self._on_connect
            self._client.on_disconnect = self._on_disconnect

            try:
                self._client.connect_async(
                    gateway_config.MQTT_HOST,
                    gateway_config.MQTT_PORT,
                    keepalive=60
                )
                self._client.loop_start()
                print("[mqtt_publisher] Connecting to broker at {}:{} ...".format(
                    gateway_config.MQTT_HOST, gateway_config.MQTT_PORT
                ))
            except Exception as e:
                print("[mqtt_publisher] Initial connect failed: {}. "
                      "Readings will be queued.".format(e))

        self._drain_thread = threading.Thread(
            target=self._drain_loop, daemon=True, name="mqtt-outbox-drain"
        )
        self._drain_thread.start()

    def stop(self):
        """Graceful shutdown — stops the drain thread and disconnects."""
        self._running = False
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
        if self._local_conn and self._local_conn.is_connected():
            self._local_conn.close()
        print("[mqtt_publisher] Stopped.")

    def publish(self, topic, payload_dict):
        """
        Publish a reading to the MQTT broker.

        If the broker is unreachable the message is written to mqtt_outbox
        in local MySQL and will be published when connectivity restores.

        Args:
            topic       (str):  MQTT topic, e.g. "enmms/siix/GAT-02/sensor/7"
            payload_dict (dict): Sensor reading dict; must be JSON-serialisable.
        """
        payload_json = json.dumps(payload_dict, default=str)

        if self._connected and self._client and _PAHO_AVAILABLE:
            try:
                result = self._client.publish(topic, payload_json, qos=1)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    return
                print("[mqtt_publisher] Publish returned rc={}. Queuing.".format(result.rc))
            except Exception as e:
                print("[mqtt_publisher] Publish error: {}. Queuing.".format(e))

        # Broker unavailable — store locally
        self._queue_to_outbox(topic, payload_json)

    # ── MQTT Callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True
            print("[mqtt_publisher] Connected to broker at {}:{}.".format(
                gateway_config.MQTT_HOST, gateway_config.MQTT_PORT
            ))
        else:
            self._connected = False
            print("[mqtt_publisher] Connection refused (rc={}). Will retry.".format(rc))

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False
        if rc != 0:
            print("[mqtt_publisher] Disconnected unexpectedly (rc={}). "
                  "paho will auto-reconnect.".format(rc))

    # ── Outbox ────────────────────────────────────────────────────────────────

    def _ensure_outbox_table(self):
        """Create mqtt_outbox if it doesn't already exist."""
        if not self._local_conn:
            return
        try:
            cursor = self._local_conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mqtt_outbox (
                    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    topic      VARCHAR(255) NOT NULL,
                    payload    JSON         NOT NULL,
                    qos        TINYINT      NOT NULL DEFAULT 1,
                    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_created (created_at)
                )
            """)
            self._local_conn.commit()
            cursor.close()
        except Exception as e:
            print("[mqtt_publisher] Could not ensure outbox table: {}".format(e))

    def _queue_to_outbox(self, topic, payload_json):
        if not self._local_conn:
            print("[mqtt_publisher] No local DB connection — reading dropped.")
            return
        try:
            db_connections.ensure_connected(self._local_conn)
            cursor = self._local_conn.cursor()
            cursor.execute(
                "INSERT INTO mqtt_outbox (topic, payload) VALUES (%s, %s)",
                (topic, payload_json)
            )
            self._local_conn.commit()
            cursor.close()
            print("[mqtt_publisher] Queued to outbox (topic: {}).".format(topic))
        except Exception as e:
            print("[mqtt_publisher] Failed to write to outbox: {}".format(e))

    def _drain_loop(self):
        """Background thread: drains outbox whenever the broker is connected."""
        while self._running:
            time.sleep(self.DRAIN_INTERVAL)
            if self._connected and self._client and _PAHO_AVAILABLE:
                self._drain_outbox()

    def _drain_outbox(self):
        if not self._local_conn:
            return
        try:
            db_connections.ensure_connected(self._local_conn)
            cursor = self._local_conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT id, topic, payload FROM mqtt_outbox "
                "ORDER BY created_at ASC LIMIT %s",
                (self.DRAIN_BATCH,)
            )
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                return

            print("[mqtt_publisher] Draining {} outbox message(s).".format(len(rows)))
            published_ids = []

            for row in rows:
                if not self._connected:
                    break   # broker went away mid-drain — stop and retry next cycle
                try:
                    result = self._client.publish(
                        row['topic'],
                        json.dumps(row['payload']),
                        qos=1
                    )
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        published_ids.append(row['id'])
                    else:
                        break
                except Exception:
                    break

            if published_ids:
                del_cursor = self._local_conn.cursor()
                placeholders = ','.join(['%s'] * len(published_ids))
                del_cursor.execute(
                    "DELETE FROM mqtt_outbox WHERE id IN ({})".format(placeholders),
                    published_ids
                )
                self._local_conn.commit()
                del_cursor.close()
                print("[mqtt_publisher] Drained {} message(s) from outbox.".format(
                    len(published_ids)
                ))

        except Exception as e:
            print("[mqtt_publisher] Drain error: {}".format(e))
