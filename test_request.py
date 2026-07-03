import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime


def cloud_database():
    try:
        connection = mysql.connector.connect(
            host="srv2208.hstgr.io",
            user="u565803524_gateway_test",
            password="GatewayTest0123",
            database="u565803524_gateway_test"
        )

        if connection.is_connected():
            print("Connected to Cloud Database")
            return connection

    except Error as e:
        print(f"Connection Error: {e}")

    return None


def ensure_connected(conn):
    try:
        if conn is None:
            return cloud_database()

        if not conn.is_connected():
            print("Connection lost. Reconnecting...")
            conn.reconnect(attempts=3, delay=2)

    except Error:
        conn = cloud_database()

    return conn


def insert_data(conn, parameter):
    try:
        cursor = conn.cursor()

        sql = """
        INSERT INTO test_tbl (parameters)
        VALUES (%s)
        """

        cursor.execute(sql, (parameter,))
        conn.commit()

        print(f"[{datetime.now()}] Inserted: {parameter}")

        cursor.close()

    except Error as e:
        print(f"Insert Error: {e}")


def main():

    conn = cloud_database()

    counter = 1

    while True:

        conn = ensure_connected(conn)

        if conn:

            parameter = f"Sample Parameter {counter}"

            insert_data(conn, parameter)

            counter += 1

        time.sleep(5)


if __name__ == "__main__":
    main()
