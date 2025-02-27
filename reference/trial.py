from pymodbus.client import ModbusSerialClient
import mysql.connector
import time
from datetime import datetime
import mariadb
import sys

client = ModbusSerialClient(
	port='/dev/ttyUSB0',
	baudrate =9600,
	stopbits=1,
	parity="N",
	bytesize=8,
	timeout=2	
)

host = "localhost"
database = "meterdb"
user = "marvin"
password = "0marvin0"

slave_list = [7, 6, 5]
param_name = ['V  :', 'I  :', 'W  :', 'VA :', 'kWh:']
reg_address = [0, 6, 52, 56, 342]
placeholder = [1, 2, 3, 4, 5] 
reg = 0

while True:
	dt = datetime.now()
	timeStamp = dt.strftime('%Y-%m-%d %H:%M:%S')
	minute=dt.strftime('%M')
	print(timeStamp)

	if int(minute)%5 == 0 and int(dt.strftime('%S'))==0:

		for s in slave_list:
			unit_id = s
			y=0
			for x in reg_address:
				register_address = x
				
				if client.connect():
					try:
						response = client.read_input_registers(address=register_address, count=2, slave=unit_id)
						if not response.isError():
							placeholder[y] = client.convert_from_registers(response.registers, data_type=client.DATATYPE.FLOAT32)
							print('ID',s, param_name[y], "%.2f"%placeholder[y])
							#print('id',s, param_name[y],client.convert_from_registers(response.registers, data_type=client.DATATYPE.FLOAT32))
						
						else:
							print(f"Error reading register: {response}")
					finally:
						client.close()
				else:
					print("Unable to connect to the Modbus server.")
				y=y+1
			
			
			print('next')
			try:
				conn = mariadb.connect(
					host=host,
					user=user,
					password=password,
					database=database
				)
				cur = conn.cursor()
				cur.execute('''CREATE TABLE IF NOT EXISTS SPMSPSCOFFC01
									(date DATETIME, SID int, V float, I float, P float, VA float, kWh float)''')
				cur.execute("INSERT INTO SPMSPSCOFFC01 (date, SID, V, I, P, VA, kWh) VALUES (?,?,?,?,?,?,?)",
									(timeStamp, s,"%.2f"%placeholder[0], "%.2f"%placeholder[1], "%.2f"%placeholder[2], "%.2f"%placeholder[3], "%.2f"%placeholder[4]))			
				conn.commit()
			
			except mariadb.Error as e:
				print(f"Error connecting to MariaDB Platform: {e}")
				sys.exit(1)
				
			finally:
				if cur:
					cur.close()
				if conn:
					conn.close()
					
					
			try:
				conn = mariadb.connect(
					host="45.89.204.3",
					database = "u449166819_john",
					user="u449166819_gabriel",
					password="0Bathan0",
					port = 3306
				)
				cur = conn.cursor()
			
				sql = (
					"INSERT INTO SPMSPSCOFFC01(date, SID, V, I, P, VA, kWh)"
					"VALUES(%s,%s,%s,%s,%s,%s,%s)"
				)
				val = (timeStamp, s,"%.2f"%placeholder[0], "%.2f"%placeholder[1], "%.2f"%placeholder[2], "%.2f"%placeholder[3], "%.2f"%placeholder[4])
				cur.execute(sql, val)
				conn.commit()
			
			except mariadb.Error as e:
				print(f"Error connecting to MariaDB Platform: {e}")
				sys.exit(1)
				
			finally:
				if cur:
					cur.close()
				if conn:
					conn.close()
			
			
	time.sleep(1)
