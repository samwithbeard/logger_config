#!/usr/bin/env python3
version="0.0.116"
print(version)

import hashlib
import serial
import serial.tools.list_ports
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import os
import logging
from configparser import ConfigParser
import traceback
import ssl
import uuid
import re
import json
import jsonschema
from jsonschema import validate
import requests
from SDMU_parser import parse_ICN_line
from multiprocessing import Process, Queue
from gps_collector import start_gps_collector
import subprocess
import psutil
from enum import Enum
import threading
import ftplib
import io
import copy

if os.name == 'posix':  # Check if the operating system is Linux
    from gpiozero import LED


else:    
    print("gpiozero is not supported on Windows. Skipping GPIO setup.")
    # Define a simulated LED class for Windows or systems without gpiozero
    class SimulatedLED:
        def __init__(self, pin):
            self.pin = pin
            self._state = False

        def on(self):
            self._state = True
            print(f"Simulated LED on pin {self.pin} is ON")

        def off(self):
            self._state = False
            print(f"Simulated LED on pin {self.pin} is OFF")

        @property
        def is_lit(self):
            return self._state

    # Assign the simulated LED to the LED variable
    LED = SimulatedLED

    # Define a simulated LED
    class SimulatedLED:
        def __init__(self, pin):
            self.pin = pin
            self._state = False

        def on(self):
            self._state = True
            print(f"Simulated LED on pin {self.pin} is ON")

        def off(self):
            self._state = False
            print(f"Simulated LED on pin {self.pin} is OFF")

        @property
        def is_lit(self):
            return self._state
        

    # Assign the simulated LED to the LED variabledata/merged_global_log.csv
    LED = SimulatedLED

led = LED(6)
led.off()
logging_active=False
startup_sleep=1
print("wait "+str(startup_sleep)+"s for startup..")
time.sleep(startup_sleep)
led.on()
def toggle_led():
    """
    Toggle the state of the LED depending on its current state.
    """
    if led.is_lit:
        led.off()
        print("LED is turned OFF")
    else:
        led.on()
        print("LED is turned ON")

def toggle_flicker_led():
    """
    Toggle the state of the LED depending on its current state.
    """
    if led.is_lit:
        led.off()
        time.sleep(0.1)  # Wait for 0.1 seconds
        led.on()
        time.sleep(0.1)  # Wait for 0.1 seconds
        led.off()
        print("LED is turned OFF")
    else:
        led.on()
        time.sleep(0.1)  # Wait for 0.1 seconds
        led.off()
        time.sleep(0.1)  # Wait for 0.1 seconds
        led.on()
        print("LED is turned ON")
# Function to load the JSON schema from a local file

def flicker_led(n=1):
    """
    Toggle the state of the LED depending on its current state.
    """
    print("flicker led"+str(n))
    i=0
    if led.is_lit:
        while i<=n:
            led.off()
            time.sleep(0.05)  # Wait for 0.1 seconds
            led.on()
            i+=1
        
        print("LED is turned ON")
    else:
        while i<=n:
            led.on()
            time.sleep(0.05)  # Wait for 0.1 seconds
            led.off()
            i+=1
        print("LED is turned OFF")
    time.sleep(0.1)
# Function to load the JSON schema from a local file
flicker_led(1)

def load_json_schema(file_path):
    with open(file_path, 'r') as file:
        schema = json.load(file)
    return schema

# Function to load the JSON schema from a URL
def load_remote_json_schema(url):
    response = requests.get(url)
    schema = response.json()
    return schema

# Function to validate a JSON string against a JSON schema
def validate_json(json_data, schema):
    try:
        # Check if json_data is a string, and if so, parse it into a dictionary
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
        elif not isinstance(json_data, dict):
            raise ValueError("Input JSON data must be a dictionary or a JSON string.")

        validate(instance=json_data, schema=schema)
        print("JSON is valid.")
        return True
    except jsonschema.exceptions.ValidationError as err:
        print("JSON is invalid.")
        print("Validation error message:", err.message)
        return False
    except json.JSONDecodeError as err:
        print("Invalid JSON string.")
        print("Decoding error message:", err.msg)
        return False
    except Exception as err:
        print("An unexpected error occurred.")
        print("Error message:", str(err))
        return False

# URL of the JSON schema
schema_url = "https://code.sbb.ch/projects/PPP_FAHRZEUGDIAGNOSE_ROS/repos/train_data_transmission_format/raw/schema/train-diagnostic-data.schema.json"

# Path to the local JSON schema file
schema_file_path = os.path.join(os.path.dirname(__file__), 'config','train-diagnostic-data.schema.json')

# Load the JSON schema
schema = load_json_schema(schema_file_path)

# Example JSON string to validate
json_string = '''
{
  "header": {
    "fleet": "TrainFleet001",
    "genTime": "2024-12-17T10:00:00Z",
    "Vehicle": "12345678901"
  }
}
'''


max_speed=0.0
config_path = os.path.join(os.path.dirname(__file__), 'config')
data_path = os.path.join(os.path.dirname(__file__), 'data')
telegram_hex=""
try:
    logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'serial_logger.log'), level=logging.DEBUG)
except:
    logging.basicConfig(filename='serial_logger.log', level=logging.DEBUG)


try:
    def print_file_md5():
        file_path = __file__
        with open(file_path, 'rb') as file:
            md5_hash = hashlib.md5()
            while chunk := file.read(4096):
                md5_hash.update(chunk)
        print(f"MD5 hash of file '{file_path}': {md5_hash.hexdigest()}")
        return md5_hash
    md5_hash=print_file_md5()
except Exception as e:
    print("cannot check pem file")

# read Configuration parameter
config = ConfigParser()
config_file = os.path.join(os.path.dirname(__file__), 'config','config.ini')
config_file = os.path.normpath(config_file)
print("config path "+config_file)
config.read(config_file)

mqtt_user_id = config.get('credentials', 'mqtt_username')
mqtt_password = config.get('credentials', 'mqtt_password')
mqtt_broker_intern = config.get('credentials', 'mqtt_broker_intern')
mqtt_broker_outside = config.get('credentials', 'mqtt_broker_outside')
default_vehicle_id = config.get('credentials', 'default_vehicle_id')
sim_pin = config.get('credentials', 'sim_pin')

# FTP server configuration

ftp_host="ftp.carres.ch"
ftp_user="carresch"
ftp_password="9948Wragabrr%"

# read public Configuration parameter
config_p = ConfigParser()
public_config_path = os.path.join(os.path.dirname(__file__), 'config','public_config.ini')
public_config_file = os.path.normpath(public_config_path)
print("config path "+public_config_file)
config_p.read(public_config_file)
conf_dbr_odo = float(config_p.get('recording', 'dbr_odo'))
conf_min_time_odo = float(config_p.get('recording', 'min_time_odo'))
conf_min_time_novram = float(config_p.get('recording', 'min_time_novram'))
conf_status_period = float(config_p.get('recording', 'status_period'))
#uic range 94856500666 - 94856500669        
#uuid       
try:
    UIC_VehicleID=config.get('credentials', 'vehicle_id')
except:
    print("no vehicle id in config file, use default "+str(default_vehicle_id))
    UIC_VehicleID=default_vehicle_id

def get_speed_from_nmea(nmea_sentence):
        parts = nmea_sentence.split(',')
        if parts[0] == '$GPRMC' and parts[2] == 'A':
            try:
                speed_knots = float(parts[7])  # Speed over ground in knots
                speed_kmh = speed_knots * 1.852  # Convert knots to km/h
                return speed_kmh
            except (ValueError, IndexError):
                print("Error parsing speed from NMEA sentence.")
                return 0.0
        else:
            return 0.0

def get_position_from_nmea(nmea_sentence):
    parts = nmea_sentence.split(',')
    if parts[0] == '$GPRMC' and parts[2] == 'A':
        latitude = float(parts[3][:2]) + float(parts[3][2:]) / 60
        if parts[4] == 'S':
            latitude = -latitude
        longitude = float(parts[5][:3]) + float(parts[5][3:]) / 60
        if parts[6] == 'W':
            longitude = -longitude

        
        google_maps_link = f'https://www.google.com/maps/search/?api=1&query={latitude},{longitude}'

        return latitude, longitude, google_maps_link
    else:
        return None , None
        
def update_data(self, message):
    try:
        msg = json.loads(message)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        print(f"Message payload: {message}")
        return
        
def get_disk_serial_number():
    if os.name == 'nt':  # Windows
        try:
            result = subprocess.check_output("wmic diskdrive get SerialNumber", shell=True)
            serial_number = result.decode().split("\n")[1].strip()
            return serial_number
        except Exception as e:
            print(f"Error retrieving disk serial number: {e}")
            return None
    else:  # Linux/Unix
        try:
            result = os.popen("lsblk -o SERIAL").read().split("\n")
            serial_number = result[1].strip() if len(result) > 1 else None
            return serial_number
        except Exception as e:
            print(f"Error retrieving disk serial number: {e}")
            return None

my_uuid = get_disk_serial_number()

print("UUID: " + str(my_uuid))
# Load vehicle_list.json and check for vehicle_id matching my_uuid
vehicle_list_path = os.path.join(os.path.dirname(__file__), 'config', 'vehicle_list.json')


'''
TO BE REMOVED
'''
def reboot_linux_after_delay(delay_minutes=15):
    def reboot():
        if os.name == 'posix':
            print(f"Rebooting system in {delay_minutes} minutes...")
            time.sleep(delay_minutes * 60)
            print("Rebooting now.")
            os.system('sudo reboot')
    t = threading.Thread(target=reboot, daemon=True)
    t.start()

reboot_linux_after_delay(120)

'''
TO BE REMOVED
'''


try:
    with open(vehicle_list_path, 'r') as file:
        vehicle_list = json.load(file)
    print(vehicle_list)
    # Check if my_uuid exists in the vehicle_list
    for vehicle in vehicle_list.get('vehicles', []):
        print(vehicle.get('device_uuid')+ " "+str(my_uuid)+" "+str(vehicle.get('device_uuid') == str(my_uuid)))
        if vehicle.get('device_uuid') == str(my_uuid):
            UIC_VehicleID = vehicle.get('vehicle_id')  # Assign the id if found
            print(f"Vehicle ID found: {UIC_VehicleID}")
            flicker_led(1)
            break
    else:
        print(f"No matching vehicle_id found for UUID: {my_uuid}")
        print(f"assigned Vehicle ID: {UIC_VehicleID}")
        flicker_led(6)
        
except FileNotFoundError:
    print(f"Vehicle list file not found at {vehicle_list_path}")
except json.JSONDecodeError as e:
    print(f"Error decoding JSON file: {e}")
except Exception as e:
    print(f"An error occurred while loading the vehicle list: {e}")
# MQTT Configuration

mqtt_port_outside = 8885               #8885
mqtt_port_intern = 8883
mqtt_topic_publish = UIC_VehicleID+"/ETCS"
mqtt_topic_raw_odo=mqtt_topic_publish+"/raw_odo"
mqtt_topic_odo=mqtt_topic_publish+"/odo"
mqtt_topic_logger=mqtt_topic_publish+"/logger"
mqtt_topic_debug=mqtt_topic_publish+"/debug"
mqtt_topic_novram=mqtt_topic_publish+"/novram"
mqtt_topic_subscribe = "+/ETCS/#"
mqtt_client_id_fahrzeug = UIC_VehicleID+"-ETCS-inte" #<uic>-ETCS-inte
mqtt_topic_test_publish=mqtt_topic_publish+"/test"
mqtt_client_id_intern = "s-ETCS-consumer-inte-"+str(my_uuid) #s-ETCS-consumer-inte-<uuid>
mqtt_user_id="ETCS_PoC-inte"

mqtt_pem_file_intern = os.path.join(config_path, "SBB-CL-B-Issuing-CA.pem")
mqtt_pem_file_intern = os.path.normpath(mqtt_pem_file_intern)

mqtt_pem_file_outside = os.path.join(config_path, "SwissSign RSA TLS DV ICA 2022 - 1.pem")
mqtt_pem_file_outside = os.path.normpath(mqtt_pem_file_outside)

#mqtt_pem_file_outside = config_path +"SwissSign RSA TLS DV ICA 2022 - 1.pem" #C:\Users\u242381\OneDrive - SBB\Dokumente\visual code\repos\serial logger
#name       #port       #datei                                  #valid
#SBB signed	8883, 8886	SBB-CL-B-Issuing-CA.pem	                27.09.2027
#SwissSign	8885, 8887	SwissSign RSA TLS DV ICA 2022 - 1.pem	29.06.2036
class OperationalStatus(Enum):
    OTHER = "other"
    UNKNOWN = "unknown"
    OPERATIONAL = "operational"
    PARKED = "parked"
    MAINTENANCE = "maintenance"
    TRAINING = "training"
    SIMULATED = "simulated"
    SCHLUMMERBETRIEB = "schlummerbetrieb"
    EOPTA = "EoptA"
    SHUTDOWN = "shutdown"

intern=False

# Assign the state Simulated to OPERATIONAL_STATUS
OPERATIONAL_STATUS = OperationalStatus.SIMULATED.value
serial_by_path_dir=[]
if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check. and set to inern")
        intern=True #if starting on windows we assume we are in the intern network
        serial_by_path_dir = "COM3"  # Set to a default COM port for Windows
        modem_port="COM3"
else:
        modem_port="/dev/serial/by-id/usb-SimTech__Incorporated_SimTech__Incorporated_0123456789ABCDEF-if04-port0"    
        OPERATIONAL_STATUS = OperationalStatus.OPERATIONAL.value        
        intern=False # if starting on linux we assume we are in the outside network



def check_network_registration(serial_port, baud_rate="115200", timeout=30):
    """
    Check if the module is registered on the network.

    :param ser: The serial connection object.
    :return: True if registered, False otherwise.
    """
    print("SIM network registration..")
    ser = serial.Serial(serial_port, baudrate=baud_rate, timeout=1)
    start_time = time.time()

        # Wait for the module to respond to AT commands
    while time.time() - start_time < timeout:
        ser.write(b'AT+COPS?\r\n')
        response = ser.read(100).decode().strip()
        print(response)
        if "+COPS:" in response and "0,0" in response:  # "0,0" indicates registered on the network
            print("Module is registered on the network.")
            return True
        time.sleep(2)  # Wait before retrying
        
    print("Module is not registered on the network.")
    return False

def check_4G_startup(serial_port, baud_rate="115200", timeout=30):
    print("4G module starting up..")
    """
    Function to check if the SIM7600G-H module has completely started up.

    :param serial_port: The serial port where the SIM7600G-H is connected (e.g., '/dev/ttyS0' or 'COM3').
    :param baud_rate: The baud rate for communication (e.g., 115200).
    :param timeout: Maximum time (in seconds) to wait for the module to start up.
    :return: True if the module is ready, False otherwise.
    """
    if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check.")
        return True

    try:
        # Open the serial connection
        ser = serial.Serial(serial_port, baudrate=baud_rate, timeout=1)
        start_time = time.time()

        # Wait for the module to respond to AT commands
        while time.time() - start_time < timeout:
            ser.write(b'AT\r\n')  # Send basic AT command
            response = ser.read(100).decode().strip()
            print(response)
            
            
            if ("OK" in response):
                print("Module is ready.")
                ser.close()
                return True

            time.sleep(2)  # Wait before retrying

        # If the loop ends without finding "OK"
        print("Module did not start within the timeout period.")
        ser.close()
        return False

    except Exception as e:
        print("Error:", e)
        ser.close()
        return False
    
def enter_sim_pin(serial_port,sim_pin):
    baud_rate="115200"
    """
    Function to enter the SIM PIN for the SIM7600G-H 4G HAT.

    :param serial_port: The serial port where the SIM7600G-H is connected (e.g., '/dev/ttyS0' or 'COM3').
    :param baud_rate: The baud rate for communication (e.g., 115200).
    :param sim_pin: The SIM PIN as a string (e.g., "1234").
    :return: True if the PIN was accepted, False otherwise.
    """
    try:
        # Open the serial connection
        ser = serial.Serial(serial_port, baudrate=baud_rate, timeout=5)
        time.sleep(1)  # Wait for the module to initialize

        # Check if the SIM card requires a PIN
        ser.write(b'AT+CPIN?\r\n')
        response = ser.read(100).decode().strip()
        print(response)
        if "SIM PIN" not in response:
            print("No SIM PIN required or already unlocked. "+str(response))
            ser.close()
            return True

        # Enter the SIM PIN
        ser.write(f'AT+CPIN="{sim_pin}"\r\n'.encode())
        response = ser.read(100).decode().strip()
        if "OK" in response:
            print("SIM PIN accepted.")
            ser.close()
            return True
        else:
            print("Failed to enter SIM PIN. Response:", response)
            ser.close()
            return False

    except Exception as e:
        print("Error:", e)
        return False
def get_signal_quality(serial_port, baud_rate="115200"):
    """
    Function to get the signal quality using the AT+CSQ command.

    :param serial_port: The serial port where the module is connected (e.g., '/dev/ttyS0' or 'COM3').
    :param baud_rate: The baud rate for communication (e.g., 115200).
    :return: A tuple (signal_strength, signal_quality) or None if the command fails.
    """
    try:
        ser = serial.Serial(serial_port, baudrate=baud_rate, timeout=1)
        ser.write(b'AT+CSQ\r\n')  # Send the AT+CSQ command
        response = ser.read(100).decode().strip()
        print("AT+CSQ Response:", response)
        for response in response.splitlines():
            if "+CSQ:" in response:
                # Extract the signal strength and quality values
                print("response to get signal str"+str(response))
                match = re.search(r"\+CSQ: (\d+),(\d+)", response)
                if match:
                    signal_strength = int(match.group(1))
                    signal_quality = int(match.group(2))
                    #print("Signal Strength:", signal_strength)
                    #print("Signal Quality:", signal_quality)
                    ser.close()
                    return signal_strength, signal_quality
                else:
                    print("Failed to parse signal strength and quality from response.")
        else:
            print("No +CSQ: found in response.")
        
        ser.close()
        return (-1,-1)  # Return None if parsing fails or response is invalid

    except Exception as e:
        print("Error:", e)
        return (-1,-1)  # Return None in case of an exception

        ser.close()
        print("Failed to retrieve signal quality.")
        return (-1,-1)#(-1,-1)



def Setup4G(modem_port, flicker_led, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin):
    if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check.")
    else:
            
        print("Serial port 4")
        flicker_led(2)
        ready_4G= check_4G_startup(modem_port)    
        print("wait for 4G to be ready..")
        flicker_led(3)
        while  not ready_4G:
            ready_4G= check_4G_startup(modem_port)
            flicker_led(4)
            time.sleep(1)
        
        print("check PIN")
        enter_sim_pin(modem_port, sim_pin)
        flicker_led(5)
        registerd = check_network_registration(modem_port)
        flicker_led(6)


Setup4G(modem_port, flicker_led, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin)
def is_modem_connected(serial_port="", baud_rate="115200"):
    """
    Check if the modem is connected and responding to AT commands.

    :param serial_port: The serial port where the modem is connected.
    :param baud_rate: The baud rate for communication.
    :return: True if the modem is connected, False otherwise.
    """
    if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check.")
    else:
        serial_port="/dev/serial/by-id/usb-SimTech__Incorporated_SimTech__Incorporated_0123456789ABCDEF-if04-port0"    

    try:
        ser = serial.Serial(serial_port, baudrate=baud_rate, timeout=1)
        ser.write(b'AT\r\n')  # Send basic AT command
        response = ser.read(100).decode().strip()
        ser.close()
        if "OK" in response:
            print("Modem is connected and responding.")
            return True
        else:
            print("Modem is not responding.")
            return False
    except Exception as e:
        print(f"Error checking modem connection: {e}")
        return False


def reconnect_modem(serial_port, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin):
    """
    Attempt to reconnect the modem by reinitializing it.

    :param serial_port: The serial port where the modem is connected.
    :param sim_pin: The SIM PIN for the modem.
    :param check_network_registration: Function to check network registration.
    :param check_4G_startup: Function to check if the modem has started up.
    :param enter_sim_pin: Function to enter the SIM PIN.
    """
    if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check.")
    else:
        serial_port="/dev/serial/by-id/usb-SimTech__Incorporated_SimTech__Incorporated_0123456789ABCDEF-if04-port0"    

    print("Attempting to reconnect the modem...")
    if not check_4G_startup(serial_port):
        print("Modem startup failed. Retrying...")
        time.sleep(5)
        return reconnect_modem(serial_port, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin)

    if not enter_sim_pin(serial_port, sim_pin):
        print("Failed to enter SIM PIN. Retrying...")
        time.sleep(5)
        return reconnect_modem(serial_port, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin)

    if not check_network_registration(serial_port):
        print("Network registration failed. Retrying...")
        time.sleep(5)
        return reconnect_modem(serial_port, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin)

    print("Modem reconnected successfully.")

if intern:
    MQTT_BROKER=mqtt_broker_intern
else:
    MQTT_BROKER=mqtt_broker_outside

def check_pem_file(pem_file):
    try:
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=pem_file)
        return True
    except ssl.SSLError as e:
        print(f"Error loading PEM file: {str(e)}")
        return False

is_valid = check_pem_file(mqtt_pem_file_outside)
if is_valid:
    print(mqtt_pem_file_outside+" PEM file is valid!")

is_valid = check_pem_file(mqtt_pem_file_intern)

if is_valid:
    print(mqtt_pem_file_intern+" PEM file is valid!")

def on_connect(client, userdata, flags, rc):
    print("Connection Return code:", rc)
    if rc == 0:
        print("Connected to MQTT broker successfully.")
        flicker_led(10)
        #client.subscribe(mqtt_topic_subscribe)
    elif rc == 1:
        print("Connection refused: incorrect protocol version.")
    elif rc == 2:
        print("Connection refused: invalid client identifier.")
    elif rc == 3:
        print("Connection refused: server unavailable.")
    elif rc == 4:
        print("Connection refused: bad username or password.")
    elif rc == 5:
        print("Connection refused: not authorized.")
    else:
        print("Connection refused: unknown reason. Return code:", rc)       

def on_publish(client, userdata, mid):
    print("mqtt message published ")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unerwartete Trennung vom MQTT-Broker "+str(rc))

def on_message(client, userdata, msg):
    print("Neue Nachricht empfangen: " + msg.topic + " " + str(msg.payload))

def replace_none_with_null(data):
    """
    Replace all occurrences of None with 'null' in a JSON object while maintaining JSON format.
    Ensure the output is valid JSON.

    :param data: The JSON object to process.
    :return: The modified JSON object with None replaced by 'null'.

    """
    step=0
    try:    
        if isinstance(data, dict):
            step=1
            return {key: replace_none_with_null(value) for key, value in data.items()}
        elif isinstance(data, list):
            step=2
            return [replace_none_with_null(item) for item in data]
        elif data is None:
            step=3
            return None  # Keep it as None, as JSON serialization will handle it correctly
        elif isinstance(data, (str, int, float, bool)):
            step=4
            return data
        else:
            step=5
            return str(data)
    except Exception as e:
        print(f"Error replacing None with null: {e}")
        #return str(data)
        raise ValueError(f"Error in step "+str(step)+" replacing None with null. type:"+str(type(data))+" exc: {e} debugdata: "+str(data))

def send_string_to_ftp(ftp_host, ftp_user, ftp_password, data_string, remote_dir, remote_filename):
    data_string = str(data_string)
    try:
        # Connect to the FTP server
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login(ftp_user, ftp_password)

            # Attempt to change to the specified directory
            try:
                ftp.cwd(remote_dir)
            except ftplib.error_perm:
                # If the directory does not exist, create it
                ftp.mkd(remote_dir)
                ftp.cwd(remote_dir)  # Change to the new directory

            # Use a BytesIO buffer to simulate a file
            with io.BytesIO(data_string.encode('utf-8')) as file_buffer:
                # Store the file in the FTP server with a .tmp extension
                tmp_filename = f"{remote_filename}.tmp"
                ftp.storbinary(f'STOR {tmp_filename}', file_buffer)

            # Attempt to rename the file to the desired filename
            try:
                # Check if the target filename already exists; if so, skip renaming
                if not remote_filename in ftp.nlst():                   
     
                    ftp.rename(tmp_filename, remote_filename)
                    print(f'Successfully sent data to {remote_dir}/{remote_filename} on FTP server.')
                else:
                    # If renaming fails, find a unique filename
                    base, ext = os.path.splitext(remote_filename)
                    counter = 1
                    new_filename = f"{base}({counter}){ext}"
                    
                    while new_filename in ftp.nlst() and counter < 1000:  # Limit to 1000 attempts
                        counter += 1
                        new_filename = f"{base}({counter}){ext}"
                
                    ftp.rename(tmp_filename, new_filename)
                    print(f'Successfully renamed to {remote_dir}/{new_filename} on FTP server.')
                    
            except ftplib.error_perm:
                print(f'Failed to rename {tmp_filename} to {new_filename}. It may already exist.')
                

    except ftplib.all_errors as e:
        print(f'FTP error: {e}')


def ensure_json_serializable(data):
    """
    Ensure the data is JSON serializable by converting it to a JSON string and back.

    :param data: The data to validate.
    :return: A JSON-serializable object.
    :raises ValueError: If the data cannot be serialized to JSON.
    """
    try:
        json_string = json.dumps(data)
        return json.loads(json_string)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Data is not JSON serializable: {e}")
        
def send_json_message(topic, json_message_i,message_counter):
    try:    
        json_message=""
        json_message_i = add_element(json_message_i, "seq", "Sequence Number", message_counter)
        
        if os.name == 'nt':     
            print("Windows OS detected.send json message-----------------------------")
        '''        
            print("Windows OS detected. simulate data")
            real_data=json_message_i
            json_message_i= {'header': {'fleet': 'ICN', 'genTime': '2025-08-04T12:28:39.103890Z', 'sendVehicle': '94856500668'}, 'opdata': [{'vehicleUIC': '94856500668', 'vehicleType': 'ICN', 'specification':'0.0.75', 'time': '2025-08-04T12:28:39.103890Z', 'operationalStatus': 'operational', 'data': [{'key': 'lgr_cpu_temp','name': 'Logger CPU Temperature', 'value': 64.8}, {'key': 'lgr_max_speed', 'name': 'Maximum Speed', 'value': 0},{'key': 'lgr_gps', 'name': 'position', 'value': '$GPRMC,,V,,,,,,,,,,N*53'}, {'key': 'source', 'name': 'source','value': 'default'}, {'key': 'lgr_lat', 'name': 'Latitude', 'value': 0}, {'key': 'lgr_lon', 'name': 'Longitude','value': 0}, {'key': 'lgr_gps_speed', 'name': 'GPS Speed', 'value': 0.0}, {'key': 'lgr_signal_strength', 'name':'Signal Strength', 'value': 16}, {'key': 'lgr_signal_quality', 'name': 'Signal Quality', 'value': 99}, {'key':'lgr_cpu_load', 'name': 'CPU Load', 'value': 0.62255859375}, {'key': 'lgr_memory_load', 'name': 'Memory Load','value': 7.0}, {'key': 'lgr_disk_space', 'name': 'Disk Space', 'value': 28.7}, {'key': 'seq', 'name': 'SequenceNumber', 'value': 27}]}]}
            json_message_i= {'header': {'fleet': 'ICN', 'genTime': '2025-08-04T14:00:17.446717Z', 'sendVehicle': '94856500668'}, 'opdata':[{'vehicleUIC': '94856500668', 'vehicleType': 'ICN', 'specification': '0.0.77', 'time': '2025-08-04T14:00:17.446717Z','operationalStatus': 'operational', 'data': [{'key': 'lgr_cpu_temp', 'name': 'Logger CPU Temperature', 'value': None},{'key': 'lgr_max_speed', 'name': 'Maximum Speed', 'value': 0}, {'key': 'lgr_gps', 'name': 'position', 'value':'$GPRMC,,V,,,,,,,,,,N*53'}, {'key': 'source', 'name': 'source', 'value': 'default'}, {'key': 'lgr_lat', 'name':'Latitude', 'value': 0}, {'key': 'lgr_lon', 'name': 'Longitude', 'value': 0}, {'key': 'lgr_gps_speed', 'name': 'GPS Speed', 'value': 0.0}, {'key': 'lgr_signal_strength', 'name': 'Signal Strength', 'value': 25}, {'key':'lgr_signal_quality', 'name': 'Signal Quality', 'value': 99}, {'key': 'lgr_cpu_load', 'name': 'CPU Load', 'value':1.45263671875}, {'key': 'lgr_memory_load', 'name': 'Memory Load', 'value': 6.4}, {'key': 'lgr_disk_space', 'name':'Disk Space', 'value': 29.1}, {'key': 'seq', 'name': 'Sequence Number', 'value': 0}]}]}
        '''
        #json_message=replace_none_with_null(json_message_i)
        json_message=json_message_i
      
        
    except Exception as e:
        send_debug_message("Error send json message: " + str(e))
        send_debug_message("type of json_message_i: " + str(type(json_message_i)))
        json_message=json_message_i    
    try:
        if validate_json(json_message,schema):
            message=json.dumps(json_message)
            client.publish(topic, message)
            print("mqtt message "+str(message))
        
            if message_counter >= 10000:
                message_counter=0
            else:
                message_counter += 1
        else:
            print("message not valid "+str(json_message))
        
    except Exception as e:
        print("fail to send "+str(json_message) + str(e))
    
    toggle_led()
    return message_counter

def send_text_message(topic, message):
    try:        
        client.publish(topic, message)
        print("send "+str(message))
    except Exception as e:
        print("fail to send "+str(message) + str(e))

def send_debug_message(message):
    message=" time:"+str(datetime.now())+" uuid:"+str(my_uuid)+" vehicle: "+str(UIC_VehicleID)+" version: "+str(version)+" message: "+str(message)+" .end."
    l=10000
    if len(message) > l: 
        message = message[:l]+"[...]"+str(len(message)-l)  # Limit message length to 1000 characters
    #message = message.replace('\n',
    #message=message[:1000]  # Limit message length to 1000 characters
    print("send debug message "+str(message))
    send_text_message(mqtt_topic_debug, message)

def wait_till_online(n_max=100000):
    waiting =True
    print("wait until we are online...")
    while waiting:
        counter =0
        t=1000
        toggle_flicker_led() 
        try:
            if os.name == 'nt':
                print("ping failed (only possible on linux)")
                waiting=False
                t=0
            else:  
                
                t = os.system('ping -c 1 8.8.8.8')
                
        except Exception as e:
            print("ping failed (only possible on linux)"+str(e))
            waiting=False
            t=0
           
        if t < 1:
            waiting=False
            print("internet available")
        else:
            counter +=1
            time.sleep(1) #wait between two pings only relevant during startup
            if counter == n_max: # this will prevent an never ending loop, set to the number of tries you think it will require
                waiting = False
    led.on()

wait_till_online(n_max=100000)

def setup_mqtt_connection(intern):
    try:
        if intern:
            client = mqtt.Client(client_id=mqtt_client_id_intern)    
        else :
            client = mqtt.Client(client_id=mqtt_client_id_fahrzeug)    
    except: 
        print("mqtt connection failed "+ str(client._client_id))
   

    # MQTT-Broker mit TLS-Verbindung konfigurieren
    try:
        tls_version=ssl.PROTOCOL_TLSv1_2
        if intern:
            client.tls_set(ca_certs=mqtt_pem_file_intern, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=tls_version, ciphers=None)    
        else :
            client.tls_set(ca_certs=mqtt_pem_file_outside, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=tls_version, ciphers=None)
    except: 
        print("TLS failed")

    try:
        client.username_pw_set(username=mqtt_user_id, password=mqtt_password)
        client.on_connect = on_connect
        client.on_publish = on_publish
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        timeout=60
        if intern:
            client.connect(mqtt_broker_intern, mqtt_port_intern, timeout)
        else:
            client.connect(mqtt_broker_outside, mqtt_port_outside, timeout)
        client.loop_start()
        # Testnachricht senden
        message= str(my_uuid)+"logger started mqtt connection"+" hash"+str(md5_hash)
        send_text_message(mqtt_topic_debug,message)
        print("mqtt has been setup")
    except Exception as e:
        print("not able to connect to mqtt broker ")
        print(f"Exception: {e}")
        print(f"Client ID: {str(client._client_id)}")
        print(f"Username: {mqtt_user_id}")
        print(f"Password: {mqtt_password[:3]}...")
        print(f"Broker: {mqtt_broker_intern if intern else mqtt_broker_outside}")
        print(f"Port: {mqtt_port_intern if intern else mqtt_port_outside}")
        print(f"TSL version: {str(tls_version)}")
        print(f"Timeout: {timeout}")
        time.sleep(2)
    return client

client=setup_mqtt_connection(intern)
#print("sleep 30s..")
#time.sleep(30)



def list_serial_interfaces_linux(adapter_name_start='usb-1a86_',directory = '/dev/serial/by-id/'):
    
    ports=[]
    if os.path.exists(directory) and os.path.isdir(directory):
        files = os.listdir(directory)
        if files:
            print("Available serial interfaces:")
            for filename in files:
                if filename.startswith(adapter_name_start):
                    ports.append(os.path.join(directory, filename))
                    
        else:
            print("No serial interfaces found.")
       
        return ports



def find_serial_adapters(target_description, directory = '/dev/serial/by-id/'):
    ports=[]
    sers=[]
    if os.name != 'nt':
        print("not windows")
        port_list=list_serial_interfaces_linux(target_description, directory)
        try:
            for port in port_list:
                sers.append(serial.Serial(port, 115200, timeout=1))
            print(str(sers))
            fail=False
            
        except (OSError, serial.SerialException):         
            print("no linux serial addapter")
    else:
        port_list = serial.tools.list_ports.comports()
        print("look for serial port..")
        for port in port_list:
            print(port.description)
            if target_description in port.description:
                try:
                    ser = serial.Serial(port.device,timeout=0.1)                    
                    ser.close()
                    ports.append(port.device)
                except (OSError, serial.SerialException):
                    print("cannot connect to "+str(port.description))
                    pass
        for port in ports:
            sers.append(serial.Serial(port, 115200, timeout=1))
    return sers

sers=[]
sers2=[]
fail=True
while fail:
        try:
            target_description = "USB-SERIAL CH340"  # Beispielhafte Beschreibung
            target_description = 'usb-1a86_'  # adapter nme by id unter linux
            sers = find_serial_adapters(target_description)
            sers2 = find_serial_adapters("-", directory = '/dev/serial/by-path/')
            fail= False            
            
        except Exception as e:
            fail= True
            message= str(my_uuid)+" serial port  not ready"+ str(e)
            send_text_message(mqtt_topic_debug,message)
            #time.sleep(5)



print("serial connection opened")
def get_temp():
    if os.name == 'nt':
        temp=0
        return temp
    else:
        res = os.popen('vcgencmd measure_temp').readline()
        temp_str = res.replace("temp=", "").replace("'C\n", "").strip()
        try:
            temp = float(temp_str)
            return temp
        except ValueError:
            print("Invalid temperature value:", temp_str)
            return -0.666

def get_cpu_load():
    """
    Returns the CPU load percentage.
    Checks if the system is Windows or Linux and retrieves the CPU load accordingly.
    """
    try:
        if os.name == 'nt':  # Windows
            return psutil.cpu_percent(interval=1)
        else:  # Linux
            load1, load5, load15 = os.getloadavg()
            cpu_count = os.cpu_count()
            return (load1 / cpu_count) * 100
    except Exception as e:
        print(f"Error retrieving CPU load: {e}")
        return -1

def get_memory_load():
    """
    Returns the memory usage percentage.
    Uses psutil to retrieve memory usage statistics.
    """
    try:
        memory = psutil.virtual_memory()
        return memory.percent
    except Exception as e:
        print(f"Error retrieving memory load: {e}")
        return -1

def get_disk_space():
    """
    Returns the disk usage percentage of the root directory.
    Uses psutil to retrieve disk usage statistics.
    """
    try:
        disk = psutil.disk_usage('/')
        return disk.percent
    except Exception as e:
        print(f"Error retrieving disk space usage: {e}")
        return -1
   

def temp_check():
    temp = get_temp()    
    message=("logger CPU temp "+str(int(temp)))
    send_text_message(mqtt_topic_debug, message)
    
    print(f"Current CPU temperature: {temp}°C")
    if temp > 80.0:  # Threshold for warning
        print("Warning: CPU temperature is too high! wait..")
        while temp > 82.0:
            temp = get_temp()
            message="logger CPU too high, having a break.."
            send_text_message(mqtt_topic_debug, message)
            time.sleep(1)  # Log every 1 seconds 
            message="..continue"
            send_text_message(mqtt_topic_debug, message)


def create_JSON_object(timestamp,UIC_VehicleID,cpu_temp,max_speed,position="",source="default"):
    # Create the JSON object
    json_data = {
        "header": {
            "fleet": "ICN",
            "genTime": timestamp,
            "sendVehicle": UIC_VehicleID
        },
        "opdata": [
            {
                "vehicleUIC": UIC_VehicleID,
                "vehicleType": "ICN",
                "specification": version,
                "time": timestamp,
                "operationalStatus": OPERATIONAL_STATUS,
                "data": [
                    {
                        "key": "lgr_cpu_temp",
                        "name": "Logger CPU Temperature",
                        "value": cpu_temp 
                    },
                    {
                        "key": "lgr_max_speed",
                        "name": "Maximum Speed",
                        "value": max_speed
                    },
                    {
                        "key": "lgr_gps",
                        "name": "position",
                        "value": position
                    },
                    {
                        "key": "source",
                        "name": "source",
                        "value": source
                    }
                ]
            }
        ]
    }
    return json_data

def create_raw_JSON_object(timestamp,UIC_VehicleID,raw_data,position="",source="default"):
    # Create the JSON object
    json_data = {
        "header": {
            "fleet": "ICN",
            "genTime": timestamp,
            "sendVehicle": UIC_VehicleID
        },
        "opdata": [
            {
                "vehicleUIC": UIC_VehicleID,
                "vehicleType": "ICN",
                "specification": version,
                "time": timestamp,
                "operationalStatus": "simulated",
                "data": [
                    {
                        "key": "raw_frame",
                        "name": "RAW FRAME",
                        "value": raw_data
                    },
                    {
                        "key": "lgr_gps",
                        "name": "position",
                        "value": position
                    },
                    {
                        "key": "source",
                        "name": "source",
                        "value": source
                    }
                ]
            }
        ]
    }
    return json_data

def add_element(json_data, key, name, value):
    # Work on a copy to avoid mutating the caller's object
    try:
        json_data = copy.deepcopy(json_data)
    except Exception:
        # Fallback to JSON round-trip if deepcopy fails
        try:
            json_data = json.loads(json.dumps(json_data))
        except Exception as e:
            raise ValueError(f"Unable to copy json_data: {e}")

    # Ensure the expected structure exists: opdata -> list -> [0] -> data -> list
    #if not isinstance(json_data, dict):
    #    raise ValueError("json_data must be a dict")

    #if 'opdata' not in json_data or not isinstance(json_data['opdata'], list) or len(json_data['opdata']) == 0:
    #   json_data.setdefault('opdata', [{}])

    #if not isinstance(json_data['opdata'][0], dict):
    #    json_data['opdata'][0] = {}

    #if 'data' not in json_data['opdata'][0] or not isinstance(json_data['opdata'][0]['data'], list):
    #    json_data['opdata'][0]['data'] = []
    # Create the new data element
    new_element = {
        "key": key,
        "name": name,
        "value": value
    }
    
    # Add the new element to the data array in the opdata section
    json_data['opdata'][0]['data'].append(new_element)
    return json_data

def add_odo_frame(json_data, parsed_frame):
    for key, value in parsed_frame.items():
        json_data = add_element(json_data, key, key.replace('_', ' ').title(), value)
    return json_data

def extract_error_message(line):
    # Remove the starting "!" and ending "</e>"
    line = line.strip("! </e>")
    
    # Split the line by whitespace
    parts = line.split()
    try:
        if len(parts) < 2:
            return "Malformed error message."
            error_name = "Error name not found."
        else:
        # Retrieve the error name (which is the second part)
            error_name = parts[1]
    except IndexError:
        return "Malformed error message."        
    
    return error_name

def parse_novram_objects(novram_object):
    '''
     "! <e>  1485 ODO_CHANNEL0_SS1_NOT_AVAILABLE_ERROR TRUE   54633227</e>\r\n! <e>  1501 ODO_CHANNEL1_SS1_NOT_AVAILABLE_ERROR TRUE   54633234</e>\r\n! <e>  1517 ODO_CHANNEL2_SS1_NOT_AVAILABLE_ERROR TRUE   54633241</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2344 ODO_CHANNEL0_SS1_AVAILABLE_ERROR TRUE   54633567</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2345 ODO_CHANNEL1_SS1_AVAILABLE_ERROR TRUE   54633576</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2346 ODO_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   54633585</e>\r\n0"

    '''
    '''
    extract single error messages from a cluster of novram messages and retreive info from the error_list
    ! <e>  1485 ODO_CHANNEL0_SS1_NOT_AVAILABLE_ERROR TRUE   85675961</e>
    ! <e>  1501 ODO_CHANNEL1_SS1_NOT_AVAILABLE_ERROR TRUE   85675968</e>
    ! <e>  1517 ODO_CHANNEL2_SS1_NOT_AVAILABLE_ERROR TRUE   85675975</e>
    SENSOR IN REVALIDATION
    ! <e>  2344 ODO_CHANNEL0_SS1_AVAILABLE_ERROR TRUE   85676300</e>
    SENSOR IN REVALIDATION
    ! <e>  2345 ODO_CHANNEL1_SS1_AVAILABLE_ERROR TRUE   85676309</e>
    SENSOR IN REVALIDATION
    ! <e>  2346 ODO_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   85676317</e>
    0
    '''
    if 'O_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   11963886' in novram_object:
        print("Debug: Found O_CHANNEL2_SS1_AVAILABLE_ERROR in novram_object") 
    #novram_object="! <e>  1485 ODO_CHANNEL0_SS1_NOT_AVAILABLE_ERROR TRUE   54633227</e>\r\n! <e>  1501 ODO_CHANNEL1_SS1_NOT_AVAILABLE_ERROR TRUE   54633234</e>\r\n! <e>  1517 ODO_CHANNEL2_SS1_NOT_AVAILABLE_ERROR TRUE   54633241</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2344 ODO_CHANNEL0_SS1_AVAILABLE_ERROR TRUE   54633567</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2345 ODO_CHANNEL1_SS1_AVAILABLE_ERROR TRUE   54633576</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2346 ODO_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   54633585</e>\r\n0"
    
    novram_objects = novram_object.split('\n')
    # normalize lines and remove empty ones
    novram_objects = [ln.strip() for ln in novram_objects if ln.strip()]

    processed = []
    for line in novram_objects:
        # if the line contains opening or closing <e> tags (with optional leading '!'), strip those tags
        if re.search(r'!\s*<\s*e\s*>', line, flags=re.IGNORECASE) or re.search(r'<\s*e\s*>', line, flags=re.IGNORECASE) or re.search(r'</\s*e\s*>', line, flags=re.IGNORECASE):
            cleaned = re.sub(r'!\s*', '', line, flags=re.IGNORECASE)                # remove leading '!' if present
            cleaned = re.sub(r'<\s*e\s*>', '', cleaned, flags=re.IGNORECASE)       # remove opening tag
            cleaned = re.sub(r'</\s*e\s*>', '', cleaned, flags=re.IGNORECASE)      # remove closing tag
            cleaned = cleaned.strip()
            if cleaned:
                processed.append(cleaned)
        else:
            # keep line as-is when no tags are present
            processed.append(line)

    novram_objects = processed
    """ novram_objects = [x.strip() for x in novram_objects if x.strip()]  # Remove empty lines and strip whitespace
    # Extract messages between "! <e>" and "</e>" (support multiple occurrences)
    matches = re.findall(r'!\s*<e>\s*(.*?)\s*</e>', novram_object, flags=re.IGNORECASE | re.DOTALL)
    if matches:
        novram_objects = [m.strip() for m in matches]
    else:
        # Fallback: keep lines that start with '! <e>' and strip the tags
        novram_objects = [re.sub(r'^\s*!\s*<e>\s*|\s*</e>\s*$', '', x).strip()
                          for x in novram_objects if x.lstrip().startswith('! <e>')] 
                          """
    
    #print(info)
    return novram_objects
   
#logpath=os.getcwd()+"/data/serial_log.txt"
logpath=data_path+"serial_log_test.txt"
loopcounter=0


position_queue = Queue()
if os.name == 'nt':
    print('no gps on windows')
    time.sleep(0.1)
else:
    message_types  = ['GPRMC', 'GPGGA', 'GPGSA', 'GPGSV']# ['GPRMC']  # Filter for GPRMC messages only  = ['GPRMC', 'GPGGA', 'GPGSA', 'GPGSV']
    gps_process = Process(target=start_gps_collector, args=(position_queue, message_types))
    gps_process.start()
cpu_temp=-1
max_speed=0
gps_data=""
gps_speed=0
last_gps_speed=0
last_speed=0
last_message_time=0
last_basic_message_time=0
last_remaining_message_time=0
last_ex_message_time=0
last_odo_message_time=0
last_novram_message_time=0
skipped_message = 0
unclassified_telegrams = 0
novram_classified=0
odo_icn_classified=0
odo_other_classified=0
deviation_to_send = conf_dbr_odo # only send message if speed changes more than 0.5 km/h
time_threshold = conf_min_time_odo  # time threshold in seconds
message_counter=0 
message_counter_raw=0   
Latitude, Longitude, gm_link=(0,0,"")  
num_satellites=0          
fix_mode=0 
fix_quality=0              
last_basic_message=""
gps_error_count=0
# flush serial buffer on startup
print("flushing serial buffer..")
for ser in sers2:
    if hasattr(ser, 'port'):
        port_path = ser.port
    else:
        port_path = str(ser)
    message= "logger script" +str(version)+" loop starting at "+str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))+" path: "+str(port_path)
    send_text_message(mqtt_topic_debug,message)        

for ser in sers:
    data = b""
    while ser.in_waiting > 0:
        print("flushing serial buffer..")
        data = ser.read(ser.in_waiting)

data = b""
online=True
retry=0
signal_strength, signal_quality=get_signal_quality(modem_port)
message= "logger script " +str(version)+" loop starting at "+str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))+" vehicleID: "+str(UIC_VehicleID)+" signal strength: "+str(signal_strength)+" signal quality: "+str(signal_quality)
send_text_message(mqtt_topic_debug,message)
send_debug_message(message)

send_string_to_ftp(ftp_host, ftp_user, ftp_password, message, "public_html/ETCSLoggerData/",str(UIC_VehicleID)+"_"+str(int(time.time()))+"logger_startup.txt")

try:
    if os.name == 'nt':
        print("Windows OS detected. Skipping serial port by path check.")
    else:
        serial_by_path_dir = "/dev/serial/by-path/"
        list=list_serial_interfaces_linux(serial_by_path_dir)
        for port in list:
            message= str("serial port found by path: "+str(port))  
            send_text_message(mqtt_topic_debug,message)     
except IndexError:
    message= "no serial ports found by path"
    send_text_message(mqtt_topic_debug,message)

try:
    print("waiting for serial messages..")
    ##################################
    # START MAIN LOOP
    ##################################
    while True:
        #temp_check()        
        #with open('data/serial_log.txt', 'a') as log_file:        

            loopcounter+=1

            try:
                now=time.time()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                timestamp_fzdia = datetime.now().isoformat() + "Z"
                cpu_temp=get_temp()
                
                
                #cpu_temp=0

                message=(str(UIC_VehicleID)+" INIT at time: "+str(timestamp)+" cpu temp:"+str(cpu_temp)+ " v_max "+str(max_speed))
                

                #gps_data=""
                if os.name == 'nt':
                    #print('no gps on windows')
                    gps_data="$GPRMC,123815.00,A,4723.945715,N,00803.375235,E,0.0,,250425,0.4,W,A*02"
                    Latitude, Longitude, gm_link=(47.344920,8.550087,"https://www.google.com/maps?q=47.344920,8.550087")
                    gps_speed=0
                else:
                    if not gps_process.is_alive():
                        #print("GPS process has stopped. Restarting...")
                        gps_process = Process(target=start_gps_collector, args=(position_queue,))
                        gps_process.start()

                    if not position_queue.empty():
                        while not position_queue.empty():
                            gps_raw_data = position_queue.get()

                            # Output debug information in a human-readable format
                            if gps_raw_data.startswith('$GPRMC'):
                                gps_data = gps_raw_data
                                #print("Debug Info: GPRMC - Recommended Minimum Specific GPS/Transit Data")
                            elif gps_raw_data.startswith('$GPGGA'):
                                #print("Debug Info: GPGGA - Global Positioning System Fix Data")
                                fields = gps_raw_data.split(',')
                                if len(fields) > 6:
                                    fix_quality = fields[6]
                                    #print(f"Field: Fix Quality Indicator ({fix_quality} = {'Invalid fix' if fix_quality == '0' else 'GPS fix' if fix_quality == '1' else 'Differential GPS fix' if fix_quality == '2' else 'Unknown'})")
                            elif gps_raw_data.startswith('$GPVTG'):
                                print("Debug Info: GPVTG - Track Made Good and Ground Speed")
                            elif gps_raw_data.startswith('$GPGSA'):
                                #print("Debug Info: GPGSA - GPS DOP and Active Satellites")
                                fields = gps_raw_data.split(',')
                                if len(fields) > 2:
                                    fix_mode = fields[2]
                                    #print(f"Field: Fix Mode ({fix_mode} = {'Fix not available' if fix_mode == '1' else '2D fix' if fix_mode == '2' else '3D fix' if fix_mode == '3' else 'Unknown'})")
                            elif gps_raw_data.startswith('$GPGSV'):
                                #print("Debug Info: GPGSV - GPS Satellites in View")
                                fields = gps_raw_data.split(',')
                                if len(fields) > 3:
                                    num_satellites = fields[3]
                                    #print(f"Field: Number of Satellites in View ({num_satellites})")




                        gps_speed = get_speed_from_nmea(gps_data)
                        try:
                            Latitude, Longitude, gm_link = get_position_from_nmea(gps_data)
                            gps_error_count=0
                        except Exception as e:
                            #print("Error parsing GPS data:", e)
                            #Latitude, Longitude, gm_link = (0, 0, "")
                            gps_error_count+=1
                            MAX_GPS_ERROR_COUNT=600
                            send_text_message(mqtt_topic_debug, f"Error parsing GPS data: {e}"+ str(Latitude)+" "+str(Longitude)+" "+str(gm_link)+ " errors till restart: "+str(MAX_GPS_ERROR_COUNT - gps_error_count) +" num sat: "+str(num_satellites)+ " fix mode: "+str(fix_mode)+ " fix quality: "+str(fix_quality))
                            send_text_message(mqtt_topic_debug, f"GPS raw data: "+ str(gps_data))
                            
                            if  gps_error_count > MAX_GPS_ERROR_COUNT:
                                #print("GPS error count exceeded "+MAX_GPS_ERROR_COUNT+", restarting GPS process..")
                                gps_process.terminate()
                                gps_process.join()
                                gps_process = Process(target=start_gps_collector, args=(position_queue, message_types))
                                gps_process.start()
                                gps_error_count=0
                                send_text_message(mqtt_topic_debug, f"GPS error count exceeded "+str(MAX_GPS_ERROR_COUNT)+", restarting GPS process..")
                        

                        #print("Received GPS data in main script:")
                        #print(gps_data)
                        #time.sleep(2)
                    if not online:
                        print("Modem not connected. Attempting to reconnect...")
                        # Attempt to reconnect the modem
                        try:
                            reconnect_modem(modem_port, sim_pin, check_network_registration, check_4G_startup, enter_sim_pin)   
                            online=True
                        except Exception as e: 
                            print("Error during modem reconnection:", e)
                    
                        

                    
                message=""
                message = create_JSON_object(timestamp_fzdia,UIC_VehicleID,cpu_temp,max_speed,gps_data)
                #print("create json message"+str(message))
                try:
                    message=add_element(message, "lgr_lat", "Latitude", Latitude)
                except Exception as e:
                    message=add_element(message, "lgr_lat", "Latitude", "null")
                try:
                    message=add_element(message, "lgr_lon", "Longitude", Longitude)
                except Exception as e:
                    message=add_element(message, "lgr_lon", "Longitude", "null")
                try:
                    message=add_element(message, "lgr_gps_speed", "GPS Speed", gps_speed)   
                except Exception as e:
                    message=add_element(message, "lgr_gps_speed", "GPS Speed", "null")    
                #print("add element to message"+str(message))           
                v_diff=abs(float(gps_speed) - float(last_gps_speed)) if isinstance(gps_speed, (int, float)) and isinstance(last_gps_speed, (int, float)) else 0
                #compile basicmessages 
                if v_diff > deviation_to_send or(time.time() - last_basic_message_time >= conf_status_period):
                    last_gps_speed = gps_speed
                    if os.name != 'nt':
                        try:
                            print("retrieve signal quality for sending")
                            new_signal_strength, new_signal_quality=get_signal_quality(modem_port)
                            if(new_signal_quality==-1 or new_signal_strength==-1):
                                print("signal quality not available")
                            else:
                                signal_quality=new_signal_quality
                                signal_strength=new_signal_strength    
                            message=add_element(message, "lgr_signal_strength", "Signal Strength", signal_strength)                    
                            message=add_element(message, "lgr_signal_quality", "Signal Quality", signal_quality) 
                        except Exception as e:
                            print("Error getting signal quality:", e)
                            signal_strength, signal_quality=(-1, -2) 
                                                  
                    else:
                        signal_strength, signal_quality=(-1, -2)
                    try:
                        cpu_load = get_cpu_load()
                        message = add_element(message, "lgr_cpu_load", "CPU Load", cpu_load)
                    except Exception as e:
                        print("Error getting CPU load:", e)
                        cpu_load = -1
                        message = add_element(message, "lgr_cpu_load", "CPU Load", cpu_load)

                    try:
                        memory_load = get_memory_load()
                        message = add_element(message, "lgr_memory_load", "Memory Load", memory_load)
                    except Exception as e:
                        print("Error getting memory load:", e)
                        memory_load = -1
                        message = add_element(message, "lgr_memory_load", "Memory Load", memory_load)

                    try:
                        disk_space = get_disk_space()
                        message = add_element(message, "lgr_disk_space", "Disk Space", disk_space)
                    except Exception as e:
                        print("Error getting disk space:", e)
                        disk_space = -1
                        message = add_element(message, "lgr_disk_space", "Disk Space", disk_space)
                    try:
                        print("try to send json message..")
                        message_counter=send_json_message(mqtt_topic_logger, message,message_counter)
                        if message_counter % 50 == 0:
                            send_string_to_ftp(ftp_host, ftp_user, ftp_password, message, "public_html/ETCSLoggerData/"+str(UIC_VehicleID)+"/"+day, str(int(time.time()))+"default.txt")
                        last_basic_message=message.copy()  # Create a copy of the message for basic message
                    except Exception as e:
                        print(message)
                        print("Error sending JSON message:", e)
                        retry+=1
                        if retry> 10:
                            online=False
                            retry=0

                    last_basic_message_time = time.time()#basic message without serial
                
		#retrieve serial data
                sdmu_port="/dev/serial/by-id/usb-1a86_USB2.0-Ser_-if00-port0"
                core_port="/dev/serial/by-id/usb-1a86_USB_Serial-if00-port0"
                throttle=0.5
                print(len(sers), "serial ports found")
                if os.name == 'nt':
                    print("Windows OS detected. Simulating serial data.")
                    sers = [sdmu_port, core_port]  # Simulated ports for Windows

                for ser in sers:
                    time.sleep(throttle)  # Add a small delay to avoid overwhelming the serial port
                    telegrams=[]
                    data=b""
                    if os.name != 'nt':
                        if hasattr(ser, 'port'):
                            port_path = ser.port
                        else:
                            port_path = str(ser)
                        ser_id = id(ser)
                        
                        while ser.in_waiting > 0:
                            data += ser.read(ser.in_waiting)                      



                        telegrams.append(data)

                    if os.name == 'nt':#generate test data on windows
                        print("Windows OS detecte simulate serial data")
                        port_path = core_port
                        ser_id='0'
                        telegram_hex_odo='1b02445d09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005d1a111b031b02445e09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005e1a131b031b02445f09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f1a151b031b02446009b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000601a171b031b02446109b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000611a191b031b02446209b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000621a1b011b031b02446309b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000631a1d1b031b02446409b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000641a1f1b031b02446509b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000651a211b031b02446609b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000661a231b031b02446709b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000671a251b031b02446809b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000681a271b031b02446909b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000691a291b031b02446a09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006a1a2b1b031b02446b09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006b1a2d1b031b02446c09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006c1a2f1b031b02446d09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006d1a311b031b02446e09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006e1a331b031b02446f09b500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006f1a351b031b02447009b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000701a371b031b02447109b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000711a391b031b02447209b50000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000721a3b1b031b02447309b5000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
                        telegram_raw_odo=bytes.fromhex(telegram_hex_odo)
                        telegram_raw_novram=b'! <e>  1485 ODO_CHANNEL0_SS1_NOT_AVAILABLE_ERROR TRUE   54633227</e>\r\n! <e>  1501 ODO_CHANNEL1_SS1_NOT_AVAILABLE_ERROR TRUE   54633234</e>\r\n! <e>  1517 ODO_CHANNEL2_SS1_NOT_AVAILABLE_ERROR TRUE   54633241</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2344 ODO_CHANNEL0_SS1_AVAILABLE_ERROR TRUE   54633567</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2345 ODO_CHANNEL1_SS1_AVAILABLE_ERROR TRUE   54633576</e>\r\nSENSOR IN REVALIDATION\r\n! <e>  2346 ODO_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   54633585</e>\r\n'
    
                        #telegram_raw_novram=b'SENSOR IN REVALIDATION ! <e>  2344 ODO_CHANNEL0_SS1_AVAILABLE_ERROR TRUE   22489643</e> SENSOR IN REVALIDATION ! <e>  2345 ODO_CHANNEL1_SS1_AVAILABLE_ERROR TRUE   22489652</e> SENSOR IN REVALIDATION! <e>  2346 ODO_CHANNEL2_SS1_AVAILABLE_ERROR TRUE   22489661</e> 0'
                        telegrams.append(telegram_raw_novram)
                        
                    #print("data "+str(data))
                    #telegrams.append(ser.readline())
                    #header = ser.read(4*8).hex()
                    #if header == "1a6b":
                    #    telegram=ser.read(362*8).hex()
                    #time.sleep(1)

                    for telegram_raw in telegrams:
                        #send_text_message(mqtt_topic_debug, f"TESTDATA Received telegram from serial port {port_path} (ID: {ser_id}): {telegram_raw.hex()}")

                        #print("telegram_raw "+str(telegram_raw)) #novram is shown here as plaintext
                        telegram_hex=telegram_raw.hex() #odo is usable as hex
                        #print("telegram_hex "+str(telegram_hex))
                        telegram_header=(telegram_hex[:4])   
                        if logging_active:
                                
                                with open(os.path.join(data_path, 'hex_telegram.txt'), 'a') as file:
                                    file.write(telegram_hex+"\n")  
                                    
                                with open(os.path.join(data_path, 'raw_telegram.txt'), 'a') as file:
                                    file.write(str(telegram_raw)+"\n")  

                        ICN_Separator = "1b0244"
                        pattern = r'(?=.{10}' + ICN_Separator + r')' #ff1a551b031b02440009b5
                                               
                        # Count occurrences of "1b0244"
                        icn_count=str(telegram_raw).count(ICN_Separator)
                        #print(f"Count of '1b0244' in the telegram raw string: {icn_count}  raw "+str(telegram_raw)[:100]+"...")
                        
                        icn_count = telegram_hex.count(ICN_Separator)+icn_count
                        #print(f"Count of '1b0244' in the telegram hex string: {icn_count} " +str(telegram_hex)[:100]+"...")

                        #telegram_header = "1a6b" 
                        #if len(telegram_hex)>0:
                            #print("telegram header "+telegram_header+"\t "+str(len(telegram_hex)))

                        if port_path==sdmu_port and (telegram_header == "1a6b" or icn_count>1):#sdmu frame
                        #if telegram_header == "1a6b" or icn_count>1:#main odo frame yvverdon
                            odo_icn_classified+=1
                            #print("telegram 1a6b")
                            test_frame = "08401b031b02441909b5000000a600000002cedb7e83040100002b8a2b7a2b612b5d000000000000000000000000000000000000000000000000000000000000000000000000000000000000050000002b862b6f2b6f2b7c2b8200000000000000000000000000000000000000000000000000000000000000000000000000000000050211671b5500000000000000690000ff02ff02ca44e705049c0032200000a2330e00021f6e000220c201bd0005000400021"
                            #telegram_hex = test_frame #todo remove
                            
                            ICN_Separator = "1b0244"
                            pattern = r'(?=.{10}' + ICN_Separator + r')' #ff1a551b031b02440009b5
                            lines = re.split(pattern, telegram_hex)
                            
                            # Count occurrences of "1b0244"
                            count = telegram_hex.count(ICN_Separator)
                            #print(f"Count of '1b0244' in the telegram hex string: {count}")
                            
                            for line in lines:
                                frame, splitted_line, parsed_frame = ("","","")#parse_ICN_line(line)
                                timestamp_fzdia = datetime.now().isoformat() + "Z"
                                message = create_JSON_object(timestamp_fzdia, UIC_VehicleID, cpu_temp, max_speed, gps_data, source="ODO")
                                #message = add_element(message, "serial_id", "Serial ID", str(ser_id))
                                
                                speed = float(last_gps_speed)# float(parsed_frame['speed'])
                                current_time = time.time()
                                if abs(speed - last_speed) > deviation_to_send or (current_time - last_odo_message_time) > time_threshold:
                                    last_odo_message_time = current_time
                                    #message = add_odo_frame(message, parsed_frame)
                                    print("no odo frame is beeing sent (commented out since it is empty on the test vehicle TODO: re implement)")
                                    #print("speed changed "+str(speed)+" last speed "+str(last_speed)+" deviation "+
            ######odo>###############message_counter=send_json_message(mqtt_topic_odo, message,message_counter)                                            
                                    #message_raw=create_raw_JSON_object(timestamp_fzdia,UIC_VehicleID,telegram_hex,gps_data,source="ODO_RAW")                     
                                    #print("message_raw "+str(message_raw))                                    
            ######odo>##############message_counter_raw=send_json_message(mqtt_topic_raw_odo, message_raw, message_counter)
                                    last_speed = speed
                                                             
                                

                            message=str(my_uuid)+" t1a6b "+"\t"+timestamp+"\t"+str(now)+"\t"+str(telegram_hex)+"\t"+str(telegram_hex)
                            message = message.rstrip('\n')
                            if logging_active:
                                with open(data_path+"\\output_ODO_main_frem_raw.txt", 'a') as file:
                                    file.write(message+"\n")
                            
                            telegram_hex = telegram_hex+" time "+str(now)
                            
                            for line in lines:                   
                                if len(line)==362:
                                    try:
                                        zero_count = line[22:].count('0')
                                        rad_speed = str(float(int(line[125] + line[126],16))/100)
                                        lineid=line[0:2]       
                                        message=(" speed: "+str(rad_speed))
                                        if logging_active:    
                                            
                                            with open(os.path.join(data_path, 'output_ODO_interpreted.txt'), 'a') as file:
                                                file.write(message+" "+line+"\n")
                                        if float(rad_speed) > float(max_speed):
                                            max_speed=rad_speed           
                                    except Exception as e:
                                        rad_speed = 'NAN'        
                                        print(e)              
                                    #print(lineid + " "+line[0:22]+ " len: " +str(len(line))+" speed "+str(rad_speed)+" max "+str(max_speed)+ " zeros "+ str(zero_count))

                            #print("all lines")
                        elif port_path==sdmu_port and telegram_header == "09b5":#special frame ODO Yverdon
                            odo_other_classified+=1
                            #print("telegram l 346")
                            c=telegram_hex.count("0")
                            #print("count"+str(c))
                            #print("telegram hex"+telegram_hex)
                            
                            message=str(my_uuid)+" t2 "+"\t"+timestamp+"\t"+str(now)+"\t"+str(telegram_hex)+"\t"+str(telegram_hex)
                            message = message.rstrip('\n')
                            message=create_raw_JSON_object(timestamp_fzdia,UIC_VehicleID,telegram_hex,"ODO 09b5")                     
                            #message_counter=send_json_message(mqtt_topic_odo, message, message_counter)
                            
                        elif port_path==core_port and len(telegram_hex) > 3:
##NOVRAM------->########################################################################
                        #num_unprintable_hex < 1 and num_unprintable_raw < 1 and len(telegram_hex) > 10 and icn_count < 1: #if no header found, check if telegram is printable
                            novram_classified+=1
                            # CORE NOVRAM: Only process if all characters are printable (or whitespace)                        
                           
                            if len(telegram_raw)>0:
                                #print("no header found-> plain text len"+str(len(telegram_raw))+" time "+timestamp+" telegram: "+str(telegram_raw))
                            
                                try:
                                    telegram_utf=telegram_raw.decode('utf-8')
                                    
                                except:
                                    telegram_utf=str(telegram_raw)
                                    
                                novram_message = str(my_uuid)+"\t"+timestamp+"\t"+str(now)+"\t"+str(telegram_utf)+"\t"+str(telegram_hex)
                                novram_message = novram_message.rstrip('\n')
                                if logging_active:
                                    with open( os.path.join(data_path, 'output.txt'), 'a') as file:
                                        file.write(novram_message)
                                if time.time() - last_novram_message_time >= conf_min_time_novram:
                                    novram_objects=parse_novram_objects(telegram_utf)
                                    
                                    # create a safe template copy of last_basic_message to use when building NOVRAM messages
                                    try:
                                        if isinstance(last_basic_message, dict):
                                            novram_template = json.loads(json.dumps(last_basic_message))  # deep-copy via JSON
                                        else:
                                            novram_template = create_JSON_object(timestamp_fzdia, UIC_VehicleID, cpu_temp, max_speed, gps_data,source="NOVRAM")
                                    except Exception:                                        
                                        novram_template = create_JSON_object(timestamp_fzdia, UIC_VehicleID, cpu_temp, max_speed, gps_data,source="NOVRAM")

                                    for novram_element in novram_objects:
                                        try:                                            
                                            novram_message=add_element(novram_message, "NOVRAM1", "NOVRAM Raw Data", str(novram_element))                                            
                                        except Exception as e:   
                                            novram_message=add_element(novram_message, "NOVRAM1", "NOVRAM Raw Data", "empty")   
                                       
                                        if len(str(novram_element).strip()) > 2:
                                            try:
                                                # list of strings to match (case-insensitive)
                                                novram_tags = ["SENSOR IN REVALIDATION","SS1_AVAILABLE_ERROR","SS1_NOT_AVAILABLE_ERROR"]  # extend this list as needed
                                                text = str(novram_element).upper()
                                                matched = None
                                                for pattern in novram_tags:
                                                    if pattern.upper() in text:
                                                        matched = pattern
                                                        break
                                                if matched:
                                                    tag="informational" 
                                                else:
                                                    tag = "notag"
                                            
                                            except Exception:
                                                tag = ""
                                            try:
                                                if "_" in str(novram_element):
                                                    Error_ID = str(novram_element).split(" ")[0].strip()
                                                    relative_timestamp = str(novram_element).split(" ")[-1].strip()
                                                    novram_name= str(novram_element).split(" ")[1]
                                                else:
                                                    novram_name= str(novram_element)
                                                    Error_ID = "0"
                                                    relative_timestamp = "0"
                                            except Exception:
                                                novram_name="none"
                                                Error_ID = "0"
                                                relative_timestamp = "0"
                                            
                                            novram_message = add_element(novram_template, "seq", "Sequence Number",str(message_counter))
                                            
                                            try:                                            
                                                novram_message=add_element(novram_message, "NOVRAM", "NOVRAM Data", str(novram_name))                                            
                                            except Exception as e:   
                                                novram_message=add_element(novram_message, "NOVRAM", "NOVRAM Data", "empty")                                           
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))

                                            try:    
                                                novram_message = add_element(novram_template, "err_id", "Error ID",tag)
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            try:    
                                                novram_message = add_element(novram_template, "rel_time", "Relative Timestamp",tag)
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            try:    
                                                novram_message = add_element(novram_template, "tag", "tag",tag)
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            try:        
                                                novram_message = add_element(novram_message, "len", "length", str(len(novram_element)))
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            try:    
                                                novram_message = add_element(novram_message, "serial_id", "Serial ID", str(ser_id))
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            try:    
                                                message_counter=send_json_message(mqtt_topic_novram, novram_message,message_counter)
                                            except Exception as e:                                                                                            
                                                send_text_message(mqtt_topic_debug, str(e)+" "+str(traceback.format_exc()))
                                            tag = "notag"
                                            day=time.strftime('%Y-%m-%d', time.localtime())
                                            send_string_to_ftp(ftp_host, ftp_user, ftp_password, message, "public_html/ETCSLoggerData/"+str(UIC_VehicleID)+"/"+day, str(int(time.time()))+"NOVRAM.txt")
                                            last_novram_message_time = time.time()#basic message without serial
                                    skipped_message = 0
                                else:
                                    skipped_message +=1
                        else: 
                            unclassified_telegrams += 1

                        #sending statistics about sent messages    
                        if time.time() - last_remaining_message_time >= conf_status_period :
                            try:                                
                                send_text_message(mqtt_topic_debug, "META unclassified telegrams: "+str(unclassified_telegrams)+" odoframes "+str(odo_icn_classified+odo_other_classified)+" novram classified "+str(novram_classified)+" telegram header: "+str(telegram_header)+" len "+str(len(telegram_hex)))
                                unclassified_telegrams = 0
                                novram_classified=0
                                odo_icn_classified=0
                                odo_other_classified=0
                            except Exception as e:
                                print("Error sending debug message:", e)                                
                            
                            
                            last_remaining_message_time = time.time()#basic message without serial

            except Exception as e:
                print(e)
                message=str(e)+ " "+ str(traceback.format_exc())
                message=create_raw_JSON_object(timestamp,UIC_VehicleID,message,"Exception") 
                message= str(UIC_VehicleID)+" time: "+str(now)+" exception: "+ str(message)
                if time.time() - last_ex_message_time >= 3:
                    send_text_message(mqtt_topic_debug, message)
                    last_ex_message_time = time.time()

            #if telegram_hex:
                #print("telegram available")
                #print("hex"+telegram) 
                #print("raw"+str(telegram_raw))       

except KeyboardInterrupt:
    logging.error('Error occurred', exc_info=True)    
    print("Measurement stopped by user")

finally:
    send_text_message(mqtt_topic_debug, "logger script stopped at "+str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
       
    client.disconnect   

    print("Terminating GPS process...")
    if os.name != 'nt':
        gps_process.terminate()
        gps_process.join()
    
    led.off()
    # Cleanup GPIO resources
    if os.name == 'posix':  # Only perform GPIO cleanup on Linux
        try:
            led.off()  # Ensure the LED is turned off
            print("GPIO resources cleaned up.")
        except Exception as e:
            print(f"Error during GPIO cleanup: {e}")
    for ser in sers:
        ser.close() 
    print("bye..")
