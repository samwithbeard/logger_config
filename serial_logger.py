#!/usr/bin/env python3

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
import os
import json
import jsonschema
from jsonschema import validate
import requests
from SDMU_parser import parse_ICN_line
from multiprocessing import Process, Queue
from gps_collector import start_gps_collector
import subprocess
import psutil

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

    # Assign the simulated LED to the LED variable
    LED = SimulatedLED
led = LED(6)
led.off()

version="0.0.17"
print(version)
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
        validate(instance=json_data, schema=schema)
        #print("JSON is valid.")
    except jsonschema.exceptions.ValidationError as err:
        print("JSON is invalid.")
        print("Error message:", err.message)
        n=0

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
intern=False
if os.name == 'nt':
        print("Windows OS detected. Skipping 4G module startup check.")
        modem_port="COM3"
else:
        modem_port="/dev/serial/by-id/usb-SimTech__Incorporated_SimTech__Incorporated_0123456789ABCDEF-if04-port0"    


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
    MQTT_BROKER=mqtt_port_outside

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

def replace_none_with_null(json_data):
    """
    Replace all occurrences of None with 'null' in a JSON object while maintaining JSON format.
    Ensure the output is valid JSON.

    :param json_data: The JSON object to process.
    :return: The modified JSON object with None replaced by 'null'.
    """
    if isinstance(json_data, dict):
        return {key: replace_none_with_null(value) for key, value in json_data.items()}
    elif isinstance(json_data, list):
        return [replace_none_with_null(item) for item in json_data]
    elif json_data is None:
        return None  # Keep it as None, as JSON serialization will handle it correctly
    else:
        return json_data

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
    json_message_i = add_element(json_message_i, "seq", "Sequence Number", message_counter)
    json_message=replace_none_with_null(json_message_i)
    
    try:
        validate_json(json_message,schema)
        message=str(json_message)
        client.publish(topic, message)
        print("mqtt message "+str(message))
        if message_counter >= 10000:
            message_counter=0
        else:
            message_counter += 1
        
    except Exception as e:
        print("fail to send "+str(message) + str(e))
    
    toggle_led()
    return message_counter

def send_text_message(topic, message):
    try:        
        client.publish(topic, message)
        print("send "+str(message))
    except Exception as e:
        print("fail to send "+str(message) + str(e))

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
        send_text_message(mqtt_topic_publish,message)
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



def list_serial_interfaces_linux():
    directory = '/dev/serial/by-id/'
    ports=[]
    if os.path.exists(directory) and os.path.isdir(directory):
        files = os.listdir(directory)
        if files:
            print("Available serial interfaces:")
            for filename in files:
                if filename.startswith('usb-1a86_'):
                    ports.append(os.path.join(directory, filename))
                    
        else:
            print("No serial interfaces found.")
       
        return ports



def find_serial_adapters(target_description):
    ports=[]
    sers=[]
    if os.name != 'nt':
        print("not windows")
        port_list=list_serial_interfaces_linux()
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
fail=True
while fail:
        try:
            target_description = "USB-SERIAL CH340"  # Beispielhafte Beschreibung
            sers = find_serial_adapters(target_description)
            fail= False            
            
        except Exception as e:
            fail= True
            message= str(my_uuid)+" serial port  not ready"+ str(e)
            send_text_message(mqtt_topic_publish,message)
            #time.sleep(5)

print("serial connection opened")
def get_temp():
    if os.name == 'nt':
        temp=0
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
    send_text_message(mqtt_topic_publish, message)
    
    print(f"Current CPU temperature: {temp}°C")
    if temp > 80.0:  # Threshold for warning
        print("Warning: CPU temperature is too high! wait..")
        while temp > 82.0:
            temp = get_temp()
            message="logger CPU too high, having a break.."
            send_text_message(mqtt_topic_publish, message)
            time.sleep(1)  # Log every 1 seconds 
            message="..continue"
            send_text_message(mqtt_topic_publish, message)


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
                "operationalStatus": "simulated",
                "data": [
                    {
                        "key": "cpu_temp",
                        "name": "CPU Temperature",
                        "value": cpu_temp 
                    },
                    {
                        "key": "max_speed",
                        "name": "Maximum Speed",
                        "value": max_speed
                    },
                    {
                        "key": "gps",
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
                        "key": "gps",
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
   
#logpath=os.getcwd()+"/data/serial_log.txt"
logpath=data_path+"serial_log_test.txt"
loopcounter=0


position_queue = Queue()
if os.name == 'nt':
    print('no gps on windows')
    time.sleep(3)
else:
    message_types = ['GPRMC']  # Filter for GPRMC messages only
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
last_ex_message_time=0
last_odo_message_time=0
last_novram_message_time=0
skipped_message = 0
deviation_to_send = conf_dbr_odo # only send message if speed changes more than 0.5 km/h
time_threshold = conf_min_time_odo  # time threshold in seconds
message_counter=0                            

# flush serial buffer on startup
print("flushing serial buffer..")
for ser in sers:
    data = b""
    while ser.in_waiting > 0:
        print("flushing serial buffer..")
        data = ser.read(ser.in_waiting)

data = b""
online=True
retry=0
signal_strength, signal_quality=get_signal_quality(modem_port)
try:
    print("waiting for serial messages..")
    while True:

        #temp_check()        
        #with open('data/serial_log.txt', 'a') as log_file:
        while True:

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
                    gps_data="nt"
                else:
                    if not gps_process.is_alive():
                        #print("GPS process has stopped. Restarting...")
                        gps_process = Process(target=start_gps_collector, args=(position_queue,))
                        gps_process.start()

                    if not position_queue.empty():
                        gps_data = position_queue.get()
                        gps_speed = get_speed_from_nmea(gps_data)
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
                    
                        

                    
                
                message = create_JSON_object(timestamp_fzdia,UIC_VehicleID,cpu_temp,max_speed,gps_data)
                #print("create json message"+str(message))
                message=add_element(message, "gps_speed", "GPS Speed", gps_speed)       
                #print("add element to message"+str(message))           
                v_diff=abs(float(gps_speed) - float(last_gps_speed)) if isinstance(gps_speed, (int, float)) and isinstance(last_gps_speed, (int, float)) else 0
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
                            message=add_element(message, "signal_strength", "Signal Strength", signal_strength)                    
                            message=add_element(message, "signal_quality", "Signal Quality", signal_quality) 
                        except Exception as e:
                            print("Error getting signal quality:", e)
                            signal_strength, signal_quality=(-1, -2) 
                                                  
                    else:
                        signal_strength, signal_quality=(-1, -2)
                    try:
                        cpu_load = get_cpu_load()
                        message = add_element(message, "cpu_load", "CPU Load", cpu_load)
                    except Exception as e:
                        print("Error getting CPU load:", e)
                        cpu_load = -1
                        message = add_element(message, "cpu_load", "CPU Load", cpu_load)

                    try:
                        memory_load = get_memory_load()
                        message = add_element(message, "memory_load", "Memory Load", memory_load)
                    except Exception as e:
                        print("Error getting memory load:", e)
                        memory_load = -1
                        message = add_element(message, "memory_load", "Memory Load", memory_load)

                    try:
                        disk_space = get_disk_space()
                        message = add_element(message, "disk_space", "Disk Space", disk_space)
                    except Exception as e:
                        print("Error getting disk space:", e)
                        disk_space = -1
                        message = add_element(message, "disk_space", "Disk Space", disk_space)
                    try:
                        print("try to send json message..")
                        message_counter=send_json_message(mqtt_topic_publish, message,message_counter)
                    except Exception as e:
                        print(message)
                        print("Error sending JSON message:", e)
                        retry+=1
                        if retry> 10:
                            online=False
                            retry=0

                    last_basic_message_time = time.time()#basic message without serial
                
		#Odometrie:
                telegrams=[]
                
                for ser in sers:
                    data=b""
                    while ser.in_waiting > 0:
                        data += ser.read(ser.in_waiting)
                    
                    telegrams.append(data)
                    #print("data "+str(data))
                    #telegrams.append(ser.readline())
                    #header = ser.read(4*8).hex()
                    #if header == "1a6b":
                    #    telegram=ser.read(362*8).hex()
                    #time.sleep(1)

                for telegram_raw in telegrams:
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
                    if icn_count>1:
                        time.sleep(1)
                    #telegram_header = "1a6b" 
                    #if len(telegram_hex)>0:
                        #print("telegram header "+telegram_header+"\t "+str(len(telegram_hex)))

                    if telegram_header == "1a6b" or icn_count>1:#main odo frame yvverdon
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
                            frame, splitted_line, parsed_frame = parse_ICN_line(line)
                            timestamp_fzdia = datetime.now().isoformat() + "Z"
                            message = create_JSON_object(timestamp_fzdia, UIC_VehicleID, cpu_temp, max_speed, gps_data, source="ODO")
                            message = add_odo_frame(message, parsed_frame)

                            speed = float(parsed_frame['speed'])
                            current_time = time.time()
                            if abs(speed - last_speed) > deviation_to_send or (current_time - last_odo_message_time) > time_threshold:
                                message_counter=send_json_message(mqtt_topic_publish, message,message_counter)
                                
                                last_speed = speed
                                last_odo_message_time = current_time
                        
                            

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
                    elif telegram_header == "09b5":#special frame ODO Yverdon
                        #print("telegram l 346")
                        c=telegram_hex.count("0")
                        #print("count"+str(c))
                        #print("telegram hex"+telegram_hex)
                        
                        message=str(my_uuid)+" t2 "+"\t"+timestamp+"\t"+str(now)+"\t"+str(telegram_hex)+"\t"+str(telegram_hex)
                        message = message.rstrip('\n')

                        message=create_raw_JSON_object(timestamp_fzdia,UIC_VehicleID,telegram_hex,"ODO")                     

                        message_counter=send_json_message(mqtt_topic_publish, message, message_counter)
                    else:#CORE NOVRAM
                        
                        if len(telegram_raw)>0:
                            #print("no header found-> plain text len"+str(len(telegram_raw))+" time "+timestamp+" telegram: "+str(telegram_raw))
                        
                            try:
                                telegram_utf=telegram_raw.decode('utf-8')
                            except:
                                telegram_utf=str(telegram_raw)
                            message=str(my_uuid)+"\t"+timestamp+"\t"+str(now)+"\t"+str(telegram_utf)+"\t"+str(telegram_hex)
                            message = message.rstrip('\n')
                            if logging_active:
                                with open( os.path.join(data_path, 'output.txt'), 'a') as file:
                                    file.write(message)
                            if time.time() - last_novram_message_time >= conf_min_time_novram:
                                message=create_raw_JSON_object(timestamp_fzdia,UIC_VehicleID,telegram_utf+str(skipped_message),"NOVRAM")
                                message_counter=send_json_message(mqtt_topic_publish, message,message_counter)
                                last_novram_message_time = time.time()#basic message without serial
                                skipped_message = 0
                            else:
                                skipped_message +=1
                            

            except Exception as e:
                print(e)
                message=str(e)+ " "+ str(traceback.format_exc())
                message=create_raw_JSON_object(timestamp,UIC_VehicleID,message,"Exception") 
                message= str(UIC_VehicleID)+" time: "+str(now)+" exception: "+ str(message)
                if time.time() - last_ex_message_time >= 3:
                    send_text_message(mqtt_topic_publish, message)
                    last_ex_message_time = time.time()

            #if telegram_hex:
                #print("telegram available")
                #print("hex"+telegram) 
                #print("raw"+str(telegram_raw))       

except KeyboardInterrupt:
    logging.error('Error occurred', exc_info=True)    
    print("Measurement stopped by user")

finally:
    client.publish(mqtt_topic_test_publish,str(UIC_VehicleID)+" time: "+str(now)+" logger stopped ")     
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
