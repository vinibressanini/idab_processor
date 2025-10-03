from abc import ABC, abstractmethod
from queue import Queue
import paho.mqtt.client as mqtt
import random

from utils.converter import Converter

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "oven/01"

class CommunicationAdapter(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def read(self, tags):
        pass

class MqttAdapter(CommunicationAdapter):
    
    def __init__(self):
        self._host = MQTT_BROKER
        self._port = MQTT_PORT
        self._base_topic = MQTT_TOPIC
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_message = self._on_message_callback
        self._message_queue = Queue()

    def connect(self):
        print(f"Connecting MQTT client to {self._host}...")
        self._client.connect(self._host, self._port, 60)
        self._client.subscribe(f"{self._base_topic}/#")
        self._client.loop_start()
        print("MQTT client connected and listening.")

    def _on_message_callback(self, client, userdata, msg):
        self._message_queue.put(msg)
        
    def read(self, tags=None):
       
        plc_address_map = {
            tag['plc_address']: {'name' : tag['name'], 'type' : tag['type']}
                  for tag in tags}
        readings = {}

        while not self._message_queue.empty():
            msg = self._message_queue.get()
            
            plc_address = msg.topic.split("/")[-1]
            
            if plc_address in plc_address_map:
                tag_name = plc_address_map[plc_address]['name']
                payload = msg.payload.decode("utf-8")
                readings[tag_name] = Converter.cast(payload, plc_address_map[plc_address]['type'])
        
        return readings



class PLCDataReader(CommunicationAdapter):

    def connect(self):
        print("Connected To PLC (MOCKED DATA)")

    def read(self, tags):

        readings = {}

        for tag in tags:

            reading = None

            match (tag['plc_address']):
            # --- Mash Tun MT-01 (Mashing Process) ---
                case "100":  # Temperature
                    # 98% chance of normal temp, 2% chance of a spike
                    if random.random() < 0.1:
                        reading = random.uniform(81, 85.0)  # Simulate a spike
                    else:
                        reading = random.uniform(65.0, 68.0)  # Normal mashing temp

                case "101":  # WaterVolume
                    reading = random.uniform(160, 200)

                case "102":  # AgitatorStatus
                    # Simulate agitator is on (1) most of the time, off (0) occasionally
                    reading = 1 if random.random() < 0.9 else 0

                case "103":  # CycleStep
                    # Randomly reading = one of the possible mashing steps
                    reading = random.choice([1, 2, 3])

                # --- Fermentation Tank FV-101 (Fermenting Process) ---
                case "200": # Fermentation Temperature
                    reading = random.uniform(17.5, 25.5)

                case "201": # Tank Pressure (Bar)
                    reading = random.uniform(1.0, 1.4)
                    
                case "202":  # SpecificGravity
                    # reading = a random value within the typical fermentation range
                    reading = random.uniform(1.012, 1.060)

                case "203": # Fermentation Phase
                    # Randomly choose between active fermentation (3) and conditioning (4)
                    reading = random.choice([3, 4])
                    
                case "204": # CoolingSystemStatus
                    reading = 1 # Assume cooling is always active

                # --- Packaging Process ---
                case "300": # FillVolume (ml)
                    # Simulate that the machine is sometimes idle (reading =s 0)
                    reading = random.uniform(495, 505) if random.random() < 0.85 else 0

                case "301": # ConveyorBeltSpeed (m/s)
                    reading = random.uniform(2.8, 3.2)
                    
                case "302":  # MachineState for Can Filler
                    # reading = a random state, weighted to be 'Running' most often
                    # States: 1=Idle, 2=Running, 3=Fault
                    reading = random.choices([1, 2, 3], weights=[0.15, 0.80, 0.05], k=1)[0]
                    
                case "303":  # RejectCount
                    # A random number of rejects at any given time
                    reading = random.randint(0, 50)
                    
                case "304": # FilledCanWeight (grams)
                    reading = random.uniform(472.5, 473.5)

                # --- Utility/Lab Readings ---
                case "400": # CO2 Level (Volumes)
                    reading = random.uniform(3.0, 4.0)
                    
                case "401": # Lab Temperature
                    # 98% chance of a normal temp, 2% chance of a low outlier
                    reading = random.uniform(18, 25) if random.random() < 0.98 else 13.0
                    
                case "402": # Water pH Level
                    reading = random.uniform(9.8, 10.2)
                    
                case "403": # TransferPumpState
                    # States: 1=Off, 2=On
                    reading = random.choice([1, 2])
                    
                case _:  # Default case for any other address
                    reading = random.randint(0, 200)
            
            readings[tag['name']] = reading
        
        return readings