from abc import ABC, abstractmethod
from queue import Queue
import paho.mqtt.client as mqtt
import random

from decorator.metric_decorator import update_prometheus_on_read
from utils.converter import Converter

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "oven/01"

class CommunicationAdapter(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def read(self, equipment):
        pass

class MqttAdapter(CommunicationAdapter):
    
    def __init__(self, equipments):
        self._host = MQTT_BROKER
        self._port = MQTT_PORT
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_message = self._on_message_callback
        self._message_queues = {}
        self.plc_address_map = {}

        for equipment in equipments:
            self._message_queues[equipment.name] = Queue()
            for tag in equipment.tags:  
                self.plc_address_map[tag['plc_address']] = {'name' : tag['name'], 'type' : tag['type']}
                
    def connect(self, equipments):
        print(f"Connecting MQTT client to {self._host}...")
        self._client.connect(self._host, self._port, 60)
        topics = []

        for equipment in equipments:
            topic_name = f"/{equipment.name}/#"
            topics.append((topic_name, 0))

        self._client.subscribe(topics)
        self._client.loop_start()
        print("MQTT client connected and listening.")

    def _on_message_callback(self, client, userdata, msg):
        
        try:
            equip_name = msg.topic.split('/')[1]
        except IndexError:
            print(f"Warning: Could not parse equipment name from topic: {msg.topic}")
            return

        print(equip_name)

        if equip_name in self._message_queues:
            self._message_queues[equip_name].put(msg)
        else:
            print(f"Warning: Received message for unknown equipment queue: {equip_name}")
            print(self._message_queues)
        
        
    def read(self, equipment=None):
        readings = {}

        if not equipment:
            return readings # Cannot read without knowing which equipment

        # --- CHANGE 3: Read from the specific equipment's queue ---
        equip_name = equipment.name
        
        # Get the correct queue for this equipment; return if not found.
        target_queue = self._message_queues.get(equip_name)
        if not target_queue:
            return readings

        # Drain only the messages from this equipment's queue
        while not target_queue.empty():
            msg = target_queue.get()
            plc_address = msg.topic.split("/")[-1]

            if plc_address in self.plc_address_map:
                tag_name = self.plc_address_map[plc_address]['name']
                payload = msg.payload.decode("utf-8")
                readings[tag_name] = Converter.cast(payload, self.plc_address_map[plc_address]['type'])
        
        return readings


### DADOS MOCKADOS PARA DEMO SOMENTE
class PLCDataReader(CommunicationAdapter):

    def __init__(self): 
        self.simulation_state = {}

    def connect(self):
        print("Connected To PLC (MOCKED DATA)")

    @update_prometheus_on_read
    def read(self, equipment):
        readings = {}
        eq_name = equipment.name

        if eq_name not in self.simulation_state:
            self.simulation_state[eq_name] = {}

        for tag in equipment.tags:
            tag_name = tag['name']
            
            if tag_name not in self.simulation_state[eq_name]:
                if "Temperatura" in tag_name:
                    self.simulation_state[eq_name][tag_name] = 20.0
                elif "Pressao" in tag_name:
                    self.simulation_state[eq_name][tag_name] = 3.0
                else:
                    self.simulation_state[eq_name][tag_name] = 0

            last_reading = self.simulation_state[eq_name][tag_name]
            reading = last_reading 

            match (tag['plc_address']):
                
                case "100": 
                    noise = random.uniform(-0.2, 0.2) if random.random() > 0.55 else random.uniform(-0.5, 0.5)
                    reading = last_reading + noise

                    if reading < 1.95: reading =1.95
                    if reading > 3.25: reading = 3.25

                case "101": 
                     reading = random.choice([1, 2, 3])
                
                case "102":  
                    noise = random.randint(1, 3) if random.random() > 0.1 else 0

                    reading = last_reading + noise

                case "200": 

                    noise = random.uniform(-0.4, 0.65) if random.random() > 0.4 else random.uniform(-1.25, 1.55)

                    reading = last_reading + noise
                    if reading < 17: reading = 17
                    if reading > 26: reading = 26

                case "201": 
                    noise = random.uniform(-0.02, 0.02)
                    reading = last_reading + noise
                    if reading < 0.9: reading = 0.9
                    if reading > 1.5: reading = 1.5
                    

                case "203": 
                    reading = random.choice([1, 3])
                    
            self.simulation_state[eq_name][tag_name] = reading
            readings[tag_name] = round(reading, 3) if isinstance(reading, float) else reading
        
        return readings