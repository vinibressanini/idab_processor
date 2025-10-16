from threading import Event, Timer
import time
import signal
import sys
from services.config_loader import ConfigLoader
from services.data_reader import MqttAdapter, PLCDataReader
from services.event_generator import EventGenerator
from prometheus_client import start_http_server
from services.event_publisher import EventPublisher, MockEventPublisher, RabbitMQEventPublisher

shutdown_event = Event()
def handle_signal(signum, frame):
    global shutdown_event
    print(f"\nSignal {signum} received. Initiating graceful shutdown...")
    shutdown_event.set()

if __name__ == "__main__":
    
    signal.signal(signal.SIGTERM, handle_signal) 
    signal.signal(signal.SIGINT, handle_signal) 

    start_http_server(8001)
    loader = ConfigLoader()
    equipments , interpreter = loader.initialize()
    sender = RabbitMQEventPublisher()
    generator = EventGenerator(sender=sender, shutdown_event = shutdown_event)
    plc_reader = MqttAdapter(equipments)
    
    count = 1

    try:
        
        plc_reader.connect(equipments)
        generator.start(interpreter = interpreter, timespan = 3.0, equipments = equipments)
        
        while not shutdown_event.is_set():
            print(f"---------------------------- Cycle Number {count} ----------------------------------------")

            for equipment in equipments:
                readings = plc_reader.read(equipment)
                if (readings):
                    equipment.update_values(readings)
                    print("Cycle Readings: ", readings)

            time.sleep(1)
            count += 1
    
    finally:
        
        print("\nMain loop exited. Performing cleanup...")
        generator.shutdown()
        print("Cleanup complete. Exiting.")