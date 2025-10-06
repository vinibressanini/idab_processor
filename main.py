from threading import Timer
import time
import signal
import sys
from services.config_loader import ConfigLoader
from services.data_reader import MqttAdapter, PLCDataReader
from services.event_generator import EventGenerator
from prometheus_client import start_http_server

shutdown_flag = False
def handle_signal(signum, frame):
    global shutdown_flag
    print(f"\nSignal {signum} received. Initiating graceful shutdown...")
    shutdown_flag = True

if __name__ == "__main__":
    
    signal.signal(signal.SIGTERM, handle_signal) 
    signal.signal(signal.SIGINT, handle_signal) 

    start_http_server(8001)
    loader = ConfigLoader()
    equipments , interpreter = loader.initialize()
    count = 1
    generator = EventGenerator()
    plc_reader = PLCDataReader()
    
    timer = None

    try:
        plc_reader.connect()
        
        timer = Timer(3.0, generator.evaluate_rules, kwargs={'interpreter' : interpreter, 'timespan' : 3.0, 'equipments' : equipments})
        timer.daemon = True
        timer.start()

        while not shutdown_flag:
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
        if timer:
            timer.cancel()
            print("Event generator timer canceled.")
        print("Cleanup complete. Exiting.")