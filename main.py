from threading import Timer
import time
from services.config_loader import ConfigLoader
from services.data_reader import MqttAdapter, PLCDataReader
from services.event_generator import EventGenerator

if __name__ == "__main__":

    loader = ConfigLoader()
    equipments , interpreter = loader.initialize()
    count = 1
    generator = EventGenerator()
    plc_reader = PLCDataReader()
    plc_reader.connect()
    timer = Timer(5.0, generator.evaluate_rules, kwargs={'interpreter' : interpreter, 'timespan' : 5.0, 'equipments' : equipments})
    timer.daemon = True
    timer.start()

    while(True):
        
        print(f"---------------------------- Cycle Number {count} ----------------------------------------")

        for equipment in equipments:

            readings = plc_reader.read(equipment.tags)

            if (readings):
                equipment.update_values(readings)
                print("Cycle Readings: ", readings)

        time.sleep(1)
        count += 1



        

        
        

