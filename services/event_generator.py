from datetime import datetime
from threading import Timer
import threading
from asteval import Interpreter

from services.event_publisher import EventPublisher



class EventGenerator():

    def __init__(self):
        self.sender = EventPublisher()

    def evaluate_rules(self, interpreter : Interpreter, timespan, equipments):
       
        events = []

        for equipment in equipments:
            interpreter.symtable.update(equipment.symtable)
            for rule in equipment.rules:
                
                print(f"\n ---------- Evaluating Rule : {rule['name']} ----------------")
                triggered = interpreter.run(rule['expression'])
                
                if triggered and not rule['onchange']:
                    event = self._create_event_payload(rule, equipment.metadata)
                    events.append(event)
                    print(event)

                elif rule['onchange'] and rule['state'] != triggered:
                    event = self._create_event_payload(rule, equipment.metadata)
                    events.append(event)
                    print(event)

                rule['state'] = triggered
            
        timer = Timer(timespan, self.evaluate_rules, kwargs={'interpreter' : interpreter, 'timespan' : timespan, 'equipments' : equipments})
        timer.daemon = True
        timer.start()

        thread = threading.Thread(target=self.sender.send_event, kwargs={'events' : events})
        thread.start()
        
        return events
    
    def _create_event_payload(self, rule, metadata):
        
        return {
            "event_name": rule['name'],
            "timestamp": datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
            "metadata" : metadata,
        }
