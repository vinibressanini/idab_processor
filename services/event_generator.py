from datetime import datetime
from threading import Timer
from asteval import Interpreter
from services.outbox import store_event

from decorator.metric_decorator import update_event_counter



class EventGenerator():    

    def __init__(self, sender):
        self.sender = sender

    @update_event_counter
    def evaluate_rules(self, interpreter : Interpreter, timespan, equipments):
       
        events = []

        for equipment in equipments:
            interpreter.symtable.update(equipment.symtable)
            for rule in equipment.rules:
                
                print(f"\n ---------- Evaluating Rule : {rule['name']} ----------------")
                triggered = interpreter.run(rule['expression'])
                
                if triggered:
                    event = self._create_event_payload(rule, equipment.metadata)
                    events.append(event)
                    store_event(event['event_name'], event['metadata'], event['timestamp'])
                    print(event)
            
        timer = Timer(timespan, self.evaluate_rules, kwargs={'interpreter' : interpreter, 'timespan' : timespan, 'equipments' : equipments})
        timer.daemon = True
        timer.start()

        ## SERVICE BUS CALL. BACKGROUND TASK USING A LIGHTWEIGHT THREAD

        # thread = threading.Thread(target=self.sender.send_event, kwargs={'events' : events})
        # thread.start()
        
        return events
    
    def _create_event_payload(self, rule, metadata):
        
        return {
            "event_name": rule['name'],
            "timestamp": int(datetime.now().timestamp()),
            "metadata" : metadata,
        }
