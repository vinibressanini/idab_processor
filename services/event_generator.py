from threading import Event, Timer
import threading
import time
from asteval import Interpreter

from decorator.metric_decorator import update_event_counter
from models.equipment import Equipment



class EventGenerator():

    def __init__(self, sender,  shutdown_event : Event):
        self.sender = sender
        self.shutdown_event = shutdown_event
        self.active_threads = []

    @update_event_counter
    def evaluate_rules(self, interpreter : Interpreter, timespan, equipments):

        if self.shutdown_event.is_set():
            print("Shutdown detected, stopping rule evaluation.")
            return
       
        events = []

        for equipment in equipments:
            
            if equipment.tags[0]['name'] not in equipment.symtable:
                continue

            interpreter.symtable.update(equipment.symtable)
            for rule in equipment.rules:
                
                print(f"\n ---------- Evaluating Rule : {rule['name']} ----------------")
                triggered = interpreter.run(rule['expression'])

                if triggered and rule['state'] != triggered:
                    event = self._create_event_payload(rule, equipment)
                    events.append(event)
                    print(event)

                rule['state'] = triggered
                
                # if triggered:
                #     event = self._create_event_payload(rule, equipment)
                #     events.append(event)
                #     print(event)
            
        self._cleanup_finished_threads()

        ## SERVICE BUS CALL. BACKGROUND TASK USING A LIGHTWEIGHT THREAD
        if events:
            thread = threading.Thread(target=self.sender.send_event, kwargs={'events' : events})
            thread.start()

        # scheduling new rule evaluation
        if not self.shutdown_event.is_set():
            self.timer = Timer(timespan, self.evaluate_rules, kwargs={'interpreter' : interpreter, 'timespan' : timespan, 'equipments' : equipments})
            self.timer.daemon = True
            self.timer.start()

        return events

    def start(self, interpreter, timespan, equipments):
        print("Starting event generator...")
        self.evaluate_rules(interpreter, timespan, equipments)

    def shutdown(self):

        if self.timer:
            self.timer.cancel()
            
        for thread in self.active_threads:
            thread.join()

        self.sender.close()
    
    def _create_event_payload(self, rule, equipment : Equipment):

        event = {
            "event_name": rule['name'],
            "code" : equipment.code,
            "routing_key" : rule['routing_key'] or "",
            "timestamp": int(time.time()),
            "metadata" : equipment.metadata
        }

        if rule['output'] : event['data'] =  {rule['output'] : equipment.symtable.get(rule['output'])}
    
        return event
    
    def _cleanup_finished_threads(self):
        self.active_threads = [t for t in self.active_threads if t.is_alive()]
