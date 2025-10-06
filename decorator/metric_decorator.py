from functools import wraps
from prometheus_client import Gauge, Counter

plc_sensor_readings = Gauge('plc_sensor_reading', 'Current value of a PLC sensor', ['equipment', 'sensor'])
raw_data_events_counter = Counter('raw_data_events_total', 'Total number of PLC value readings')
events_triggered_counter = Counter('events_triggered_total', 'Total number of events rule triggered')

low_pressure_counter = Counter('low_pressure_total', 'low pressures total triggers')
temp_out_counter = Counter('temp_out_total', 'temp out of bounds total triggers')

def update_prometheus_on_read(func):
    
    @wraps(func)
    def wrapper(self, equipment, *args, **kwargs):
        readings = func(self, equipment, *args, **kwargs)

        if readings:
            for tag_name, value in readings.items():
                try:
                    numeric_value = float(value)
                    plc_sensor_readings.labels(equipment=equipment.name, sensor=tag_name).set(numeric_value)
                    raw_data_events_counter.inc()
                except (ValueError, TypeError):
                    pass
        
        return readings
    return wrapper

def update_event_counter(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        triggered_events = func(*args, **kwargs)

        if len(triggered_events) > 0:

            for evt in triggered_events:

                if evt["event_name"] == "PressaoCO2Baixa" : low_pressure_counter.inc()
                else : temp_out_counter.inc()

            events_triggered_counter.inc(len(triggered_events))

        return triggered_events
    return wrapper