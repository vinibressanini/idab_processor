from datetime import datetime
from asteval import asteval, Interpreter

class Equipment():

    def __init__(self, name : str, ip : str, config : dict, compiled_rules : dict):

        self.name = name
        self.ip = ip

        self.tags = config['tags']
        self.rules = []
        self.symtable = asteval.make_symbol_table()
        self.metadata = config['metadata']
        
        for rule in config['event_rules']:

            expression = rule['expression']

            compiled_rule = compiled_rules[expression]

            self.rules.append({
                'name' : rule['name'],
                'expression' : compiled_rule,
                'onchange' : rule['onchange'],
                'state' : None,
            })

    def update_values(self, new_values):
        self.symtable = new_values

    
    





