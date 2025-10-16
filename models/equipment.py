from asteval import asteval

class Equipment():

    def __init__(self, name : str, ip : str, code : str, config : dict, compiled_rules : dict):

        self.name = name
        self.ip = ip
        self.code = code

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
                'routing_key' : rule['routing_key'] or "",
                'output' : rule['output'],
                'state' : False
            })

    def update_values(self, new_values):
        try:
            self.symtable.update(new_values)
        except AttributeError:
            self.symtable = new_values

    
    





