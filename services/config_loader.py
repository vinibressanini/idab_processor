import json
import asteval
from models.equipment import Equipment


class ConfigLoader():

    def _load_config(self):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                config = json.load(f)

                return config
            
        except json.JSONDecodeError as e:
            print(f"Error while decoding JSON config file: {e}")
            exit(0)

    def _compile_event_rules(self, config, interpreter):

        compiled_rules = {}

        for eq_name, eq_cfg in config.items():
            for rule in eq_cfg['event_rules']:
                if (rule['expression'] not in compiled_rules.keys()):
                    compiled_rules[rule["expression"]] = interpreter.parse(rule["expression"])
    
        return compiled_rules
    
    def _build_equipments(self,config, compiled_rules):

        equipments = []

        for eq_name, eq_cfg in config.items():
            
            equipment = Equipment(
                eq_name,
                eq_cfg['ip'],
                config[eq_name],
                compiled_rules
            )

            equipments.append(equipment)

        return equipments

    def initialize(self):

        try:
            interpreter = asteval.Interpreter()
            config = self._load_config()
            compiled_rules = self._compile_event_rules(config, interpreter)
            equipments = self._build_equipments(config, compiled_rules)
                               
            return equipments, interpreter

        except Exception as e:
            raise e

