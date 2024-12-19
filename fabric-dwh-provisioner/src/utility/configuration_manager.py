import os

class ConfigurationManager:

    def __init__(self, configuration_key:str):
        value = os.getenv(configuration_key)
        if value is None:
            raise ValueError(f"Required environment key {configuration_key} not found")
        else:
            return value