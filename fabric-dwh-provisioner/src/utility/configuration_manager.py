import os


class ConfigurationManager:
    def __init__(self, configuration_key: str):
        self.value = os.getenv(configuration_key)
        if self.value is None:
            raise ValueError(f"Required environment key {configuration_key} not found")
