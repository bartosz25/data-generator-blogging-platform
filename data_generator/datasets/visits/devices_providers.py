import random
from abc import ABC, abstractmethod

from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.visits.model import TechnicalContext


class TechnicalContextProvider(ABC):

    @abstractmethod
    def provide(self) -> TechnicalContext: raise NotImplementedError

    @staticmethod
    def generate_browser() -> str:
        return random.choice(['Chrome', 'Firefox', 'Safari'])

    @staticmethod
    def generate_browser_version() -> str:
        return random.choice(['20.0', '20.1', '20.2', '21.0', '22.0', '23.0', '23.1', '23.2', '24.11', '25.09'])

    @staticmethod
    def generate_network_type() -> str:
        return random.choice(['5G', '4G', 'Wi-Fi', 'LAN'])


class RandomTechnicalContextProvider(TechnicalContextProvider):

    def provide(self) -> TechnicalContext:
        return TechnicalContext(
            browser=self.generate_browser(),
            browser_version=self.generate_browser_version(),
            network_type=self.generate_network_type(),
            device_type=random.choice(['PC', 'MacBook', 'iPad', 'Smartphone', 'iPhone']),
            device_version=random.choice(['1.0', '2.0', '3.0', '4.0', '5.0', '6.0'])
        )


class DeviceTechnicalContextProvider(TechnicalContextProvider):

    def __init__(self, device_entity_generator: DeviceEntityGenerator):
        self.device_entity_generator = device_entity_generator
        self.last_accessed_device = -1

    def provide(self) -> TechnicalContext:
        self.last_accessed_device += 1
        generated_devices = self.device_entity_generator.get_generated_devices()
        if self.last_accessed_device >= len(generated_devices):
            self.last_accessed_device = 0
        device_to_provide = generated_devices[self.last_accessed_device]

        return TechnicalContext(
            browser=self.generate_browser(),
            browser_version=self.generate_browser_version(),
            network_type=self.generate_network_type(),
            device_type=device_to_provide.type,
            device_version=device_to_provide.version
        )
