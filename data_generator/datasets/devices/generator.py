import logging
import random
import time

from data_generator.datasets.devices.model import Device
from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.datasets.generation_context import EntityGenerator


class DeviceEntityGenerator(EntityGenerator):
    LENOVO = 'lenovo'
    GALAXY = 'galaxy'
    HTC = 'htc'
    LG = 'lg'
    IPHONE = 'iphone'
    MAC = 'mac'
    TYPES = [LENOVO, GALAXY, HTC, LG, IPHONE, MAC]
    FULL_NAMES = {
        LENOVO: ['ThinkPad X1 Carbon Gen 10 (14" Intel) Laptop', 'ThinkPad X1 Carbon Gen 11 (14" Intel) Laptop',
                 'ThinkPad P16s Gen 2 (16" AMD) Mobile Workstation', 'ThinkBook 13s Gen 4 (13" AMD) Laptop',
                 'ThinkBook 16 Gen 6 (16" Intel) Laptop', 'Legion Pro 5i Gen 8 (16" Intel) Gaming Laptop',
                 'Legion Slim 5 Gen 8 (16" AMD) Gaming Laptop', 'Yoga 7 (16" AMD) 2-in-1 Laptop',
                 'Yoga 7i (14" Intel) 2 in 1 Laptop', 'Lenovo Slim Pro 7 (14" AMD) Laptop'],
        GALAXY: ['Galaxy Nexus', 'Galaxy Mini', 'Galaxy Y', 'Galaxy Ace', 'Galaxy Ace II', 'Galaxy Gio',
                 'Galaxy W', 'Galaxy S', 'Galaxy S Plus', 'Galaxy S 4g', 'Galaxy S II',
                 'Galaxy S Blaze 4g', 'Galaxy S 3 mini', 'Galaxy Camera', 'Galaxy Q'],
        HTC: ['Evo 3d', 'Sensation', 'Sensation Xe', 'Sensation 4g', 'Amaze 4g'],
        LG: ['Nexus 4', 'Intuition', 'G2x', 'Spectrum'],
        IPHONE: ['APPLE iPhone 8 Plus (Gold, 64 GB)', 'APPLE iPhone 8 Plus (Space Grey, 256 GB)',
                 'APPLE iPhone 8 Plus (Silver, 256 GB)', 'APPLE iPhone 8 (Silver, 256 GB)',
                 'APPLE iPhone SE (Black, 64 GB)', 'APPLE iPhone 11 (White, 64 GB)',
                 'APPLE iPhone 11 Pro Max (Midnight Green, 64 GB)', 'APPLE iPhone 12 Pro Max (Graphite, 128 GB)',
                 'APPLE iPhone 12 (Black, 128 GB)'],
        MAC: ['MacBook Air (M1, 2020)', 'MacBook Pro (13-inch, M1, 2020)',
              'MacBook Pro (14-inch, 2021)', 'MacBook Pro (16-inch, 2021)',
              'MacBook Pro (13-inch, M2, 2022)', 'MacBook Air (13-inch, M2, 2022)',
              'MacBook Pro (14-inch, M2, 2023)', 'MacBook Pro (16-inch, M2, 2023)',
              'MacBook Air (15-inch, M2, 2023)', 'MacBook Pro (14-inch, M3, 2023)',
              'MacBook Pro (16-inch, M3, 2023)']
    }
    ANDROID_VERSIONS = ['Android Pie', 'Android 10', 'Android 11', 'Android 12', 'Android 12L', 'Android 13',
                        'Android 14', 'Android 15']
    OS_VERSIONS = {
        LENOVO: ['Windows 10', 'Windows 11', 'Ubuntu 20', 'Ubuntu 22', 'Ubuntu 23'],
        GALAXY: ANDROID_VERSIONS,
        HTC: ANDROID_VERSIONS,
        LG: ANDROID_VERSIONS,
        IPHONE: ['iOS 13', 'iOS 14', 'iOS 15', 'iOS 16', 'iOS 17'],
        MAC: ['macOS Sonoma', 'macOS Ventura', 'macOS Monterey', 'macOS Big Sur', 'macOS Catalina']
    }

    def __init__(self):
        self.generated_rows = []
        self.already_generated_keys = set()

    def generate_row(self, index: int) -> DataGeneratorEntity:
        logging.debug(f'Generating Device for Index {index}')
        try:
            device_from_index = self.generated_rows[index]
            return device_from_index
        except IndexError:
            device = DeviceEntityGenerator._generate_device(False)
            device_key = DeviceEntityGenerator._generate_device_key(device)
            if device_key in self.already_generated_keys:
                device = DeviceEntityGenerator._generate_device(True)
                device_key = DeviceEntityGenerator._generate_device_key(device)
            self.generated_rows.append(device)
            self.already_generated_keys.add(device_key)
            return device

    @staticmethod
    def _generate_device(was_duplicated: bool) -> Device:
        device_type = random.choice(DeviceEntityGenerator.TYPES)
        full_name = random.choice(DeviceEntityGenerator.FULL_NAMES[device_type])
        os_version = random.choice(DeviceEntityGenerator.OS_VERSIONS[device_type])
        if was_duplicated:
            unique_key = time.time_ns() // 1_000_000
            random_int = random.randint(0, 10000)
            os_version = f'v{unique_key}{random_int}'
        device = Device(
            type=device_type, full_name=full_name, version=os_version
        )
        return device

    @staticmethod
    def _generate_device_key(device: Device) -> str:
        return f'{device.type}_{device.full_name}_{device.version}'
