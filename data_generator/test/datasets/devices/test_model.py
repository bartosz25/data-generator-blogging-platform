from assertpy import assert_that

from data_generator.datasets.devices.model import Device


def should_return_device_as_json():
    device = Device(type='mac', full_name='Mac', version='12.0')

    device_dict = device.as_dict()

    assert_that(device_dict).is_equal_to({'type': 'mac', 'full_name': 'Mac', 'version': '12.0'})


def should_return_partition_key_for_the_device():
    device = Device(type='mac', full_name='Mac', version='12.0')

    partition_key = device.partition_key()

    assert_that(partition_key).is_equal_to('mac')
