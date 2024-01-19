from assertpy import assert_that

from data_generator.datasets.devices.model import Device


def should_return_device_as_json():
    device = Device(type='mac', full_name='Mac', version='12.0')

    device_dict = device.as_dict()

    assert_that(device_dict).is_equal_to({'type': 'mac', 'full_name': 'Mac', 'version': '12.0'})


def should_return_partition_key_for_the_device():
    device = Device(type='mac', full_name='Mac', version='12.0')

    partition_key = device.entity_partition_key()

    assert_that(partition_key).is_equal_to('mac')


def should_return_default_partition_key_for_the_device_without_type():
    device = Device(type=None, full_name='Mac', version='12.0')

    partition_key = device.entity_partition_key()

    assert_that(partition_key).is_none()
    assert_that(device.partition_key()).is_not_none()
    assert_that(device.partition_key()).is_not_empty()
    uuid4_pattern = '[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}'
    assert_that(device.partition_key()).matches(uuid4_pattern)
