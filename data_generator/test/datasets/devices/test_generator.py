from assertpy import assert_that

from data_generator.datasets.devices.generator import DeviceEntityGenerator


def should_generate_device_only_once():
    devices_generator = DeviceEntityGenerator()

    device_1 = devices_generator.generate_row(0)
    device_2 = devices_generator.generate_row(0)

    assert_that(device_1).is_equal_to(device_2)


def should_generate_different_devices_for_different_indexes():
    devices_generator = DeviceEntityGenerator()

    device_1 = devices_generator.generate_row(0)
    device_2 = devices_generator.generate_row(1)

    assert_that(device_1).is_not_equal_to(device_2)


def should_generate_complete_device():
    devices_generator = DeviceEntityGenerator()

    device = devices_generator.generate_row(0)

    # we can't really assert the properties as they're generated randomly
    assert_that(device.type).is_not_empty()
    assert_that(device.full_name).is_not_empty()
    assert_that(device.version).is_not_empty()