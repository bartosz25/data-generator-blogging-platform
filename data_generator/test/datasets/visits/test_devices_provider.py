from assertpy import assert_that

from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.visits.devices_providers import DeviceTechnicalContextProvider, \
    RandomTechnicalContextProvider


def should_generate_device_randomly():
    devices_provider = RandomTechnicalContextProvider()

    context = devices_provider.provide()

    assert_that(context.device_type).is_not_empty()
    assert_that(context.device_version).is_not_empty()
    assert_that(context.browser).is_not_empty()
    assert_that(context.browser_version).is_not_empty()
    assert_that(context.network_type).is_not_empty()


def should_generate_device_from_devices_generator():
    devices_entity_generator = DeviceEntityGenerator()
    device = devices_entity_generator.generate_row(0)
    devices_provider = DeviceTechnicalContextProvider(devices_entity_generator)

    context = devices_provider.provide()

    assert_that(context.device_type).is_equal_to(device.type)
    assert_that(context.device_version).is_equal_to(device.version)
    assert_that(context.browser).is_not_empty()
    assert_that(context.browser_version).is_not_empty()
    assert_that(context.network_type).is_not_empty()
