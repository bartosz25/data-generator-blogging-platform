import json
import os
import pathlib
import shutil

from assertpy import assert_that

from data_generator import generate_dataset


def should_generate_dataset_with_reference_datasets():
    output_paths = ['/tmp/dataset-1-users', '/tmp/dataset-2-devices', '/tmp/dataset-3-visits']
    for output_path in output_paths:
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path, exist_ok=True)

    path = pathlib.Path(__file__).parent.absolute()
    config_path = f'{path}/test_generate_datasets_with_reference_datasets.yaml'

    generate_dataset.main(['--config_file', config_path])

    # assert users
    generated_users = set()
    with open(f'{output_paths[0]}/dataset.json') as json_file:
        all_rows = 0
        for line in json_file.readlines():
            json_data = json.loads(line.strip())
            connection_datetime = json_data['last_connection_datetime']
            if not connection_datetime:
                connection_datetime = ''
            generated_users.add(json_data['id'] + json_data['login'] + connection_datetime)
            assert_that(json_data['id']).is_not_empty()
            assert_that(json_data['login']).is_not_empty()
            assert_that(json_data['registered_datetime']).is_not_empty()
            # last connection is not required, can be empty; generator is random, so we don't assert it here
            all_rows += 1
        assert_that(all_rows).is_equal_to(3)

    # assert devices
    generated_devices = set()
    with open(f'{output_paths[1]}/dataset.json') as json_file:
        all_rows = 0
        for line in json_file.readlines():
            json_data = json.loads(line.strip())
            generated_devices.add(json_data['type'] + json_data['version'])
            assert_that(json_data['type']).is_not_empty()
            assert_that(json_data['full_name']).is_not_empty()
            assert_that(json_data['version']).is_not_empty()
            all_rows += 1
        assert_that(all_rows).is_equal_to(5)

    # assert visits
    # all visits should have devices from the devices dataset
    # only 3 first should reference the users
    with open(f'{output_paths[2]}/dataset.json') as json_file:
        all_rows = 0
        expected_fields = ['visit_id', 'event_time', 'user_id', 'keep_private', 'page', 'context']
        for line in json_file.readlines():
            json_data = json.loads(line.strip())
            for expected_field in expected_fields:
                assert_that(json_data[expected_field]).is_not_none()
                user_data = json_data['context']['user']
                if 'connected_since' not in user_data:
                    connected_since = ''
                else:
                    connected_since = user_data['connected_since']
                device_key = (json_data['context']['technical']['device_type'] +
                              json_data['context']['technical']['device_version'])
                assert_that(generated_devices).contains(device_key)
                user_key = json_data['user_id'] + json_data['context']['user']['login'] + connected_since
                if all_rows < 3:
                    assert_that(generated_users).contains(user_key)
                else:
                    assert_that(generated_users).does_not_contain(user_key)
            all_rows += 1
        assert_that(all_rows).is_equal_to(5)
