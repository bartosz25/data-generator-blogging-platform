import datetime
import logging
import random
import threading
import uuid

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.datasets.generation_context import EntityGenerator
from data_generator.datasets.users.model import RegisteredUser


class RegisteredUserEntityGenerator(EntityGenerator):
    def __init__(self):
        self.generated_rows = []

    def generate_row(self, index: int) -> DataGeneratorEntity:
        logging.debug(f'Generating User for Index {index}')
        try:
            user_from_index = self.generated_rows[index]
            return user_from_index
        except IndexError:
            unique_id = threading.current_thread().ident + len(self.generated_rows)
            login = f'user_{unique_id}'
            email = f'{login}@abcdefghijklmnop.com'
            registered_date = datetime.datetime.today() - datetime.timedelta(days=
                                                                             random.randint(0, 365 * 2))
            # TODO: handle the case when the first_connection > today()
            first_connection_date = None
            last_connection_date = None
            if random.choice([True, False]):
                first_connection_date = registered_date + datetime.timedelta(minutes=random.randint(1, 60*24*6))
                last_connection_date = datetime.datetime.today() - datetime.timedelta(minutes=random.randint(1, 60*24*20))
                # TODO: check if last is always after the first and they don't overlap with today()!

            registered_user = RegisteredUser(
                id=str(uuid.uuid4()), login=login, email=email, registered_datetime=registered_date,
                first_connection_datetime=first_connection_date,
                last_connection_datetime=last_connection_date
            )
            self.generated_rows.append(registered_user)
            return registered_user
