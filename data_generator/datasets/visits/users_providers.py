import datetime
import random
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Tuple

from faker import Faker

from data_generator.datasets.users.generator import RegisteredUserEntityGenerator
from data_generator.datasets.visits.model import UserContext


class UserContextProvider(ABC):

    def __init__(self):
        self.faker_generator = Faker()

    @abstractmethod
    def provide(self, index: int, visits_start_time: datetime.datetime) -> Tuple[UserContext, str]:
        raise NotImplementedError


class RandomUserContextProvider(UserContextProvider):

    def provide(self, index: int, visits_start_time: datetime.datetime) -> Tuple[UserContext, str]:
        connection_date = [None, None,
                           (visits_start_time - datetime.timedelta(days=random.randint(1, 30)))]

        random_user_context = UserContext(
            ip=self.faker_generator.ipv4(),
            login=self.faker_generator.simple_profile()['username'],
            connected_since=random.choice(connection_date)
        )
        user_id = f'{threading.current_thread().ident}_{str(uuid.uuid4())}'
        return random_user_context, user_id


class UserContextWithUserProvider(UserContextProvider):

    def __init__(self, user_entity_generator: RegisteredUserEntityGenerator):
        super().__init__()
        self.user_entity_generator = user_entity_generator
        self.last_accessed_user = 0
        self.random_context_provider = RandomUserContextProvider()

    def provide(self, index: int, visits_start_time: datetime.datetime) -> Tuple[UserContext, str]:
        generated_users = self.user_entity_generator.get_generated_users()
        if index >= len(generated_users):
            # It means we have more visits than users, and we should consider all additional users as guests
            return self.random_context_provider.provide(index, visits_start_time)

        registered_user = generated_users[index]
        registered_user_context = UserContext(
            ip=self.faker_generator.ipv4(),
            login=registered_user.login,
            connected_since=registered_user.last_connection_datetime
        )
        return registered_user_context, registered_user.id
