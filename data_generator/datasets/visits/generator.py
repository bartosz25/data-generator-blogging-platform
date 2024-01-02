import dataclasses
import datetime
import logging
import random
import threading
import uuid
from typing import List

from faker import Faker

from data_generator.datasets.data_generator_entity import EntityGenerator, DataGeneratorEntity
from data_generator.datasets.visits.model import Visit, VisitContext, UserContext, TechnicalContext


class VisitEntityMutator:

    @staticmethod
    def mutate_visit(visit_to_mutate: Visit) -> Visit:
        mutated_visit = dataclasses.replace(visit_to_mutate)
        if not visit_to_mutate.event_time:
            # It's maybe one of the data quality issues; we don't mutate event time
            return mutated_visit
        # TODO: ; compare with NOW
        if random.randint(0, 300) == 0:
            mutated_visit.event_time = (visit_to_mutate.event_time +
                                        datetime.timedelta(seconds=random.randint(1, 30)))
        else:
            mutated_visit.event_time = (visit_to_mutate.event_time +
                                        datetime.timedelta(minutes=random.randint(1, 5)))
        return mutated_visit


@dataclasses.dataclass
class VisitEntityGeneratorWrapper:
    visit: Visit
    visit_remaining_refreshes: int


class VisitEntityGenerator(EntityGenerator):
    PAGES = ['index', 'contact', 'home', 'main', 'about', 'categories', 'category_static',
             'page_static']
    REFERRALS = ['Google Search', 'Twitter', 'Facebook', 'LinkedIn', 'YouTube', 'Medium',
                 'Google Ads', 'StackOverflow', None]

    def __init__(self, start_time: str):
        self.generated_rows: List[VisitEntityGeneratorWrapper] = []
        self.start_time_as_datetime = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
        self.faker_generator = Faker()

    def generate_row(self, index: int) -> DataGeneratorEntity:
        logging.debug(f'Generating Visit for Index {index}')
        page = random.choice(VisitEntityGenerator.PAGES)
        if page.endswith('_static'):
            page = page.replace('_static', f'_{random.randint(1, 20)}')
        try:
            visit_wrapper = self.generated_rows[index]
            if visit_wrapper.visit_remaining_refreshes > 0:
                mutated_visit = VisitEntityMutator.mutate_visit(visit_wrapper.visit)
                mutated_visit.page = page
                remaining_refreshes = visit_wrapper.visit_remaining_refreshes - 1
                visit_wrapper = VisitEntityGeneratorWrapper(
                    visit=mutated_visit,
                    visit_remaining_refreshes=remaining_refreshes
                )
            else:
                visit_wrapper = self._start_new_visit(page=page)
            self.generated_rows[index] = visit_wrapper
            return visit_wrapper.visit
        except IndexError:
            visit_wrapper = self._start_new_visit(page=page)
            self.generated_rows.append(visit_wrapper)
            return visit_wrapper.visit

    def _start_new_visit(self, page: str) -> VisitEntityGeneratorWrapper:
        keep_private = random.choice([True, False, False, False, False, False, False])
        visit_id = f'{threading.current_thread().ident}_{len(self.generated_rows)}'
        user_id = f'{threading.current_thread().ident}_{str(uuid.uuid4())}'

        ads_ids = [None, None, None, 'ad 1', 'ad 2', 'ad 3', 'ad 4', 'ad 5']
        connection_date = [None, None,
                           (self.start_time_as_datetime - datetime.timedelta(days=random.randint(1, 30)))]
        visit_context = VisitContext(
            referral=random.choice(VisitEntityGenerator.REFERRALS),
            ad_id=random.choice(ads_ids),
            user=UserContext(
                ip=self.faker_generator.ipv4(),
                login=self.faker_generator.simple_profile()['username'],
                connected_since=random.choice(connection_date)
            ),
            technical=TechnicalContext(
                browser=random.choice(['Chrome', 'Firefox', 'Safari']),
                browser_version=random.choice(['20.0', '20.1', '20.2', '21.0', '22.0', '23.0', '23.1', '23.2',
                                               '24.11', '25.09']),
                network_type=random.choice(['5G', '4G', 'Wi-Fi', 'LAN']),
                device_type=random.choice(['PC', 'MacBook', 'iPad', 'Smartphone', 'iPhone']),
                device_version=random.choice(['1.0', '2.0', '3.0', '4.0', '5.0', '6.0'])
            )
        )
        visit = Visit(
            visit_id=visit_id, event_time=self.start_time_as_datetime, user_id=user_id,
            keep_private=keep_private, page=page,
            context=visit_context
        )
        return VisitEntityGeneratorWrapper(
            visit=visit, visit_remaining_refreshes=random.randint(5, 500)
        )
