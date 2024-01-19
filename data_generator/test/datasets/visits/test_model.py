import datetime

from assertpy import assert_that

from data_generator.datasets.visits.model import Visit, VisitContext, UserContext, TechnicalContext


def should_return_visit_as_dict():
    visit = Visit(visit_id='visit 1',
                  event_time=datetime.datetime(year=2023, month=10, day=30, hour=18, minute=38, second=55),
                  user_id='user 1', keep_private=True, page='home.html',
                  context=VisitContext(
                      referral='google.com', ad_id='ad 1',
                      user=UserContext(
                          ip='127.0.0.1', login=None,
                          connected_since=datetime.datetime(year=2023, month=10, day=30, hour=18, minute=20, second=55)
                      ),
                      technical=TechnicalContext(
                          browser='Firefox', browser_version='1beta', network_type='wifi',
                          device_type='pc', device_version='Windows 10'
                      )
                  ))

    visit_dict = visit.as_dict()

    assert_that(visit_dict).is_equal_to({'context': {
        'referral': 'google.com', 'ad_id': 'ad 1',
        'user': {'ip': '127.0.0.1', 'login': None,
                 'connected_since': datetime.datetime(2023, 10, 30, 18, 20, 55)},
        'technical': {
            'browser': 'Firefox', 'browser_version': '1beta', 'network_type': 'wifi', 'device_type': 'pc',
            'device_version': 'Windows 10'}
    }, 'event_time': datetime.datetime(2023, 10, 30, 18, 38, 55),
        'keep_private': True, 'page': 'home.html', 'user_id': 'user 1',
        'visit_id': 'visit 1'})


def should_return_partition_key_for_the_visit():
    visit = Visit(visit_id='visit 1',
                  event_time=datetime.datetime(year=2023, month=10, day=30, hour=18, minute=38, second=55),
                  user_id='user 1', keep_private=True, page='home.html',
                  context=VisitContext(
                      referral='google.com', ad_id='ad 1',
                      user=UserContext(
                          ip='127.0.0.1', login=None,
                          connected_since=datetime.datetime(year=2023, month=10, day=30, hour=18, minute=20, second=55)
                      ),
                      technical=TechnicalContext(
                          browser='Firefox', browser_version='1beta', network_type='wifi',
                          device_type='pc', device_version='Windows 10'
                      )
                  ))

    partition_key = visit.entity_partition_key()

    assert_that(partition_key).is_equal_to('visit 1')
