from assertpy import assert_that

from data_generator.datasets.visits.generator import VisitEntityGenerator


def should_generate_different_visit_for_the_same_index_because_of_the_mutation():
    visits_generator = VisitEntityGenerator('2023-06-10T14:00:12+00:00')

    visit_1 = visits_generator.generate_row(0)
    visit_2 = visits_generator.generate_row(0)

    assert_that(visit_1).is_not_equal_to(visit_2)
    assert_that(visit_1.visit_id).is_equal_to(visit_2.visit_id)
    assert_that(visit_1.event_time).is_before(visit_2.event_time)
    # Do not assert on the page as we can generate the same page and it's not a bug
    # It would mean the next tracking info was sent when the user was still on the previous page
    # assert_that(visit_1.page).is_not_equal_to(visit_2.page)


def should_generate_different_visits_for_different_indexes():
    visits_generator = VisitEntityGenerator('2023-06-10T14:00:12+00:00')

    visit_1 = visits_generator.generate_row(0)
    visit_2 = visits_generator.generate_row(1)

    assert_that(visit_1).is_not_equal_to(visit_2)


def should_generate_complete_visit():
    visits_generator = VisitEntityGenerator('2023-06-10T14:00:12+00:00')

    has_referral = False
    has_ad = False
    has_connected_since = False
    for i in range(0, 100):
        visit = visits_generator.generate_row(i)

        assert_that(visit.visit_id).is_not_empty()
        assert_that(visit.user_id).is_not_empty()
        assert_that(visit.event_time).is_not_none()
        assert_that(visit.keep_private).is_not_none()
        assert_that(visit.page).is_not_empty()
        assert_that(visit.context).is_not_none()
        if visit.context.referral:
            has_referral = True
        if visit.context.ad_id:
            has_ad = True
        assert_that(visit.context.user).is_not_none()
        assert_that(visit.context.user.ip).is_not_empty()
        assert_that(visit.context.user.login).is_not_empty()
        if visit.context.user.connected_since:
            has_connected_since = True
        assert_that(visit.context.technical).is_not_none()
        assert_that(visit.context.technical.browser).is_not_empty()
        assert_that(visit.context.technical.browser_version).is_not_empty()
        assert_that(visit.context.technical.network_type).is_not_empty()
        assert_that(visit.context.technical.device_type).is_not_empty()
        assert_that(visit.context.technical.device_version).is_not_empty()

    assert_that(has_referral).is_true()
    assert_that(has_ad).is_true()
    assert_that(has_connected_since).is_true()
