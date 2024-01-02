from assertpy import assert_that


def assert_visits_consistency(action_1, action_2):
    assert_that(action_1).is_not_equal_to(action_2)

    assert_that(action_1['visit_id']).is_equal_to(action_2['visit_id'])

    # pages sequence
    action_1_pages = action_1['page']
    action_2_pages = action_2['page']
    assert_that(action_1_pages['current']).is_equal_to(action_2_pages['previous'])
    assert_that(action_1_pages['previous']).is_none()
    assert_that(action_2_pages['current']).is_not_none()

    # normally all fields except the "event_time" should be the same
    def create_comparable_copy(action):
        del action['event_time']
        del action['page']
        return action

    action_1_comparable = create_comparable_copy(action_1.copy())
    action_2_comparable = create_comparable_copy(action_2.copy())
    assert_that(action_1_comparable).is_equal_to(action_2_comparable)
