from assertpy import assert_that

from data_generator.datasets.dataset_generators import (OneShotDatasetGenerationController,
                                                        FixedTimesDatasetGenerationController,
                                                        ContinuousDatasetGenerationController)


def should_run_only_once_for_the_OneShotDatasetGenerator():
    generator = OneShotDatasetGenerationController()

    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_false()
    assert_that(generator.should_continue()).is_false()


def should_run_3_times_for_FixedTimesDatasetGenerator():
    generator = FixedTimesDatasetGenerationController(3)

    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_false()


def should_run_continuously_for_ContinuousDatasetGenerator():
    generator = ContinuousDatasetGenerationController()

    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
    assert_that(generator.should_continue()).is_true()
