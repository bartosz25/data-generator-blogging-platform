from abc import ABC, abstractmethod


class DatasetGenerator(ABC):

    @abstractmethod
    def should_continue(self) -> bool: raise NotImplementedError


class OneShotDatasetGenerator(DatasetGenerator):

    def __init__(self):
        self._already_ran = False

    def should_continue(self) -> bool:
        if self._already_ran:
            return False
        self._already_ran = True
        return True


class FixedTimesDatasetGenerator(DatasetGenerator):

    def __init__(self, all_runs: int):
        self.remaining_runs = all_runs

    def should_continue(self) -> bool:
        self.remaining_runs -= 1
        return self.remaining_runs >= 0


class ContinuousDatasetGenerator(DatasetGenerator):

    def should_continue(self) -> bool:
        return True
