from abc import ABC, abstractmethod
from models.opinion_dto import OpinionDto

class IExtractor(ABC):
    @abstractmethod
    def extract(self) -> list[OpinionDto]:
        pass