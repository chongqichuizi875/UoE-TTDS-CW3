from typing import List
from abc import ABC, abstractmethod

class DBInterface(ABC):
    def __init__(self) -> None:
        pass
    @abstractmethod
    def get_page_by_page_id(self, id:str):
        raise NotImplementedError()
    
    @abstractmethod
    def get_pages_by_page_id(self, ids:List[str]):
        raise NotImplementedError()
    
    @abstractmethod
    def get_indexed_pages_by_token(self, token: str, skip:int, limit:int):
        raise NotImplementedError()