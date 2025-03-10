from abc import ABC, abstractmethod
from typing import List, Any
from pyspark.sql import DataFrame
from src.utils.logger import setup_logger

class BaseProcessor(ABC):
    """Abstract base class for document processors"""
    
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
    
    @abstractmethod
    def load(self, path: str) -> DataFrame:
        """Load documents from the specified path"""
        pass
    
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        """Process the documents"""
        pass
    
    @abstractmethod
    def save(self, df: DataFrame, output_path: str) -> None:
        """Save the processed documents"""
        pass 