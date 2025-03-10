from abc import ABC, abstractmethod
from typing import List, Iterator
from pyspark.sql import DataFrame
from src.utils.logger import setup_logger

class ChunkStrategy(ABC):
    """Abstract base class for chunking strategies"""
    
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
    
    @abstractmethod
    def create_chunks(self, df: DataFrame) -> Iterator[DataFrame]:
        """Create chunks from the input DataFrame"""
        pass

class SizeBasedChunkStrategy(ChunkStrategy):
    """Chunk documents based on size"""
    
    def __init__(self, chunk_size: int):
        super().__init__()
        self.chunk_size = chunk_size
    
    def create_chunks(self, df: DataFrame) -> Iterator[DataFrame]:
        try:
            total_count = df.count()
            num_partitions = (total_count + self.chunk_size - 1) // self.chunk_size
            
            return df.repartition(num_partitions).rdd.toLocalIterator()
        except Exception as e:
            self.logger.error(f"Error in size-based chunking: {str(e)}")
            raise

class ContentBasedChunkStrategy(ChunkStrategy):
    """Chunk documents based on content similarity"""
    
    def __init__(self, similarity_threshold: float):
        super().__init__()
        self.similarity_threshold = similarity_threshold
    
    def create_chunks(self, df: DataFrame) -> Iterator[DataFrame]:
        try:
            # Implement content-based chunking logic here
            # This could involve TF-IDF, clustering, etc.
            pass
        except Exception as e:
            self.logger.error(f"Error in content-based chunking: {str(e)}")
            raise 