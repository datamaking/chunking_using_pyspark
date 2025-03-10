from typing import List, Iterator, Tuple
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from src.chunking.chunk_strategy import ChunkStrategy
from src.utils.logger import setup_logger

class SemanticChunkStrategy(ChunkStrategy):
    """
    Chunk strategy that uses semantic similarity and overlap to create meaningful chunks.
    Uses sentence transformers for semantic similarity and implements sliding window with overlap.
    """
    
    def __init__(self, 
                 chunk_size: int = 1000,
                 overlap_size: int = 200,
                 similarity_threshold: float = 0.7,
                 model_name: str = 'all-MiniLM-L6-v2'):
        """
        Initialize semantic chunking strategy.
        
        Args:
            chunk_size: Maximum size of each chunk
            overlap_size: Number of overlapping documents between chunks
            similarity_threshold: Threshold for semantic similarity (0-1)
            model_name: Name of the sentence transformer model to use
        """
        super().__init__()
        self.chunk_size = chunk_size
        self.overlap_size = overlap_size
        self.similarity_threshold = similarity_threshold
        try:
            self.model = SentenceTransformer(model_name)
        except Exception as e:
            self.logger.error(f"Failed to load sentence transformer model: {str(e)}")
            raise
            
    def _compute_embeddings(self, texts: List[str]) -> np.ndarray:
        """Compute embeddings for a list of texts"""
        try:
            return self.model.encode(texts, show_progress_bar=False)
        except Exception as e:
            self.logger.error(f"Error computing embeddings: {str(e)}")
            raise
            
    def _find_semantic_boundary(self, 
                              embeddings: np.ndarray, 
                              start_idx: int, 
                              end_idx: int) -> int:
        """
        Find the best semantic boundary for chunk split within the overlap region.
        
        Args:
            embeddings: Document embeddings
            start_idx: Start index of overlap region
            end_idx: End index of overlap region
            
        Returns:
            Index of best split point
        """
        try:
            if start_idx >= end_idx:
                return end_idx
                
            # Calculate similarities between adjacent documents in overlap region
            similarities = []
            for i in range(start_idx, end_idx - 1):
                sim = cosine_similarity(
                    embeddings[i].reshape(1, -1),
                    embeddings[i + 1].reshape(1, -1)
                )[0][0]
                similarities.append((i, sim))
            
            # Find point of lowest similarity (natural boundary)
            split_idx, _ = min(similarities, key=lambda x: x[1])
            return split_idx + 1
            
        except Exception as e:
            self.logger.error(f"Error finding semantic boundary: {str(e)}")
            return end_idx
            
    def _create_semantic_chunks(self, 
                              texts: List[str], 
                              embeddings: np.ndarray) -> List[List[str]]:
        """
        Create chunks using semantic similarity and overlap.
        
        Args:
            texts: List of document texts
            embeddings: Pre-computed embeddings for texts
            
        Returns:
            List of chunks, where each chunk is a list of documents
        """
        try:
            chunks = []
            start_idx = 0
            
            while start_idx < len(texts):
                # Calculate end index for current chunk
                end_idx = min(start_idx + self.chunk_size, len(texts))
                
                # If this isn't the last chunk, find semantic boundary
                if end_idx < len(texts):
                    overlap_start = end_idx - self.overlap_size
                    split_idx = self._find_semantic_boundary(
                        embeddings, overlap_start, end_idx
                    )
                else:
                    split_idx = end_idx
                
                # Create chunk and add to results
                chunk = texts[start_idx:split_idx]
                chunks.append(chunk)
                
                # Update start index for next chunk
                start_idx = split_idx
                
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error creating semantic chunks: {str(e)}")
            raise
            
    def create_chunks(self, df: DataFrame) -> Iterator[DataFrame]:
        """
        Create semantically meaningful chunks from the input DataFrame.
        
        Args:
            df: Input DataFrame with 'text' column
            
        Returns:
            Iterator of DataFrame chunks
        """
        try:
            # Collect texts for processing
            texts = [row.text for row in df.select('text').collect()]
            
            if not texts:
                self.logger.warning("No texts found in DataFrame")
                return iter([])
            
            # Compute embeddings
            embeddings = self._compute_embeddings(texts)
            
            # Create semantic chunks
            chunks = self._create_semantic_chunks(texts, embeddings)
            
            # Convert chunks back to DataFrames
            for chunk_texts in chunks:
                # Filter original DataFrame to get rows for current chunk
                chunk_df = df.filter(col('text').isin(chunk_texts))
                yield chunk_df
                
        except Exception as e:
            self.logger.error(f"Error in semantic chunking: {str(e)}")
            raise 