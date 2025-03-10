from typing import List
import nltk
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from src.utils.logger import setup_logger

class WordTokenizer:
    """Word tokenizer using NLTK"""
    
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        try:
            nltk.download('punkt')
        except Exception as e:
            self.logger.error(f"Failed to download NLTK data: {str(e)}")
            raise
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into words.
        
        Args:
            text: Input text to tokenize
            
        Returns:
            List of words
        """
        try:
            if not text or not isinstance(text, str):
                return []
            return nltk.word_tokenize(text)
        except Exception as e:
            self.logger.error(f"Error in word tokenization: {str(e)}")
            return []
    
    def get_tokenize_udf(self):
        """Return a UDF for word tokenization"""
        return udf(self.tokenize, ArrayType(StringType())) 