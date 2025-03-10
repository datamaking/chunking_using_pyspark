from typing import List
import nltk
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from src.utils.logger import setup_logger

class SentenceTokenizer:
    """Sentence tokenizer using NLTK"""
    
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        try:
            nltk.download('punkt')
        except Exception as e:
            self.logger.error(f"Failed to download NLTK data: {str(e)}")
            raise
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into sentences.
        
        Args:
            text: Input text to tokenize
            
        Returns:
            List of sentences
        """
        try:
            if not text or not isinstance(text, str):
                return []
            return nltk.sent_tokenize(text)
        except Exception as e:
            self.logger.error(f"Error in sentence tokenization: {str(e)}")
            return []
    
    def get_tokenize_udf(self):
        """Return a UDF for sentence tokenization"""
        return udf(self.tokenize, ArrayType(StringType())) 