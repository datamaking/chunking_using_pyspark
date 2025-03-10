from typing import List
import pdfplumber
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from src.processors.base_processor import BaseProcessor

class PDFProcessor(BaseProcessor):
    """Processor for PDF documents"""
    
    def load(self, path: str) -> DataFrame:
        try:
            spark = self.spark
            pdf_files = spark.read.format("binaryFile") \
                .option("pathGlobFilter", "*.pdf") \
                .load(path)
            return pdf_files
        except Exception as e:
            self.logger.error(f"Error loading PDF files: {str(e)}")
            raise
    
    def extract_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF content"""
        try:
            with pdfplumber.open(pdf_content) as pdf:
                return "\n".join(page.extract_text() for page in pdf.pages)
        except Exception as e:
            self.logger.error(f"Error extracting text from PDF: {str(e)}")
            return ""
    
    def process(self, df: DataFrame) -> DataFrame:
        try:
            extract_text_udf = udf(self.extract_text, StringType())
            return df.withColumn("text", extract_text_udf(col("content")))
        except Exception as e:
            self.logger.error(f"Error processing PDF documents: {str(e)}")
            raise
    
    def save(self, df: DataFrame, output_path: str) -> None:
        try:
            df.write.mode("overwrite").parquet(output_path)
        except Exception as e:
            self.logger.error(f"Error saving processed PDF documents: {str(e)}")
            raise 