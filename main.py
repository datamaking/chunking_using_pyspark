import argparse
from src.utils.spark_utils import create_spark_session
from src.utils.logger import setup_logger
from src.processors.pdf_processor import PDFProcessor
from src.chunking.semantic_chunk_strategy import SemanticChunkStrategy
from src.tokenizers.sentence_tokenizer import SentenceTokenizer
from src.tokenizers.word_tokenizer import WordTokenizer

def parse_args():
    parser = argparse.ArgumentParser(description="Document Processing Pipeline")
    parser.add_argument("--input-path", required=True, help="Input path for documents")
    parser.add_argument("--output-path", required=True, help="Output path for processed documents")
    return parser.parse_args()

def main():
    logger = setup_logger("main")
    args = parse_args()
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Initialize components
        pdf_processor = PDFProcessor()
        chunk_strategy = SemanticChunkStrategy(
            chunk_size=1000,
            overlap_size=200,
            similarity_threshold=0.7
        )
        sentence_tokenizer = SentenceTokenizer()
        word_tokenizer = WordTokenizer()
        
        # Process documents
        df = pdf_processor.load(args.input_path)
        
        # Apply chunking
        for chunk in chunk_strategy.create_chunks(df):
            # Process chunk
            processed_chunk = pdf_processor.process(chunk)
            
            # Apply tokenization
            processed_chunk = processed_chunk \
                .withColumn("sentences", sentence_tokenizer.get_tokenize_udf()(col("text"))) \
                .withColumn("words", word_tokenizer.get_tokenize_udf()(col("text")))
            
            # Save results
            pdf_processor.save(processed_chunk, args.output_path)
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 