# Document Processing Pipeline with PySpark NLP

This project implements a scalable document processing pipeline using PySpark for processing millions of documents (PDF, HTML, Text) with different chunking strategies.

## Project Structure 

project/
├── src/
│ ├── init.py
│ ├── config.py
│ ├── processors/
│ │ ├── init.py
│ │ ├── base_processor.py
│ │ ├── pdf_processor.py
│ │ ├── html_processor.py
│ │ └── text_processor.py
│ ├── tokenizers/
│ │ ├── init.py
│ │ ├── sentence_tokenizer.py
│ │ └── word_tokenizer.py
│ ├── chunking/
│ │ ├── init.py
│ │ ├── chunk_strategy.py
│ │ └── chunk_processor.py
│ └── utils/
│ ├── init.py
│ ├── logger.py
│ └── spark_utils.py
├── requirements.txt
└── main.py

This implementation includes:

1. Proper project structure following Python best practices
2. Abstract base classes and inheritance for processors and chunking strategies
3. Comprehensive error handling and logging
4. Different chunking strategies (size-based and content-based)
5. Sentence and word tokenization using NLTK
6. Configuration management
7. Utility functions for Spark session management and logging
8. PDF processing capabilities
9. Main execution script with argument parsing

The code follows several design patterns:
- Strategy Pattern (for chunking strategies)
- Template Method Pattern (in base processor)
- Factory Pattern (for creating processors)
- Singleton Pattern (for Spark session and logger)

To extend this for HTML and text documents, you would create similar processor classes inheriting from BaseProcessor with specific implementation details for those file types.

The pipeline is scalable and can handle millions of documents through:
1. Proper chunking strategies
2. Spark's distributed processing
3. Efficient memory management
4. Configurable batch processing

Remember to adjust the Spark configuration parameters in config.py based on your cluster resources and requirements. 