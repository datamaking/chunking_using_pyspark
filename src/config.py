from pyspark.sql import SparkSession

# Spark Configuration
SPARK_CONFIG = {
    "spark.app.name": "Document Processing Pipeline",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "2",
    "spark.driver.maxResultSize": "2g"
}

# Processing Configuration
CHUNK_SIZE = 1000  # Number of documents per chunk
MAX_PARTITIONS = 100
BATCH_SIZE = 10000

# Supported file types
SUPPORTED_EXTENSIONS = {
    "pdf": ".pdf",
    "html": [".html", ".htm"],
    "text": [".txt", ".csv", ".json"]
}

# Logging Configuration
LOG_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO"
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "default",
            "filename": "pipeline.log",
            "level": "DEBUG"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"]
    }
} 