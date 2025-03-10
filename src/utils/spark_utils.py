from pyspark.sql import SparkSession
from src.config import SPARK_CONFIG
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session.
    
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        builder = SparkSession.builder
        for key, value in SPARK_CONFIG.items():
            builder = builder.config(key, value)
            
        spark = builder.getOrCreate()
        logger.info("Successfully created Spark session")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise 