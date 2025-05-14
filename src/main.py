from pyspark.sql import SparkSession
from src.etl.bronze import load_bronze
from src.etl.silver import clean_data
from src.etl.gold import aggregate_data
from src.ml.model import train_model
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting FlightLakehouse pipeline")
    
    spark = SparkSession.builder \
        .appName("FlightLakehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        bronze_df = load_bronze(spark)
        logger.info("Bronze layer loaded")
        
        silver_df = clean_data(spark, bronze_df)
        logger.info("Silver layer loaded")
        
        gold_df = aggregate_data(spark, silver_df)
        logger.info("Gold layer loaded")
        
        train_model(spark, gold_df)
        logger.info("Model trained and logged")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()