from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col
from delta.tables import DeltaTable
import logging
import os

def aggregate_data(spark: SparkSession, silver_df):
    logging.info("Processing data for Gold layer")
    
    try:
        gold_df = silver_df.groupBy("AIRLINE", "ORIGIN_AIRPORT").agg(
            avg("DEPARTURE_DELAY").alias("avg_departure_delay"),
            avg("ARRIVAL_DELAY").alias("avg_arrival_delay"),
            count("*").alias("flight_count")
        )
        
        gold_df.cache()
        
        gold_path = "file:/app/data/gold/flights"
        os.makedirs(os.path.dirname(gold_path.replace("file:/", "/")), exist_ok=True)
        
        gold_df.write.format("delta").mode("overwrite").save(gold_path)
        
        # Проверка, что Delta table существует
        if DeltaTable.isDeltaTable(spark, gold_path):
            spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (AIRLINE)")
            logging.info(f"Gold Delta table optimized at {gold_path}")
        else:
            logging.error(f"No Delta table found at {gold_path}")
            raise ValueError(f"No Delta table found at {gold_path}")
        
        logging.info(f"Gold data written to {gold_path}")
        return gold_df
    
    except Exception as e:
        logging.error(f"Failed to process Gold data: {str(e)}")
        raise