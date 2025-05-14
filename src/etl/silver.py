from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import logging
import os

def clean_data(spark: SparkSession, bronze_df):
    logging.info("Processing data for Silver layer")
    
    try:
        silver_df = bronze_df.select(
            "AIRLINE", "ORIGIN_AIRPORT", 
            "DEPARTURE_DELAY", "ARRIVAL_DELAY",
        ).na.fill({
            "DEPARTURE_DELAY": 0,
            "ARRIVAL_DELAY": 0,
        })
        
        silver_df = silver_df.filter(
            (col("DEPARTURE_DELAY") >= 0) & 
            (col("ARRIVAL_DELAY") >= 0) & 
            (col("AIRLINE").isNotNull())
        )
        
        silver_df = silver_df.repartition(8, "AIRLINE")
        

        silver_path = "file:/app/data/silver/flights"
        os.makedirs(os.path.dirname(silver_path.replace("file:/", "/")), exist_ok=True)
        
        silver_df.write.format("delta").mode("overwrite").partitionBy("AIRLINE").save(silver_path)
        
        # Проверка, что Delta table существует
        if DeltaTable.isDeltaTable(spark, silver_path):
            spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (ORIGIN_AIRPORT)")
            logging.info(f"Silver Delta table optimized at {silver_path}")
        else:
            logging.error(f"No Delta table found at {silver_path}")
            raise ValueError(f"No Delta table found at {silver_path}")
        
        logging.info(f"Silver data written to {silver_path}")
        return spark.read.format("delta").load(silver_path)
    
    except Exception as e:
        logging.error(f"Failed to process Silver data: {str(e)}")
        raise