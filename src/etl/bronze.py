from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import logging
import os

FLIGHTS_SCHEMA = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("AIRLINE", StringType(), True),
    StructField("FLIGHT_NUMBER", IntegerType(), True),
    StructField("TAIL_NUMBER", StringType(), True),
    StructField("ORIGIN_AIRPORT", StringType(), True),
    StructField("DESTINATION_AIRPORT", StringType(), True),
    StructField("SCHEDULED_DEPARTURE", IntegerType(), True),
    StructField("DEPARTURE_TIME", IntegerType(), True),
    StructField("DEPARTURE_DELAY", IntegerType(), True),
    StructField("TAXI_OUT", IntegerType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("SCHEDULED_TIME", IntegerType(), True),
    StructField("ELAPSED_TIME", IntegerType(), True),
    StructField("AIR_TIME", IntegerType(), True),
    StructField("DISTANCE", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", IntegerType(), True),
    StructField("SCHEDULED_ARRIVAL", IntegerType(), True),
    StructField("ARRIVAL_TIME", IntegerType(), True),
    StructField("ARRIVAL_DELAY", IntegerType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_REASON", StringType(), True),
    StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
    StructField("SECURITY_DELAY", IntegerType(), True),
    StructField("AIRLINE_DELAY", IntegerType(), True),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
    StructField("WEATHER_DELAY", IntegerType(), True)
])

def load_bronze(spark: SparkSession):
    logging.info("Loading data into Bronze layer")
    
    # Проверка на существование файла
    input_path = "file:/app/data/flights.csv"
    if not os.path.exists(input_path.replace("file:/", "/")):
        logging.error(f"Input file {input_path} not found")
        raise FileNotFoundError(f"{input_path} not found")
    
    bronze_path = "file:/app/data/bronze/flights"
    os.makedirs(os.path.dirname(bronze_path.replace("file:/", "/")), exist_ok=True)
    
    try:
        raw_df = spark.read.schema(FLIGHTS_SCHEMA).csv(input_path, header=True)
        

        # Проверка на присутсвие необходимых признаком
        required_columns = ["AIRLINE", "ORIGIN_AIRPORT", "DEPARTURE_DELAY", "ARRIVAL_DELAY"]
        missing_columns = [col for col in required_columns if col not in raw_df.columns]
        if missing_columns:
            logging.error(f"Missing required columns: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        

        raw_df = raw_df.repartition(8)
        
        raw_df.write.format("delta").mode("overwrite").save(bronze_path)
        
        logging.info(f"Bronze data written to {bronze_path}")
        return spark.read.format("delta").load(bronze_path)
    
    except Exception as e:
        logging.error(f"Failed to load Bronze data: {str(e)}")
        raise