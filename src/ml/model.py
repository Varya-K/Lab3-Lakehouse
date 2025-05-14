from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import mlflow
import mlflow.sklearn
import pandas as pd
import logging

def train_model(spark: SparkSession, gold_df):
    logging.info("Training ML model")
    
    pdf = gold_df.toPandas()
    
    X = pdf[["avg_arrival_delay", "flight_count"]]
    y = pdf["avg_departure_delay"]
    
    model = LinearRegression()
    model.fit(X, y)

    y_pred = model.predict(X)
    mse = mean_squared_error(y, y_pred)
    r2 = r2_score(y, y_pred)
    
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("FlightDelayPrediction")
    with mlflow.start_run():
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)
        mlflow.sklearn.log_model(model, "model")
    
    logging.info(f"Model trained and logged. MSE: {mse}, R2: {r2}")
