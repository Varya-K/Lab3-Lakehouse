FROM apache/spark-py:latest

USER root

WORKDIR /app

RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    mlflow==2.9.2 \
    scikit-learn==1.5.0 \
    pandas==2.2.2 \
    numpy==1.26.4

COPY src/ /app/src/

ENV SPARK_HOME=/opt/spark 
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH

RUN mkdir -p /app/data/bronze /app/data/silver /app/data/gold /app/logs

CMD ["spark-submit", "--packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0", "--master", "local[*]", "src/main.py"]