# README

В данной лабораторной работе в качестве набора данных был выбран dataset **2015 Flight Delays and Cancellations** (обрезанный до 100.000 строк). ML-идея: предсказание времени задержки рейса.

# Запуск

Запустить проект можно с помощью одной команды:

```bash
docker-compose up --build
```

Запуск `main.py` выполняется автоматически из-за соответствующей команды в `Dockerfile`.

# Результат

Результат запуска можно увидеть в файле `logs/app.log` . Например:

```bash
2025-05-14 12:22:33,655 - INFO - Starting FlightLakehouse pipeline
2025-05-14 12:22:36,653 - INFO - Loading data into Bronze layer
2025-05-14 12:22:58,051 - INFO - Bronze data written to file:/app/data/bronze/flights
2025-05-14 12:22:58,173 - INFO - Bronze layer loaded
2025-05-14 12:22:58,174 - INFO - Processing data for Silver layer
2025-05-14 12:23:13,819 - INFO - Silver Delta table optimized at file:/app/data/silver/flights
2025-05-14 12:23:13,820 - INFO - Silver data written to file:/app/data/silver/flights
2025-05-14 12:23:13,985 - INFO - Silver layer loaded
2025-05-14 12:23:13,986 - INFO - Processing data for Gold layer
2025-05-14 12:23:37,608 - INFO - Gold Delta table optimized at file:/app/data/gold/flights
2025-05-14 12:23:37,609 - INFO - Gold data written to file:/app/data/gold/flights
2025-05-14 12:23:37,610 - INFO - Gold layer loaded
2025-05-14 12:23:37,610 - INFO - Training ML model
2025-05-14 12:23:45,498 - INFO - Model trained and logged. MSE: 68.82606866335851, R2: 0.9532979351023791
2025-05-14 12:23:45,499 - INFO - Model trained and logged
2025-05-14 12:23:46,017 - INFO - Spark session stopped
2025-05-14 12:23:46,019 - INFO - Closing down clientserver connection
```

Также информацию по запускам можно увидеть по адресу `http://localhost:5000/`.  Например:

![image.png](image.png)