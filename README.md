# Real-Time IoT Data Pipeline Project

This project is a complete implementation of a Data Pipeline that simulates IoT sensor data from a greenhouse, sends it via **Apache Kafka**, processes it in real-time using **Spark Structured Streaming**, and then outputs the results to multiple destinations (Kafka topics and Parquet files) for immediate analysis and archiving.

## 🏛️ Project Architecture

The project consists of two main components operating concurrently:

1.  **Producer (Python Script):**
    * Simulates realistic sensor data (temperature, humidity, pH, etc.) considering environmental factors (time, season).
    * Sends this data (in JSON format) to a Kafka Topic named `farmSensors`.

2.  **Consumer (Spark Structured Streaming):**
    * Reads raw data from the `farmSensors` topic.
    * Applies a complex series of real-time transformations and processing logic.
    * **Outputs the processed data** to multiple destinations:
        * **Kafka Topic (`farmInsights`):** For enriched data, including outlier detection flags.
        * **Kafka Topic (`farmTrends`):** For windowed aggregates (every 5 minutes) to monitor trends.
        * **Kafka Topic (`farmKpis`):** For daily and weekly Key Performance Indicators (KPIs).
        * **Parquet Files:** For permanently archiving processed data and KPIs for future batch analysis.

## 🛠️ Technology Stack

* **Programming Language:** Python 3
* **Message Broker:** Apache Kafka (Version 4.1.0, KRaft mode)
* **Processing Engine:** Apache Spark (Version 4.0.1)
    * **Library:** Spark Structured Streaming
* **Python Libraries:**
    * `kafka-python` (for the Producer)
    * `pyspark` (for the Consumer)

## 📁 File Structure
