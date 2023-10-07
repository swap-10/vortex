# Vortex

## Vortex is a real-time, distributed, fault-tolerant, highly-scalable, rapid-fast data stream processing software.

It can consume data from apache Kafka topics on-the-fly, process it using apache spark, including basic processing as well as statistical ML workloads, and stream it to
an apache Ignite cluster to store as an in-memory data-grid, which can be persisted to disk as required.

Following are the steps to setup a minimal example with sample e-commerce data:

* First run the ignite server

* Then run the kafka zookeeper and the kafka server

* Then run the kafka producer to generate the event stream

* Then run the main spark app using:

```bash
vortex-venv/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark-app/spark_main.py
```


