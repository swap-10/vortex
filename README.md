First run the ignite server

Then run the kafka zookeeper and the kafka server

Then run the kafka producer to generate the event stream

Then run the main spark app using:

vortex-venv/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark-app/spark_main.py


