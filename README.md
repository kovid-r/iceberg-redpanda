## Streaming data into Apache Iceberg with Redpanda

### Prerequisites

You'll need to go through the following three steps to integrate Redpanda with Apache Iceberg.

1. Follow the official documentation to install [Docker](https://docs.docker.com/engine/install/) & [Docker Compose](https://docs.docker.com/compose/install/) on your machine.
2. Refer to the [official quickstart guide for deploying Redpanda on Docker](https://docs.redpanda.com/docs/get-started/quick-start/). For simplicity's sake, use a single-node cluster.
3. Use Tabular.io's [Docker-Spark-Iceberg](https://github.com/tabular-io/docker-spark-iceberg) image that sets up the following: a local object store powered by MinIO, JupyterHub, Apache Spark, and Apache Iceberg.

You'll be using the [`rpk` CLI tool](https://docs.redpanda.com/docs/reference/rpk/) to interact with Redpanda and [Spark Shell](https://spark.apache.org/docs/latest/quick-start.html) to interact with Apache Iceberg. The installation of both these tools is taken care of in the steps above.

Once you've gone through the installation, you can run the following command to check if all the relevant applications are running on [Docker](https://www.docker.com/):

```shell
$ docker ps -a - format "table {{.ID}}\t{{.Names}}\t{{.Ports}}"
```

The output of that command should look something like the following:

```shell
CONTAINER ID NAMES PORTS
775754a9ce2c spark-iceberg 0.0.0.0:8080->8080/tcp, 0.0.0.0:8888->8888/tcp, 0.0.0.0:10000–10001->10000–10001/tcp
d09d1e48c55a mc
80fe1d425317 minio 0.0.0.0:9000–9001->9000–9001/tcp
fe21ddcd82ed iceberg-rest 0.0.0.0:8181->8181/tcp
a8415c472e6f redpanda-1 0.0.0.0:8081–8082->8081–8082/tcp, 0.0.0.0:9092->9092/tcp, 0.0.0.0:9644->9644/tcp
```

If everything is good, let's start by setting up streaming data into Iceberg.

### Streaming data to Apache Iceberg with Redpanda

This section will take you through the steps you need to stream data into an [Apache Iceberg table](https://iceberg.apache.org/spec/). To mimic a real-world application while keeping it simple, you will need to write a few messages on a Redpanda topic that you want to stream into the table.

You will then consume these messages from the Redpanda topic. You will then create an Iceberg table that matches the structure of the data you want to ingest. Once you do that, you'll set up a sink between Spark and Apache Iceberg to stream the data into the table.

The flow will look something like the one shown in the image below:

![Simplified workflow of producing messages in Redpanda and consuming them using Spark to sink into Apache Iceberg - Image by Author](https://i.imgur.com/lYHECiX.jpg)

Follow the below-mentioned steps to replicate the flow described in the image above.

#### Step 1. Check Redpanda status

You can use the following command to see check whether Redpanda is running in a Docker container:

```shell
$ docker exec -it redpanda-1 rpk cluster info 
```

If it is running, you'll see an output specifying your cluster identifier and broker, as shown below:

```shell
CLUSTER
=======
redpanda.a3a6fa77-f135–456a-8479–8ff3806ec665
BROKERS
=======
ID HOST PORT
0* 0.0.0.0 9092
```

#### Step 2. Create a Redpanda topic

The first order of business is to create a topic using the `rpk` CLI. You can use the following command to create a topic called `rp_topic`:

```shell
$ docker exec -it redpanda-1 rpk topic create rp_ropic - brokers=localhost:9092
```

#### Step 3. Check Redpanda topic status

Although the command returns an `OK` message if the creation of the topic is successful, if you still want to check the status of the topic later, you can use the following command:

```shell
$ docker exec -it redpanda-1 rpk topic list
```

The output will tell you the topic, the number of partitions, and the number of replicas it has, as shown below:

```shell
NAME      PARTITIONS REPLICAS
rp_topic  1          1
```

#### Step 4. Produce messages on a Redpanda topic using the CLI

For demonstration purposes, write a few messages after executing the following command:

```shell
$ docker exec -it redpanda-1 rpk topic produce rp_topic - brokers=localhost:9092
```

Once you enter a message, press the return key, and you'll be ready to send another message to the topic. Here are a few sample messages:

```
Here's the first message.
Produced to partition 0 at offset 0 with timestamp 1681221187060.
The second message
Produced to partition 0 at offset 1 with timestamp 1681221190160.
Third message.
Produced to partition 0 at offset 2 with timestamp 1681221193370.
Well, here's the fourth one.
Produced to partition 0 at offset 3 with timestamp 1681221199052.
And at last - the fifth message.
Produced to partition 0 at offset 4 with timestamp 1681221204702.
```

Now, let's see how to consume these messages.

#### Step 5. Consume messages from a Redpanda topic using the CLI

To verify that your messages have reached the Redpanda topic, you can execute the following command in a new terminal window and keep the producer terminal open:

```shell
$ docker exec -it redpanda-1 rpk topic consume rp_topic - brokers=localhost:9092
```

Running this command will print out all the messages from the Redpanda topic. Here's the output based on the messages we had produced in the previous step:

```json
{
  "topic": "random_topic",
  "value": "Here's the first message.",
  "timestamp": 1681221187060,
  "partition": 0,
  "offset": 0
}
{
  "topic": "random_topic",
  "value": "The second message",
  "timestamp": 1681221190160,
  "partition": 0,
  "offset": 1
}
{
  "topic": "random_topic",
  "value": "Third message.",
  "timestamp": 1681221193370,
  "partition": 0,
  "offset": 2
}
{
  "topic": "random_topic",
  "value": "Well, here's the fourth one.",
  "timestamp": 1681221199052,
  "partition": 0,
  "offset": 3
}
{
  "topic": "random_topic",
  "value": "And at last - the fifth message.",
  "timestamp": 1681221204702,
  "partition": 0,
  "offset": 4
}
```

Now that you have messages ready to be consumed, let's configure the Iceberg table where you want to sink these messages.

#### Step 6. Configure Apache Iceberg

As mentioned at the beginning of the article, Apache Iceberg's documentation recommends using [this repository](https://github.com/tabular-io/docker-spark-iceberg.git) to get started on Docker, so use the following command to clone the repository on your local machine:

```shell
$ git clone https://github.com/tabular-io/docker-spark-iceberg.git
```

Using Docker Compose, deploy Apache Iceberg on a [MinIO storage backend](https://min.io/) using the following command:

```shell
$ cd docker-spark-iceberg
$ docker-compose up
```

Now, you're ready to interact with Apache Iceberg using the Spark Shell.

#### Step 7. Create an Apache Iceberg table for the data to be ingested

To test the integration, you need to create an Iceberg table that stores the Redpanda topic messages after ingestion. First, start a SparkSQL shell using the following command:

```shell
$ docker exec -it spark-iceberg spark-sql
```

To create an Iceberg table, you can use the following command:

```sql
CREATE TABLE test.redpanda.rp_topic (
 topic_message string )
 USING iceberg;
```

If you don't want to use Spark Shell, you can also run this SparkSQL command using the [Jupyter](https://jupyter.org) console.

#### Step 8. Sink data from Redpanda topic to the Apache Iceberg table

First, initialize your Spark session with the Spark SQL Kafka package, as prescribed by the [Kafka + Spark integration guide](https://spark.apache.org/docs/2.2.1/structured-streaming-kafka-integration.html):

```python
import os
import findspark
import pyiceberg
from pyspark.sql import SparkSession
from pyspark.sql import functions

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0'
findspark.init()

spark = SparkSession \
  .builder \
  .appName("Redpanda Iceberg Sink") \
  .getOrCreate()
```

Then start reading from the Redpanda topic into a data frame using the following snippet:

```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rp_topic") \
    .load()
```

For simplicity, we'll only get the field from the stream that corresponds to the `topic_message` text. To do that, filter your data fame to only get the `value` column. And finally, start writing the streaming data to the intended Iceberg table with the `Trigger.ProcessingTime` variable set to 5 seconds, which means that Spark will write data to Iceberg every five seconds. The checkpoint information will be stored in the `checkpoint` directory:

```python
df.selectExpr("CAST(value AS STRING)") \
    .writeStream.format("iceberg") \
    .outputMode("append") \
    .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS)) \
    .option("path", "redpanda.rp_topic") \
    .option("checkpointLocation", "checkpoint") \
    .start()
```

Once you start streaming data into the Iceberg table, you can verify your results by running a `SELECT *` command on the `rp_topic` table from the Spark SQL shell.

```shell
spark-sql> SELECT * FROM rp_topic;
Here's the first message.
Well, here's the fourth one.
The second message
And at last - the fifth message.
Third message.
Time taken: 0.333 seconds, Fetched 5 row(s)
```

You've now successfully set up a sink between a Redpanda topic and an Apache Iceberg table.
