kafka-offset-monitor
===========

**Warning: this application requires Kafka brokers version to be at least 0.10.2.0 in case of kafka internal offset storage.**

This is an app to monitor your Kafka consumers and their position (offset) in the log.

You can see the current consumer groups, for each group the topics that they are consuming and the position of the group in each topic log. This is useful to understand how quick you are consuming from a log and how fast the log is growing. It allows for debugging Kafka producers and consumers or just to have an idea of what is going on in your system.

The app keeps a history of log position and lag of the consumers so you can have an overview of what has happened in the last days. The amount of history to keep is definable at runtime.

Offset Types
===========

Kafka is flexible on how the offsets are managed. Consumer can choose arbitrary storage and format to persist offsets. kafka-offset-monitor currently supports following popular storage formats

* kafka built-in offset management API (based on broker metadata and Kafka's own internal __consumer_offsets topic)

Each runtime instance of KafkaOffsetMonitor can only support a single type of storage format.

Building It
===========

The command below will build a fat-jar in the target/scala_${SCALA_VERSION} directory which can be run by following the command in the "Running It" section.

```bash
$ sbt clean assembly
```

Running It
===========

This is a small web app, you can run it locally or on a server, as long as you have access to the Kafka broker(s) and ZooKeeper nodes storing kafka data.

```

java -cp KafkaOffsetMonitor-assembly*.jar \
       com.quantifind.kafka.offsetapp.OffsetGetterWeb \
     --offsetStorage kafka \
     --kafkaBrokers kafka-broker-1:9092,kafka-broker-2:9092 \
     --kafkaSecurityProtocol PLAINTEXT \
     --zk zk-server-1,zk-server-2 \
     --port 8081 \
     --refresh 10.seconds \
     --retain 2.days \
     --dbName offsetapp_kafka

```

The arguments are:

- **offsetStorage** valid options is ''kafka''.
- **zk** the ZooKeeper hosts
- **kafkaBrokers** comma-separated list of Kafka broker hosts (ex. "host1:port,host2:port').  Required only when using offsetStorage "kafka".
- **kafkaSecurityProtocol** security protocol to use when connecting to kafka brokers (default: ''PLAINTEXT'', optional: ''SASL_PLAINTEXT'')
- **port** the port on which the app will be made available
- **refresh** how often should the app refresh and store a point in the DB
- **retain** how long should points be kept in the DB
- **dbName** where to store the history (default 'offsetapp')

Contributing (from the original project that is not maintenated since Aug 28, 2015)
============

The KafkaOffsetMonitor is released under the Apache License and we **welcome any contributions** within this license. Any pull request is welcome and will be reviewed and merged as quickly as possible.

Because this open source tool is released by [Quantifind](http://www.quantifind.com) as a company, if you want to submit a pull request, you will have to sign the following simple contributors agreement:
- If you are an individual, please sign [this contributors agreement](https://docs.google.com/a/quantifind.com/document/d/1RS7qEjq3cCmJ1665UhoCMK8541Ms7KyU3kVFoO4CR_I/) and send it back to contributors@quantifind.com
- If you are contributing changes that you did as part of your work, please sign [this contributors agreement](https://docs.google.com/a/quantifind.com/document/d/1kNwLT4qG3G0Ct2mEuNdBGmKDYuApN1CpQtZF8TSVTjE/) and send it back to contributors@quantifind.com
