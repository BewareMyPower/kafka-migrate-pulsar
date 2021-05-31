# kafka-migrate-pulsar

A simple tool to migrate a topic's messages and committed offsets from a [Kafka](https://github.com/apache/kafka) cluster to a [KoP](https://github.com/streamnative/kop) cluster.

## How to use?

Assuming there's a Kafka cluster that listens at `localhost:9092` and a KoP cluster that listens at `localhost:19092`, run following commands to migrate topic `my-topic` from Kafka cluster to KoP cluster.

```bash
$ mvn compile
$ mvn exec:java -Dexec.mainClass=io.github.bewaremypower.Main -Dexec.args="--topic my-topic --kafka-broker-list localhost:9092 --kop-broker-list localhost:19092"
```
