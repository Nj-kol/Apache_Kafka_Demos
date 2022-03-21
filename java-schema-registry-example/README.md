

**Console Consumer**

/opt/Kafka/kafka_2.10-0.9.0.1/bin/kafka-console-producer.sh \
--broker-list localhost:9192 \
--topic AvroProducerTopic \
--from-beginning

/opt/Kafka/kafka_2.10-0.9.0.1/bin/kafka-console-consumer.sh \
--zookeeper localhost:3181 \
--topic AvroProducerTopic \
--from-beginning 