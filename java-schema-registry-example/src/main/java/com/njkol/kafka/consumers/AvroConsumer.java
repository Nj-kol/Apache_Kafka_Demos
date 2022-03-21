package com.njkol.kafka.consumers;

import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
public class AvroConsumer {

	public static void main(String[] args) throws Exception {

		String topicName = "AvroProducerTopic";
		String groupName = "AvroProducerTopicGroup";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9192");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.njkol.kafka.serde.KafkaAvroDeserializer");
		props.put("enable.auto.commit", "false");

		
		KafkaConsumer<String, Object> consumer = null;

		try {
			System.out.println("Started consuming messages ...");
			consumer = new KafkaConsumer<String, Object>(props);
			consumer.subscribe(Arrays.asList(topicName));

			while (true) {
				ConsumerRecords<String, Object> records = consumer.poll(100);
				for (ConsumerRecord<String, Object> record : records) {
					System.out.println("Key : " + record.key());
					System.out.println("Value : " + record.value());
				}
				consumer.commitAsync();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			consumer.commitSync();
			consumer.close();
		}
	}
}
