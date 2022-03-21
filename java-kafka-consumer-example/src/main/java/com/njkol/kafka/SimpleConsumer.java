package com.njkol.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {

	public static void main(String[] args) throws Exception {

		String topicName = "SimpleProducerTopic";
		String groupName = "SimpleProducerTopicGroup";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9192");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false");

		KafkaConsumer<String, String> consumer = null;

		try {
			consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList(topicName));

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
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
