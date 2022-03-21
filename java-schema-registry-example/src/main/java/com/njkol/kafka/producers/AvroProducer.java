package com.njkol.kafka.producers;

import java.io.File;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.njkol.kafka.models.User;
import com.njkol.kafka.serde.KafkaAvroSerializer;

public class AvroProducer {

	public static void main(String[] args) throws Exception {

		String bootstrap_server ="localhost:9192";
		String topicName = "AvroProducerTopic";
		
		String key = "5";
		
		User value = new User();
		value.setId(Integer.parseInt(key));
		value.setUsername("arpit_cool");
		value.setEmailAddress("arpit.cool100@gmail.com");
		value.setPhoneNumber("9031871234");
		value.setFirstName("Arpit");
		value.setMiddleName("");
		value.setLastName("Aggarwaal");
		value.setSex("M");
		
		Schema schema = new Schema.Parser().parse(new File("./src/main/resources/user_v1.avsc"));
		GenericRecord emp1 = new GenericData.Record(schema);
		emp1.put("id", 1);
		emp1.put("username", "deep_dey");
		emp1.put("email_address", "deep.de@gmail.com");
		emp1.put("phone_number", "7892198879");
		emp1.put("first_name", "Deep");
		emp1.put("middle_name", "Chandra");
		emp1.put("last_name", "Dey");
		emp1.put("sex", "M");
		
		GenericRecord emp2 = new GenericData.Record(schema);
		emp2.put("id", 2);
		emp2.put("username", "Nj-Kol");
		emp2.put("email_address", "nilanjan.sarkar100@gmail.com");
		emp2.put("phone_number", "9031871234");
		emp2.put("first_name", "Nilanjan");
		emp2.put("middle_name", "Kamalesh");
		emp2.put("last_name", "Sarkar");
		emp2.put("sex", "M");
		
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

		Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
		ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topicName, key, emp2);
		producer.send(record);
		producer.close();

		System.out.println("AvroProducer Completed!");
		
	}
	
}
