package com.tony.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tony.kafka.domain.EventMessage;
import com.tony.kafka.utils.ZookeeperKafkaMetadataStore;
import com.tony.kafka.utils.ZookeeperOffsetStore;

public class KafkaProducerProducer {

	private static final Logger log = LoggerFactory.getLogger(KafkaProducerProducer.class);

	private String zkBrokers;
	private String kafkaTopic;
	
	public static void main(String[] args) {
	
        if (args.length < 2) {
            log.error("Wrong number of arguments, aborting... The arguments should be zookepperhostlist, kafka topic");
        } else {

	        String zkBrokers = args[0];
	        String kafkaTopic = args[1];
			
			log.debug("Send event");
	
	        ZookeeperKafkaMetadataStore metastore = new ZookeeperKafkaMetadataStore(zkBrokers);
	        ZookeeperOffsetStore zkOffsetStore = new ZookeeperOffsetStore(zkBrokers);
	
			
			final Properties kafkaParams = new Properties();
			kafkaParams.put("bootstrap.servers", metastore.getBrokersList());
			kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			//kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
			kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaParams);
			String json = convertToJson(new EventMessage(UUID.randomUUID().toString()));
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, json);
			producer.send(record);
			producer.close();
		}
	}

	
	private static String convertToJson(EventMessage msg) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(msg);
		} catch (IOException e) {
			log.error(e.getCause() + ":" + e.getMessage());
			return msg.toString();
		}
	}

}
