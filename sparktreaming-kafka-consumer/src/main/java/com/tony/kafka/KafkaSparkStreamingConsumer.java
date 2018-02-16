package com.tony.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tony.kafka.utils.ZookeeperKafkaMetadataStore;
import com.tony.kafka.utils.ZookeeperOffsetStore;


/**
 * Consumes messages from topics in Kafka
 * 
 */
public final class KafkaSparkStreamingConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaSparkStreamingConsumer.class);
    
    private static MessageProcessor processor = new MessageProcessor();
    
    private KafkaSparkStreamingConsumer() {}
    
	public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            log.error(
                    "Wrong number of arguments, aborting... The arguments should be zookepperhostlist, kafka topic");
        } else {

            String zkConnect = args[0];
            String topic = args[1]; // MYTOPIC

            ZookeeperKafkaMetadataStore metastore = new ZookeeperKafkaMetadataStore(zkConnect);
            ZookeeperOffsetStore zkOffsetStore = new ZookeeperOffsetStore(zkConnect);
            Set<String> topics = new HashSet(Arrays.asList(topic));

            log.debug("Configuring Spark...");
            SparkConf sparkConf = new SparkConf().setAppName("SarparserXKafkaSparkStreaming");
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000");

            // Create the context with 10 seconds batch size
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

            final Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", metastore.getBrokersList());
            kafkaParams.put("auto.offset.reset", "largest");
            kafkaParams.put("security.protocol", "SASL_PLAINTEXT");

            log.debug("Creating Kafka stream...");
            JavaPairInputDStream<byte[], byte[]> messages = KafkaUtils.createDirectStream(jssc, byte[].class,
                    byte[].class, kafka.serializer.DefaultDecoder.class, kafka.serializer.DefaultDecoder.class,
                    kafkaParams, topics);

            // Here spark commands
            processor.process(messages);

            jssc.start();
            jssc.awaitTermination();

        }
    }
}
