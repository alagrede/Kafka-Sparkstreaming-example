package com.tony.kafka;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;

public class MessageProcessor {
	
	public void process(JavaPairInputDStream<byte[], byte[]> messages) {
		messages.print();
	}

}
