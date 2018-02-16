
# Create topic on Kafka
```
> bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic MYTOPIC --partitions 1 --config flush.messages=1 --replication-factor 1
```

# Run Producer
This sample producer generate message contening random UUID
```
java -cp kafka-producer-1.0-SNAPSHOT.jar com.tony.kafka.KafkaProducerProducer localhost:2181 MYTOPIC
```

