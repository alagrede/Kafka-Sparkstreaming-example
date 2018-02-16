package com.tony.kafka.utils;


import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import kafka.api.OffsetRequest;
import kafka.api.OffsetResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.PartitionOffsetsResponse;
import kafka.api.Request;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class ZookeeperOffsetStore implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(ZookeeperOffsetStore.class);

	private static int ZK_SESSION_TIMEOUT = 10000;
	private static int ZK_CONNECTION_TIMEOUT = 10000;
	private String zkHosts;

	public ZookeeperOffsetStore(String zkHosts) {
		this.zkHosts = zkHosts;
	}

	/**
	 * Read the last offsets of the different partitions of a list of topics
	 * 
	 * @param topics
	 * @param defaultOffset
	 * @return
	 */
	public Map<TopicAndPartition, Long> readLastOffsets(Set<String> topics, String defaultOffset) {
		Map<TopicAndPartition, Long> res = new HashMap<TopicAndPartition, Long>();
		for (String topic : topics) {
			try {
				Map<TopicAndPartition, Long> offest = getDefaultOffsets(topic, defaultOffset);
				res.putAll(offest);
			} catch (Exception e) {
				log.warn("Unable to get stream for topic " + topic);
			}
		}
		return res;
	}

	
	/**
	 * get the Kafka simple consumer so that we can fetch broker offsets
	 * @param brokerId
	 * @return
	 */
	private SimpleConsumer getConsumer(Integer brokerId) {
		ZkClient zkClient = new ZkClient(zkHosts, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false);
		try {
			String brokerInfoString = zkUtils.readData(ZkUtils.BrokerIdsPath() + "/" + brokerId)._1;

			JsonParser parser = new JsonParser();
			JsonObject o = parser.parse(brokerInfoString).getAsJsonObject();
			String hostport = o.getAsJsonArray("endpoints").get(0).getAsString();

			// The value returned looks like
			// {"jmx_port":-1,"timestamp":"1493619190359","endpoints":["PLAINTEXTSASL://fr0-datalab-p50.bdata.corp:9092"],"host":null,"version":3,"port":-1}
			int indexport = hostport.lastIndexOf(":");
			int indexhost = hostport.lastIndexOf("/");
			String host = hostport.substring(indexhost + 1, indexport);
			int port = Integer.valueOf(hostport.substring(indexport + 1));
			log.info("Creating new simple consumer for host : " + host + " and port :" + port);
			zkUtils.close();
			zkClient.close();
			return new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker", SecurityProtocol.PLAINTEXTSASL);

		} catch (Exception t) {
			log.error("Could not parse broker info", t);
			return null;
		} finally {
			zkUtils.close();
			zkClient.close();
		}

	}

	/**
	 * Merge a Map<Integer,Long> (map of partitionid,offset) and a Map<TopicAndPartition,Long> (map of topicandpartition,offset) into a Map<Integer,Long>
	 * (remove the topic reference from the second map as it is not needed)
	 * 
	 * @param map1
	 * @param map2
	 * @return map merged
	 */
	private Map<Integer, Long> mergeMaps(Map<Integer, Long> map1, Map<TopicAndPartition, Long> map2) {
		// converting Map<TopicAndPartition,Long> into Map<Integer,Long>
		Map<Integer, Long> storedOffsetsPartitions = map2.entrySet().stream().collect(Collectors.toMap(p -> p.getKey().partition(), p -> p.getValue()));

		Map<Integer, Long> mx = Stream.of(map1, storedOffsetsPartitions).map(Map::entrySet) // converts each map into an entry set
				.flatMap(Collection::stream)// converts each set into an entry stream
				.collect(Collectors.toMap( // collects into a map
						Map.Entry::getKey, // where each entry is based
						Map.Entry::getValue, // on the entries in the stream
						Long::max // such that if a value already exist for
									// a given key, the max of the old
									// and new value is taken
		));
		return mx;
	}

	/**
	 * @param topic
	 * @return
	 */
	public Map<Integer, SimpleConsumer> getConsumers(String topic) {
		ZkClient zkClient = new ZkClient(zkHosts, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false);
		List<Object> partitions = JavaConversions
				.seqAsJavaList(zkUtils.getPartitionsForTopics(scala.collection.JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
		Map<Integer, SimpleConsumer> result = new HashMap<Integer, SimpleConsumer>();
		for (Object pido : partitions) {
			Integer pid = (Integer) pido;

			int leaderid = (Integer) zkUtils.getLeaderForPartition(topic, pid).get();
			log.debug("Found leader id {} for topic {} and partition {}", leaderid, topic, pid);
			result.put(pid, getConsumer(leaderid));
		}
		zkUtils.close();
		zkClient.close();
		return result;
	}

	/**
	 * Computes the default offset for a topic
	 * 
	 * @param topic
	 * @param defaultOffset
	 * @return
	 */
	public Map<TopicAndPartition, Long> getDefaultOffsets(String topic, String defaultOffset) {
		ZkClient zkClient = new ZkClient(zkHosts, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false);

		List<Object> partitions = JavaConversions
				.seqAsJavaList(zkUtils.getPartitionsForTopics(scala.collection.JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
		Map<TopicAndPartition, Long> result = new HashMap<TopicAndPartition, Long>();
		for (Object pido : partitions) {
			Integer pid = (Integer) pido;

			int leaderid = (Integer) zkUtils.getLeaderForPartition(topic, pid).get();
			log.debug("Found leader id {} for topic {} and partition {}", leaderid, topic, pid);

			SimpleConsumer consumer = getConsumer(leaderid);
			TopicAndPartition topicpart = new TopicAndPartition(topic, pid);
			try {
				Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
				if (defaultOffset.equalsIgnoreCase("latest")) {
					requestInfo.put(topicpart, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
				} else {
					requestInfo.put(topicpart, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
				}
				scala.collection.immutable.Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoScala = JavaConverters
						.mapAsScalaMapConverter(requestInfo).asScala().toMap(Predef.<Tuple2<TopicAndPartition, PartitionOffsetRequestInfo>>conforms());
				OffsetRequest request = new OffsetRequest(requestInfoScala, kafka.api.OffsetRequest.CurrentVersion(), 0, "offsetChecker",
						Request.OrdinaryConsumerId());
				OffsetResponse response = consumer.getOffsetsBefore(request);
				Map<TopicAndPartition, PartitionOffsetsResponse> intMap = JavaConversions.asJavaMap(response.partitionErrorAndOffsets());
				Long readOffset = (Long) JavaConversions.asJavaList(intMap.get(topicpart).offsets()).get(0);
				log.debug("Found offset {} for topic {} and partition {}", readOffset, topic, pid);
				result.put(topicpart, readOffset);
			} catch (Exception e) {
				log.warn("Error fetching data Offset Data the Broker.");
				result.put(topicpart, 0L);
			}

		}
		zkUtils.close();
		zkClient.close();
		return result;
	}

}
