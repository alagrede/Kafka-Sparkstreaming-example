package com.tony.kafka.utils;


import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.protocol.SecurityProtocol;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

/**
 * Helper class to access Kafka information related to the jobs
 */
public class ZookeeperKafkaMetadataStore implements Serializable {

	private static final long serialVersionUID = 1L;

	private String zkHosts;

	public ZookeeperKafkaMetadataStore(String zkHosts) {
		this.zkHosts = zkHosts;
	}

	public String getBrokersList() {
		ZkClient zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer$.MODULE$);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts), false);
		List<Broker> listBrokers = JavaConversions.asJavaList(zkUtils.getAllBrokersInCluster());
		String result = listBrokers.stream()
				.map(b -> b.getBrokerEndPoint(SecurityProtocol.PLAINTEXTSASL).host() + ":" + b.getBrokerEndPoint(SecurityProtocol.PLAINTEXTSASL).port())
				.collect(Collectors.joining(","));
		zkUtils.close();
		zkClient.close();
		return result;
	}

}
