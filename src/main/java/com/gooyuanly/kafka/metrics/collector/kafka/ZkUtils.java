package com.gooyuanly.kafka.metrics.collector.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * author: houying
 * date  : 17-2-8
 * desc  :
 */
public class ZkUtils {
    private static final String BROKERS_TOPICS = "/brokers/topics";
    private static final String BROKERS_IDS = "/brokers/ids";

    private final String zkPath;
    private ZkClient zkClient;
    private ObjectMapper objectMapper;

    public ZkUtils(String zkHost, String zkPath) {
        this.zkPath = zkPath;
        this.zkClient = new ZkClient(zkHost, 30000, 30000, new ZkSerializerImpl());
        this.objectMapper = new ObjectMapper();
    }

    public List<String> listBrokerIds() {
        return zkClient.getChildren(zkPath + BROKERS_IDS);
    }

    public void watch(String path, IZkChildListener listener) {
        zkClient.subscribeChildChanges(path, listener);
    }

    public String readBrokerInfo(String id) {
        return zkClient.readData(zkPath + BROKERS_IDS + "/" + id);
    }

    public void close() {
        zkClient.close();
    }

    public List<LeaderPartition> getPartitions(String topic) {
        return zkClient.getChildren(zkPath + BROKERS_TOPICS + "/" + topic + "/partitions").stream().map(part -> {
            try {
                String stateJson = zkClient
                        .readData(zkPath + BROKERS_TOPICS + "/" + topic + "/partitions/" + part + "/state");
                String leader = objectMapper.readTree(stateJson).get("leader").asText();
                return new LeaderPartition(topic, Integer.valueOf(part), leader);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toList());
    }
}
