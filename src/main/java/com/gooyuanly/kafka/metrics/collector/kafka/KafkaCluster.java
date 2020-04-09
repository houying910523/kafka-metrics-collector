package com.gooyuanly.kafka.metrics.collector.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gooyuanly.kafka.metrics.collector.config.KafkaConfig;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMonitorItem;
import com.gooyuanly.kafka.metrics.collector.jmx.MetricsManager;
import com.gooyuanly.kafka.metrics.collector.reporter.Reporter;
import com.ke.streaming.common.utils.IOUtils;
import com.ke.streaming.common.utils.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class KafkaCluster implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCluster.class);
    private final KafkaConfig kafkaConfig;
    private Map<String, Broker> brokerMap;

    private String bootstrap;

    private KafkaConsumer<String, String> consumer;

    private ThreadPoolExecutor poolExecutor;
    private Reporter reporter;

    public KafkaCluster(KafkaConfig kc, ThreadPoolExecutor poolExecutor) throws Exception {
        this.kafkaConfig = kc;
        this.poolExecutor = poolExecutor;
    }

    public void start() throws Exception {
        initBrokerInfo();
        this.bootstrap = brokerMap.values().stream().map(b -> b.getHost() + ":" + b.getPort())
                .reduce((b1, b2) -> b1 + "," + b2).get();
        consumer = new KafkaConsumer<>(kafkaParams());
    }

    private Map<String, Object> kafkaParams() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-metrics-collector");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return map;
    }

    public void fetch(long ts) {
        long t1 = System.currentTimeMillis();
        consumer.listTopics().forEach((topic, partitionInfos) -> {
            poolExecutor.submit(() -> {
                fetchJmxItem(topic, partitionInfos, ts).forEach(item -> {
                    reporter.report(kafkaConfig.getName(), item);
                });
            });
        });
        long t2 = System.currentTimeMillis();
        logger.info("fetch {} cost {} ms", kafkaConfig.getName(), t2 - t1);
    }

    private Stream<JmxMetricItem> fetchJmxItem(String topic, List<PartitionInfo> partitionInfos, long timestamp) {
        Set<String> leaders = partitionInfos.stream().map(pi -> String.valueOf(pi.leader().id())).collect(Collectors.toSet());
        List<JmxMonitorItem> items = MetricsManager.apply(topic);
        return leaders.stream().map(id -> brokerMap.get(id)).flatMap(broker ->
                items.stream().flatMap(item -> broker.poll(item, timestamp).stream())
        );
    }

    @Override
    public void close() {
        brokerMap.values().forEach(IOUtils::closeQuietly);
        consumer.close();
    }

    private void initBrokerInfo() throws Exception {
        logger.info("init brokers");
        ZkUtils zkUtils = new ZkUtils(kafkaConfig.getZk(), kafkaConfig.getPath());
        brokerMap = Maps.newHashMap();
        for (String id : zkUtils.listBrokerIds()) {
            Broker broker = brokerInfo(id, zkUtils);
            brokerMap.put(id, broker);
            logger.info("init broker[{}]", broker.getHost());
        }
        zkUtils.close();
    }

    private Broker brokerInfo(String id, ZkUtils zkUtils) throws Exception {
        String json = zkUtils.readBrokerInfo(id);
        JsonNode jsonNode = JsonUtil.readTree(json);
        String host = jsonNode.get("host").asText();
        int port = jsonNode.get("port").asInt();
        return new Broker(Integer.valueOf(id), host, port, jsonNode.get("jmx_port").asInt());
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }
}
