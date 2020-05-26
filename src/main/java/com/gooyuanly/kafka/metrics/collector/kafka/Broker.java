package com.gooyuanly.kafka.metrics.collector.kafka;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxConnection;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMonitorItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author: hy
 * date: 2019/1/30
 * desc:
 */
public class Broker implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    private int id;
    private String host;
    private int port;
    private JmxConnection jmxConnection;

    private Cache<String, Integer> blacklist;


    public Broker(int id, String host, int port, int jmxPort) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        if (jmxPort == -1) {
            throw new Exception("Jmx port is -1, kafka broker: " + host);
        }
        this.jmxConnection = new JmxConnection(host, jmxPort);
        this.jmxConnection.open();
        this.blacklist = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.MINUTES).build();
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public List<JmxMetricItem> poll(JmxMonitorItem item, long timestamp) {
        if (blacklist.getIfPresent(item.getBeanName()) == null) {
            return Collections.emptyList();
        }
        try {
            return jmxConnection.getAttribution(item.getBeanName(), item.getAttribution())
                    .stream()
                    .map(tuple -> {
                        String[] array = tuple.f1().split(":", 2)[1].split(",");
                        Map<String, String> tags = Maps.newHashMap();
                        tags.put("broker", host);

                        String name = null;
                        for (String kv: array) {
                            if (kv.contains("=")) {
                                String[] keyAndValue = kv.split("=", 2);
                                if ("name".equals(keyAndValue[0])) {
                                    name = keyAndValue[1];
                                } else {
                                    tags.put(keyAndValue[0], keyAndValue[1]);
                                }
                            }
                        }

                        return new JmxMetricItem(name, tags, timestamp, tuple.f2());
                    }).collect(Collectors.toList());
        } catch(InstanceNotFoundException e) {
            blacklist.put(item.getBeanName(), 1);
            logger.info("{} 添加黑名单：{}", host, item.getBeanName());
            return Collections.emptyList();
        } catch (Exception e) {
            //throw new RuntimeException(e);
            logger.error("getAttribution error", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void close() throws IOException {
        jmxConnection.close();
    }
}
