package com.gooyuanly.kafka.metrics.collector.kafka;

import com.google.common.collect.Maps;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxConnection;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMonitorItem;
import com.ke.streaming.common.tuple.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: hy
 * date: 2019/1/30
 * desc:
 */
public class Broker implements Closeable {
    private int id;
    private String host;
    private int port;
    private JmxConnection jmxConnection;

    public Broker(int id, String host, int port, int jmxPort) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        if (jmxPort == -1) {
            throw new Exception("Jmx port is -1, kafka broker: " + host);
        }
        this.jmxConnection = new JmxConnection(host, jmxPort);
        this.jmxConnection.open();
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        jmxConnection.close();
    }
}
