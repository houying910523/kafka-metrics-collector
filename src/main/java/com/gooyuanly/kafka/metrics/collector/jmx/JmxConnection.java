package com.gooyuanly.kafka.metrics.collector.jmx;

import com.google.common.collect.Lists;
import com.ke.streaming.common.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxConnection {
    private static final Logger logger = LoggerFactory.getLogger(JmxConnection.class);
    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    private JMXConnector connector;
    private final String host;
    private final int port;
    private MBeanServerConnection connection;

    public JmxConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public List<Tuple2<String, Long>> getAttribution(String beanName, String attr) throws Exception {
        ObjectName on = new ObjectName(beanName);
        int i = 0;
        while(true) {
            if (i > 3) {
                throw new IOException("JMX连接失败: " + host);
            }
            try {
                if (beanName.contains("*")) {
                    return queryMultiple(on, attr);
                } else {
                    long value = ((Number) connection.getAttribute(on, attr)).longValue();
                    return Lists.newArrayList(new Tuple2<>(beanName, value));
                }
            } catch (IOException e) {
                logger.info("{} 查询失败，重新连接：{}", host, i);
                reconnect();
                logger.info("after reconnect");
                i++;
            }
        }
    }

    private List<Tuple2<String, Long>> queryMultiple(ObjectName objectName, String attr) throws IOException {
        Set<ObjectInstance> ois = connection.queryMBeans(objectName, null);
        return ois.stream().map(oi -> {
            try {
                Object value = connection.getAttribute(oi.getObjectName(), attr);
                return new Tuple2<>(oi.getObjectName().toString(), ((Number) value).longValue());
            } catch (Exception e) {
                throw new RuntimeException();
            }
        }).collect(Collectors.toList());
    }

    public void open() throws IOException {
        this.connector = JMXConnectorFactory.connect(new JMXServiceURL(String.format(JMX_URL, host, port)));
        this.connection = connector.getMBeanServerConnection();
    }

    public void close() throws IOException {
        connector.close();
    }

    private void reconnect() throws IOException {
        close();
        open();
    }

    public static void main(String[] args) throws Exception {
        JmxConnection connection = new JmxConnection("kafka04-matrix.zeus.lianjia.com", 9901);
        connection.open();
        List<Tuple2<String, Long>> objects = connection.getAttribution("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=search-app-18-log", "OneMinuteRate");
        objects.forEach(System.out::println);
    }
}
