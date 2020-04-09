package com.gooyuanly.kafka.metrics.collector.jmx;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hy
 * @date 2020/4/9
 * @desc
 */
public class MetricsManager {

    public static final List<JmxMonitorItem> metrics = Lists.newArrayList(
            new JmxMonitorItem("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=${topic}", "OneMinuteRate"),
            new JmxMonitorItem("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=${topic}", "OneMinuteRate")
    );

    public static List<JmxMonitorItem> apply(String topic) {
        return metrics.stream()
                .map(item -> item.applyTopic(topic))
                .collect(Collectors.toList());
    }

}
