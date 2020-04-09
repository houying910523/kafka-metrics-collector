package com.gooyuanly.kafka.metrics.collector.jmx;

import com.google.common.base.Joiner;

import java.util.Map;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class JmxMetricItem {
    private final Map<String, String> tags;
    private final String name;
    private final long timestamp;
    private final long value;

    public JmxMetricItem(String name, Map<String, String> tags, long timestamp, long value) {
        this.name = name;
        this.tags = tags;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append(" ")
                .append(name).append(" ");
        tags.forEach((key, value) -> {
            sb.append(key).append("=").append(value).append(",");
        });
        sb.replace(sb.length() - 1, sb.length(), " ");
        sb.append(value);
        return sb.toString();
    }
}
