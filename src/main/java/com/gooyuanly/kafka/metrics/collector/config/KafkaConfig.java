package com.gooyuanly.kafka.metrics.collector.config;

/**
 * @author hy
 * @date 2020/4/7
 * @desc
 */
public class KafkaConfig {
    private String name;
    private String zk;
    private String path;

    public KafkaConfig(String name, String zk, String path) {
        this.name = name;
        this.zk = zk;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public String getZk() {
        return zk;
    }

    public String getPath() {
        return path;
    }
}
