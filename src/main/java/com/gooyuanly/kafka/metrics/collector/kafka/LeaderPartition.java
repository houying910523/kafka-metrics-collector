package com.gooyuanly.kafka.metrics.collector.kafka;

/**
 * author: hy
 * date: 2019/1/30
 * desc:
 */
public class LeaderPartition {
    private String topic;
    private int partition;
    private String leader;

    public LeaderPartition(String topic, int partition, String leader) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
    }

    public String leader() {
        return leader;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }
}
