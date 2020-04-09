package com.gooyuanly.kafka.metrics.collector.reporter;

import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;

import java.io.Closeable;
import java.io.Flushable;

/**
 * @author hy
 * @date 2019/8/2
 * @desc
 */
public interface Reporter extends Closeable, Flushable {

    void report(String clusterName, JmxMetricItem jmxMetricItem);
}
