package com.gooyuanly.kafka.metrics.collector.reporter;

import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author hy
 * @date 2019/8/2
 * @desc
 */
public class InfluxDbReporter implements Reporter {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbReporter.class);
    private final InfluxDB influxDB;

    public InfluxDbReporter(String address) {
        this.influxDB = InfluxDBFactory.connect("http://" + address);
        influxDB.setDatabase("kafka_monitor");
        BatchOptions options = BatchOptions.DEFAULTS.exceptionHandler((points, throwable) -> {
            logger.error("write batch error", throwable);
            points.forEach(point -> {
                logger.info("write error point: {}", point);
            });
        });
        influxDB.enableBatch(options);
    }

    @Override
    public void report(String measurement, JmxMetricItem jmxMetricItem) {
        Point.Builder builder = Point.measurement(measurement)
                .addField("name", jmxMetricItem.getValue())
                .time(jmxMetricItem.getTimestamp(), TimeUnit.MILLISECONDS);
        jmxMetricItem.getTags().forEach(builder::tag);
        Point point = builder.build();
        influxDB.write(point);
    }

    @Override
    public void flush() throws IOException {
        influxDB.flush();
    }

    @Override
    public void close() throws IOException {
        influxDB.flush();
        influxDB.close();
    }
}
