package com.gooyuanly.kafka.metrics.collector;

import com.google.common.collect.Lists;
import com.gooyuanly.kafka.metrics.collector.config.CliOptions;
import com.gooyuanly.kafka.metrics.collector.config.KafkaConfig;
import com.gooyuanly.kafka.metrics.collector.kafka.KafkaCluster;
import com.gooyuanly.kafka.metrics.collector.reporter.InfluxDbReporter;
import com.gooyuanly.kafka.metrics.collector.reporter.Reporter;
import com.gooyuanly.kafka.metrics.collector.reporter.TsdbReporter;

import javax.management.relation.RoleUnresolved;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author hy
 * @date 2020/4/7
 * @desc
 */
public class KafkaMetricsCollector {

    public static void main(String[] args) throws Exception {
        CliOptions cliOptions = new CliOptions(args);
        String reportUrl = cliOptions.getReporter();
        int parallelism = cliOptions.getParallelism();
        List<KafkaConfig> list = cliOptions.getConfigs();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(parallelism, parallelism, 6, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), new ThreadFactory() {
            int i = 0;
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("worker-" + i);
                i++;
                return thread;
            }
        });

        Scheduler scheduler = new Scheduler(5, TimeUnit.MINUTES);

        Reporter reporter = createReporter(reportUrl);

        List<KafkaCluster> clusters = Lists.newArrayListWithCapacity(list.size());
        for (KafkaConfig kc: list) {
            KafkaCluster cluster = new KafkaCluster(kc, threadPoolExecutor);
            cluster.setReporter(reporter);
            cluster.start();
            scheduler.addTask(() -> {
                long timestamp = System.currentTimeMillis();
                cluster.fetch(timestamp);
            });
            clusters.add(cluster);
        }

        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                scheduler.stop();
                for (KafkaCluster kafkaCluster: clusters) {
                    kafkaCluster.close();
                }
                reporter.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }));
    }

    private static Reporter createReporter(String reportUrl) throws Exception {
        reportUrl = reportUrl.toLowerCase();
        URL url = new URL(null, reportUrl, new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) throws IOException {
                return null;
            }
        });
        String protocol = url.getProtocol();
        String address = url.getHost();
        if (url.getPort() != -1) {
            address = address + ":" + url.getPort();
        }
        if ("influxdb".equals(protocol)) {
            return new InfluxDbReporter(address);
        } else if ("tsdb".equals(protocol)) {
            return new TsdbReporter(address);
        } else {
            throw new RuntimeException("");
        }
    }
}
