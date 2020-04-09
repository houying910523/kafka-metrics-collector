package com.gooyuanly.kafka.metrics.collector.reporter;

import com.google.common.collect.Maps;
import com.gooyuanly.kafka.metrics.collector.jmx.JmxMetricItem;
import com.ke.streaming.common.utils.JsonUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author hy
 * @date 2020/4/8
 * @desc
 */
public class TsdbReporter implements Reporter {
    private static final Logger logger = LoggerFactory.getLogger(TsdbReporter.class);

    private static final String URL = "/api/put";
    private final CloseableHttpClient client;
    private final Thread thread;
    private final String api;
    private LinkedBlockingQueue<TsdbMetric> queue;
    private volatile boolean stop = false;

    public TsdbReporter(String address) {
        this.api = "http://" + address + URL;
        this.client = HttpClients.custom()
                .setMaxConnTotal(5)
                .setConnectionTimeToLive(5, TimeUnit.MINUTES)
                .build();
        this.queue = new LinkedBlockingQueue<>();
        this.thread = new Thread(() -> {
            logger.info("tsdb flush thread start");
            while(!stop) {
                try {
                    Thread.sleep(500L);
                    flush();
                } catch (InterruptedException e) {
                    return;
                } catch (IOException e) {
                    logger.error("flush cause error", e);
                }
            }
        });
        thread.start();
    }

    @Override
    public void report(String clusterName, JmxMetricItem jmxMetricItem) {
        try {
            this.queue.put(convert(clusterName, jmxMetricItem));
            if (queue.size() >= 50) {
                flush();
            }
        } catch (InterruptedException e) {
            //do nothing
        } catch (IOException e) {
            logger.error("flush cause error", e);
        }
    }

    private TsdbMetric convert(String clusterName, JmxMetricItem jmxMetricItem) {
        TsdbMetric metric = new TsdbMetric();
        metric.setMetric(jmxMetricItem.getName());
        metric.setTimestamp(jmxMetricItem.getTimestamp());
        metric.setValue(jmxMetricItem.getValue());
        metric.setTags(jmxMetricItem.getTags());
        metric.getTags().put("clusterName", clusterName);
        return metric;
    }

    @Override
    public void close() throws IOException {
        stop = true;
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            //do nothing
        }
        client.close();
    }

    @Override
    public void flush() throws IOException {
        if (this.queue.isEmpty()) {
            return;
        }
        Collection<TsdbMetric> list = this.queue;
        this.queue = new LinkedBlockingQueue<>();

        HttpPost post = new HttpPost(api);
        String json = JsonUtil.toJsonString(list);
        post.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
        try(CloseableHttpResponse response = client.execute(post)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.warn(EntityUtils.toString(response.getEntity()));
            }
            EntityUtils.consume(response.getEntity());
        }
    }

    class TsdbMetric {
        long timestamp;
        String metric;
        Long value;
        Map<String, String> tags;

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public Long getValue() {
            return value;
        }

        public void setValue(Long value) {
            this.value = value;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags = tags;
        }
    }
}
