package com.gooyuanly.kafka.metrics.collector.jmx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class JmxMonitorItem {
    private static final Pattern nameRegex = Pattern.compile(".+[,:]name=.+");

    private final String beanName;
    private final String attribution;

    public JmxMonitorItem applyTopic(String topic) {
        String newBeanName = beanName.replace("${topic}", topic);
        return new JmxMonitorItem(newBeanName, attribution);
    }

    JmxMonitorItem(String beanName, String attribution) {
        this.beanName = validate(beanName);
        this.attribution = attribution;
    }

    private String validate(String beanName) {
        Matcher matcher = nameRegex.matcher(beanName);
        if (!matcher.find()) {
            throw new RuntimeException();
        }
        return beanName;
    }

    public String getBeanName() {
        return beanName;
    }

    public String getAttribution() {
        return attribution;
    }
}
