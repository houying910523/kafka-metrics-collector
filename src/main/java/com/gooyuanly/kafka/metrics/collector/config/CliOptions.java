package com.gooyuanly.kafka.metrics.collector.config;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * author: hy
 * date: 2019/2/9
 * desc:
 */
public class CliOptions {

    private final String configFile;
    private final String reporter;
    private final int parallelism;

    public CliOptions(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", "config", true, "配置文件");
        options.addOption(null, "reporter", true, "influxdb地址");
        options.addOption("p", "parallelism", true, "并行度");
        CommandLine commandLine = new DefaultParser().parse(options, args);
        this.configFile = commandLine.getOptionValue("config");
        this.reporter = commandLine.getOptionValue("reporter");
        this.parallelism = Integer.valueOf(commandLine.getOptionValue("parallelism"));
    }

    public List<KafkaConfig> getConfigs() throws IOException {
        FileReader fileReader = new FileReader(configFile);
        BufferedReader reader = new BufferedReader(fileReader);
        List<KafkaConfig> result = Lists.newArrayList();
        String line;
        while((line = reader.readLine()) != null) {
            String[] array = line.split("=");
            if (array.length != 2) {
                throw new RuntimeException(line);
            }
            String name = array[0];
            String zkPath = array[1];
            int idx = zkPath.indexOf("/");
            result.add(new KafkaConfig(name, zkPath.substring(0, idx), zkPath.substring(idx, zkPath.length())));
        }
        return result;
    }

    public String getReporter() {
        return reporter;
    }

    public int getParallelism() {
        return parallelism;
    }
}
