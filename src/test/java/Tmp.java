import com.gooyuanly.kafka.metrics.collector.reporter.InfluxDbReporter;
import com.gooyuanly.kafka.metrics.collector.reporter.Reporter;
import com.gooyuanly.kafka.metrics.collector.reporter.TsdbReporter;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * @author hy
 * @date 2020/4/8
 * @desc
 */
public class Tmp {

    public static void main(String[] args) throws Exception {
        createReporter("influxdb://127.0.0.1:8086");
    }

    static Reporter createReporter(String reportUrl) throws Exception {
        reportUrl = reportUrl.toLowerCase();
        int index = reportUrl.indexOf("://") + 3;
        URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory() {
            @Override
            public URLStreamHandler createURLStreamHandler(String protocol) {
                return new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL u) throws IOException {
                        return null;
                    }
                };
            }
        });
        URL url = new URL(reportUrl);
        System.out.println(url.getProtocol());
        if (reportUrl.startsWith("influxdb")) {
            return new InfluxDbReporter(reportUrl.substring(index, reportUrl.length()));
        } else if (reportUrl.startsWith("tsdb")) {
            return new TsdbReporter(reportUrl.substring(index, reportUrl.length()));
        } else {
            throw new RuntimeException("");
        }
    }
}
