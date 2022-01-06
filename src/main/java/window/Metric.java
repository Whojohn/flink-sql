package window;

import java.math.BigDecimal;
import java.util.Map;

// disable lombok
//@Data
//@AllArgsConstructor
//@NoArgsConstructor
public class Metric {
    // 不合理的tag
    private Map<String, String> tags;
    private Map<String, BigDecimal> metrics;
    private Long eventTime;

    public Metric() {
    }

    public String getTag(String name) {
        return tags.get(name);
    }

    public BigDecimal getMetric(String name) {
        return metrics.get(name);
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, BigDecimal> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, BigDecimal> metrics) {
        this.metrics = metrics;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "tags=" + tags +
                ", metrics=" + metrics +
                ", eventTime=" + eventTime +
                '}';
    }
}