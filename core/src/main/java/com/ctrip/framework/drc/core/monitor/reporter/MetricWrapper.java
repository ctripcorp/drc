package com.ctrip.framework.drc.core.monitor.reporter;

import java.util.Map;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-01-15
 */
public class MetricWrapper {

    private Map<String, String> tags;

    private String measurement;

    public MetricWrapper(Map<String, String> tags, String measurement) {
        this.tags = tags;
        this.measurement = measurement;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getMeasurement() {
        return measurement;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricWrapper)) {
            return false;
        }
        MetricWrapper that = (MetricWrapper) o;
        return Objects.equals(getTags(), that.getTags()) &&
                Objects.equals(getMeasurement(), that.getMeasurement());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getTags(), getMeasurement());
    }
}
