package com.ctrip.framework.drc.console.cost;

import java.util.List;

/**
 * Created by jixinwang on 2022/9/9
 */
public class FlowEntity {

    FlowMetric metric;

    List<Object> values;

    public FlowMetric getMetric() {
        return metric;
    }

    public void setMetric(FlowMetric metric) {
        this.metric = metric;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowEntity that = (FlowEntity) o;

        if (!metric.equals(that.metric)) return false;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        int result = metric.hashCode();
        result = 31 * result + values.hashCode();
        return result;
    }
}
