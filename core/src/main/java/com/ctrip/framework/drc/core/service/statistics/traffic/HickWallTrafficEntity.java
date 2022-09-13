package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.util.List;

/**
 * Created by jixinwang on 2022/9/9
 */
public class HickWallTrafficEntity {

    HickWallTrafficMetric metric;

    List<Object> values;

    public HickWallTrafficMetric getMetric() {
        return metric;
    }

    public void setMetric(HickWallTrafficMetric metric) {
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

        HickWallTrafficEntity that = (HickWallTrafficEntity) o;

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
