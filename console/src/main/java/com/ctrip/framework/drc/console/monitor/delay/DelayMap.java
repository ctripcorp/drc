package com.ctrip.framework.drc.console.monitor.delay;

import com.ctrip.framework.drc.console.io.dropwizard.metrics5.Histogram;
import com.ctrip.framework.drc.console.io.dropwizard.metrics5.MetricName;
import com.ctrip.framework.drc.console.io.dropwizard.metrics5.MetricRegistry;

import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-03
 */
public class DelayMap extends LinkedHashMap<DelayMap.DrcDirection, Histogram> {

    private MetricRegistry registry = new MetricRegistry();

    private static class DelayMapHolder {
        public static final DelayMap INSTANCE = new DelayMap();
    }

    public static DelayMap getInstance() {
        return DelayMapHolder.INSTANCE;
    }

    private DelayMap() {
    }

    public void put(DrcDirection drcDirection, long delayInMillis) {
        Histogram histogram = getHistogram(drcDirection);
        histogram.update(delayInMillis);
    }

    public double avg(DrcDirection drcDirection) {
        Histogram histogram = getHistogram(drcDirection);
        return histogram.getSnapshot().getMean();
    }

    public void clear(DrcDirection drcDirection) {
        registry.remove(MetricName.build(drcDirection.toString()));
        remove(drcDirection);
    }

    public int size(DrcDirection drcDirection) {
        if(!containsKey(drcDirection)) {
            return -1;
        }
        return get(drcDirection).getSnapshot().size();
    }

    private synchronized Histogram getHistogram(DrcDirection drcDirection) {
        if(!containsKey(drcDirection)) {
            put(drcDirection, registry.histogram(drcDirection.toString()));
        }
        return get(drcDirection);
    }

    public static final class DrcDirection {

        private String srcMha;

        private String destMha;

        public DrcDirection(String srcMha, String destMha) {
            this.srcMha = srcMha;
            this.destMha = destMha;
        }

        public String getSrcMha() {
            return srcMha;
        }

        public String getDestMha() {
            return destMha;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof DrcDirection)) return false;
            DrcDirection that = (DrcDirection) o;
            return Objects.equals(getSrcMha(), that.getSrcMha()) &&
                    Objects.equals(getDestMha(), that.getDestMha());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getSrcMha(), getDestMha());
        }

        @Override
        public String toString() {
            return String.format("%s_%s", srcMha, destMha);
        }
    }
}