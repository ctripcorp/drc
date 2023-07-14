package com.ctrip.framework.drc.fetcher.system;

import java.util.Map;

public interface MetricReporter {

    MetricReporter DEFAULT = new Empty();

    class Empty implements MetricReporter {

        @Override
        public void report(String name, Map<String,String> tags, long value) {
        }
    }

    void report(String name, Map<String,String> tags, long value);
}
