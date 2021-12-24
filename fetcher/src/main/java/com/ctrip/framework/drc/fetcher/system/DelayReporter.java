package com.ctrip.framework.drc.fetcher.system;

public interface DelayReporter {

    DelayReporter DEFAULT = new Empty();

    class Empty implements DelayReporter {

        @Override
        public void report(String name, String dbName, long value) {
        }
    }

    void report(String name, String dbName, long value);
}
