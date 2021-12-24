package com.ctrip.framework.drc.fetcher.resource.context;

/**
 * @Author Slight
 * Jan 02, 2020
 */
public interface TimeTraceContext extends Context.Simple, Context {

    String KEY_NAME = "time trace";

    class TimeTrace {
        public StringBuilder trace;
        public long startTime;
        public long delay;
        public int depth;

        TimeTrace(String mark) {
            trace = new StringBuilder(mark);
            startTime = System.nanoTime();
            delay = 0;
            depth = 0;
        }
    }

    default void beginTrace(String mark) {
        update(KEY_NAME, new TimeTrace(mark));
    }

    default void atTrace(String mark) {
        try {
            TimeTrace trace = fetchTrace();
            trace.delay = System.nanoTime() - trace.startTime;
            trace.trace.append(" ");
            trace.trace.append(mark);
            trace.trace.append(trace.delay/1000);
            trace.trace.append("us");
            trace.depth += 1;
        } catch (Throwable throwable) {
            beginTrace(mark);
        }
    }

    default String endTrace(String mark) {
        atTrace(mark);
        return fetchTrace().trace.toString();
    }

    default long fetchDelayMS() {
        return fetchTrace().delay/1000000;
    }

    default int fetchDepth() {
        return fetchTrace().depth;
    }

    default TimeTrace fetchTrace() {
        return (TimeTrace) fetch(KEY_NAME);
    }

    default void updateTrace(TimeTrace trace) {
        update(KEY_NAME, trace);
    }
}
