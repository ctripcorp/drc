package com.ctrip.framework.drc.console.io.dropwizard.metrics5;

/**
 * <p>
 * Similar to {@link Gauge}, but metric value is updated via calling {@link #setValue(T)} instead.
 * See {@link DefaultSettableGauge}.
 * </p>
 */
public interface SettableGauge<T> extends Gauge<T> {
    /**
     * Set the metric to a new value.
     */
    void setValue(T value);
}
