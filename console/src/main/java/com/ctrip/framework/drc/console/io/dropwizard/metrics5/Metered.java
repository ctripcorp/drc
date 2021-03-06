package com.ctrip.framework.drc.console.io.dropwizard.metrics5;

/**
 * An object which maintains mean and moving average rates.
 */
public interface Metered extends Metric, Counting, Summing {
    /**
     * Returns the number of events which have been marked.
     *
     * @return the number of events which have been marked
     */
    @Override
    long getCount();

    /**
     * Returns the sum of events which have been marked.
     *
     * @return the sum of events which have been marked
     */
    @Override
    long getSum();

    /**
     * Returns the fifteen-minute moving average rate at which events have
     * occurred since the meter was created.
     *
     * @return the fifteen-minute moving average rate at which events have
     * occurred since the meter was created
     */
    double getFifteenMinuteRate();

    /**
     * Returns the five-minute moving average rate at which events have
     * occurred since the meter was created.
     *
     * @return the five-minute moving average rate at which events have
     * occurred since the meter was created
     */
    double getFiveMinuteRate();

    /**
     * Returns the mean rate at which events have occurred since the meter was created.
     *
     * @return the mean rate at which events have occurred since the meter was created
     */
    double getMeanRate();

    /**
     * Returns the one-minute moving average rate at which events have
     * occurred since the meter was created.
     *
     * @return the one-minute moving average rate at which events have
     * occurred since the meter was created
     */
    double getOneMinuteRate();
}
