package com.ctrip.framework.drc.core.monitor.log;

import java.util.Date;

/**
 * @Author Slight
 * Dec 31, 2019
 */
public class Accumulation extends Frequency {

    private String unit;

    public Accumulation(String name, String unit) {
        super(name);
        this.unit = unit;
    }

    @Override
    protected void record(long count) {
        logger.info(new Date(this.start * 1000) + ": " + count + unit);
    }
}
