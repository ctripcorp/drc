package com.ctrip.framework.drc.core.monitor.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * @Author Slight
 * Dec 28, 2019
 */
public class Frequency {

    protected final Logger logger;

    protected long start = 0;
    protected long count = 0;

    public Frequency(String name) {
        logger = LoggerFactory.getLogger(name);
    }

    public void addOne() {
        add(1);
    }

    public void add(long delta) {
        long now = System.currentTimeMillis();
        long start = now/1000;
        if (start == this.start) {
            count = count + delta;
        } else {
            record(count);
            this.start = start;
            count = delta;
        }
    }

    public synchronized void addOneCurrently() {
        addOne();
    }

    public synchronized void addConcurrently(long delta) { add(delta); }

    protected void record(long count) {
        logger.info(new Date(this.start * 1000) + ": " + count);
    }
}
