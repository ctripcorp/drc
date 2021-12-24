package com.ctrip.framework.drc.core.monitor.log;

import org.junit.Test;

/**
 /**
 * @Author Slight
 * Dec 28, 2019
 */
public class FrequencyTest {

    @Test
    public void simpleUse() {
        Frequency frequency = new Frequency("test");
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            frequency.addOne();
        }
    }
}