package com.ctrip.framework.drc.applier.confirmed.java8;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author Slight
 * Jun 28, 2020
 */
public class CAS {

    @Test
    public void testBoolean() {
        AtomicBoolean b = new AtomicBoolean(false);
        assert b.compareAndSet(false, true);
        assert !b.compareAndSet(false, true);
        assert !b.compareAndSet(false, true);
        assert b.get();
        assert b.compareAndSet(true, false);
        assert !b.get();
    }
}
