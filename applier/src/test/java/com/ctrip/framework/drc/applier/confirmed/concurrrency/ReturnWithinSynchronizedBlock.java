package com.ctrip.framework.drc.applier.confirmed.concurrrency;

import org.junit.Test;

/**
 * @Author Slight
 * Oct 27, 2019
 */
public class ReturnWithinSynchronizedBlock {

    Object lock = new Object();

    public void rwsb() {
        synchronized (lock) {
            return;
        }
    }

    @Test
    public void rwsbUnlock() {
        rwsb();
        synchronized (lock) {
        }
    }
}
