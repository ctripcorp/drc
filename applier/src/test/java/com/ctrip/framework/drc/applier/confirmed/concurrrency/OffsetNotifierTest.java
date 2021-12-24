package com.ctrip.framework.drc.applier.confirmed.concurrrency;

import com.ctrip.xpipe.utils.OffsetNotifier;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @Author Slight
 * Oct 27, 2019
 */
public class OffsetNotifierTest {

    @Test
    public void simpleUse() throws InterruptedException {
        OffsetNotifier notifier = new OffsetNotifier(0);
        notifier.offsetIncreased(10);
        notifier.await(10);
    }

    @Test
    public void successImmediately() throws InterruptedException {
        OffsetNotifier notifier = new OffsetNotifier(0);
        notifier.offsetIncreased(10);
        assertTrue(notifier.await(10, 0));
        assertTrue(notifier.await(9, 0));
    }

    @Test
    public void failImmediately() throws InterruptedException {
        OffsetNotifier notifier = new OffsetNotifier(0);
        notifier.offsetIncreased(9);
        assertFalse(notifier.await(10, 0));
    }
}
