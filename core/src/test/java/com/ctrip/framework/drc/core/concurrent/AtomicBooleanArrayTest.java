package com.ctrip.framework.drc.core.concurrent;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yongnian
 * @create 2024/12/31 16:38
 */
public class AtomicBooleanArrayTest {

    @Test
    public void test() {
        AtomicBooleanArray array = new AtomicBooleanArray(100);
        for (int i = 0; i < 100; i++) {
            Assert.assertFalse(array.get(i));
        }
        for (int i = 0; i < 100; i++) {
            array.set(i, true);
            Assert.assertTrue(array.get(i));
        }

        for (int i = 0; i < 100; i++) {
            array.set(i, false);
            Assert.assertFalse(array.get(i));
        }
    }
}

