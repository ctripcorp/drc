package com.ctrip.framework.drc.core.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/6/6 20:38
 */
public class ScheduleCloseGateTest {
    @Test
    public void testScheduleClose() throws Exception {
        ScheduleCloseGate scheduleCloseGate = new ScheduleCloseGate("ut");
        scheduleCloseGate.scheduleClose();
        Assert.assertTrue(scheduleCloseGate.closeIfScheduled());
        Assert.assertFalse(scheduleCloseGate.isOpen());
        Assert.assertFalse(scheduleCloseGate.closeIfScheduled());
        scheduleCloseGate.open();
        Assert.assertTrue(scheduleCloseGate.isOpen());
        Assert.assertFalse(scheduleCloseGate.closeIfScheduled());
    }

    @Test
    public void testClose() throws Exception {
        ScheduleCloseGate scheduleCloseGate = new ScheduleCloseGate("ut");
        scheduleCloseGate.close();
        Assert.assertFalse(scheduleCloseGate.isOpen());
        scheduleCloseGate.open();
        Assert.assertTrue(scheduleCloseGate.isOpen());
    }


    @Test
    public void testConcurrentCloseAndOpen() {
        ScheduleCloseGate gate = new ScheduleCloseGate("ut");

        var ref = new Object() {
            boolean running = true;
        };
        Thread thread = new Thread(() -> {
            while (ref.running) {
                gate.closeIfScheduled();
            }
        });
        thread.start();

        for (int i = 0; i < 10000; i++) {
            gate.scheduleClose();
            gate.open();
            // closeIfScheduled should not affect the final result (open)
            System.out.println(i);
            Assert.assertTrue(gate.isOpen());
            Assert.assertFalse(gate.closeIfScheduled());
        }

        for (int i = 0; i < 10000; i++) {
            gate.open();
            gate.close();
            // closeIfScheduled should not affect the final result (close)
            System.out.println(i);
            Assert.assertFalse(gate.isOpen());
        }
        ref.running = false;
    }
}
