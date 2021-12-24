package com.ctrip.framework.drc.applier.resource.condition;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author limingdong
 * @create 2021/2/4
 */
public class LWMResourceInnerChartTest {

    private LWMResource.InnerChart innerChart;

    private AtomicLong times = new AtomicLong(0);

    private int step = 1000;

    private long low = Math.abs(new Random().nextInt(step));

    private long high = low + step;

    @Before
    public void setUp() {
        innerChart = new LWMResource.InnerChart();
        for (long i = low; i < high; ++i) {
            innerChart.add(i);
            innerChart.add(i, () -> times.addAndGet(1));
        }
    }

    @Test
    public void testTick() throws InterruptedException {
        innerChart.tick(low - 1);
        Assert.assertEquals(times.get(), 0);
        innerChart.tick(low);
        Assert.assertEquals(times.get(), 1);

        innerChart.tick(high);
        Assert.assertEquals(times.get(), step);

        innerChart.tick(high);
        Assert.assertEquals(times.get(), step);
    }


}
