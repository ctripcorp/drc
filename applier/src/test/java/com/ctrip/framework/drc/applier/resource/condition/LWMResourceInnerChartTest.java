package com.ctrip.framework.drc.applier.resource.condition;

import com.ctrip.framework.drc.fetcher.resource.condition.LWMPassHandler;
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

    @Test
    public void testClear() {
        LWMResource.InnerChart innerChart = new LWMResource.InnerChart();


        int count = 10;
        final int[] closeCount = {0};
        for (long i = 0; i < count; ++i) {
            innerChart.add(i);
            innerChart.add(i, new LWMPassHandler() {
                @Override
                public void onBegin() throws InterruptedException {
                    times.addAndGet(1);
                }
                @Override
                public void close(){
                    closeCount[0]++;
                }
            });
        }
        Assert.assertNotEquals(0, innerChart.size());
        Assert.assertEquals(0, closeCount[0]);
        innerChart.clear();
        Assert.assertEquals(0, innerChart.size());
        Assert.assertEquals(closeCount[0], count);

    }


}
