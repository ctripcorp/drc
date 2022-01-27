package com.ctrip.framework.drc.core.filter.aviator;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jixinwang on 2021/11/24
 */
public class AviatorRegexFilterTest {

    @Test
    public void test_regex() {
        AviatorRegexFilter filter = new AviatorRegexFilter("s1\\..*,s2\\..*");
        boolean result = filter.filter("s1.t1");
        Assert.assertEquals(true, result);

        result = filter.filter("s1.t2");
        Assert.assertEquals(true, result);

        result = filter.filter("");
        Assert.assertEquals(true, result);

        result = filter.filter("s12.t1");
        Assert.assertEquals(false, result);

        result = filter.filter("s2.t2");
        Assert.assertEquals(true, result);

        result = filter.filter("s3.t2");
        Assert.assertEquals(false, result);

        result = filter.filter("S1.S2");
        Assert.assertEquals(true, result);

        result = filter.filter("S2.S1");
        Assert.assertEquals(true, result);

        AviatorRegexFilter filter2 = new AviatorRegexFilter("s1\\..*,s2.t1");

        result = filter2.filter("s1.t1");
        Assert.assertEquals(true, result);

        result = filter2.filter("s1.t2");
        Assert.assertEquals(true, result);

        result = filter2.filter("s2.t1");
        Assert.assertEquals(true, result);

        AviatorRegexFilter filter3 = new AviatorRegexFilter("foooo,f.*t");

        result = filter3.filter("fooooot");
        Assert.assertEquals(true, result);

        AviatorRegexFilter filter4 = new AviatorRegexFilter("otter2.otter_stability1|otter1.otter_stability1|retl.retl_mark|retl.retl_buffer|retl.xdual");
        result = filter4.filter("otter1.otter_stability1");
        Assert.assertEquals(true, result);
    }

    @Test
    public void testDisordered() {
        AviatorRegexFilter filter = new AviatorRegexFilter("u\\..*,uvw\\..*,uv\\..*,a\\.x,a\\.xyz,a\\.xy,abc\\.x,abc\\.xyz,abc\\.xy,ab\\.x,ab\\.xyz,ab\\.xy");

        boolean result = filter.filter("u.abc");
        Assert.assertEquals(true, result);

        result = filter.filter("ab.x");
        Assert.assertEquals(true, result);

        result = filter.filter("ab.xyz1");
        Assert.assertEquals(false, result);

        result = filter.filter("abc.xyz");
        Assert.assertEquals(true, result);

        result = filter.filter("uv.xyz");
        Assert.assertEquals(true, result);

    }



    @Test
    public void test_multi_aviator_filter() {
        int count = 5;
        ExecutorService executor = Executors.newFixedThreadPool(count);

        final CountDownLatch countDown = new CountDownLatch(count);
        final AtomicInteger successed = new AtomicInteger(0);
        for (int i = 0; i < count; i++) {
            executor.submit(() -> {
                try {
                    for (int i1 = 0; i1 < 100; i1++) {
                        doRegexTest();
                        // try {
                        // Thread.sleep(10);
                        // } catch (InterruptedException e) {
                        // }
                    }

                    successed.incrementAndGet();
                } finally {
                    countDown.countDown();
                }
            });
        }

        try {
            countDown.await();
        } catch (InterruptedException e) {
        }

        Assert.assertEquals(count, successed.get());
        executor.shutdownNow();
    }

    private void doRegexTest() {
        AviatorRegexFilter filter3 = new AviatorRegexFilter("otter2.otter_stability1|otter1.otter_stability1|"
                + RandomStringUtils.randomAlphabetic(200));
        boolean result = filter3.filter("otter1.otter_stability1");
        Assert.assertEquals(true, result);
        result = filter3.filter("otter2.otter_stability1");
        Assert.assertEquals(true, result);
    }
}
