package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class EventGroupContextTest {

    TestContext context;

    class TestContext extends AbstractContext implements EventGroupContext {}

    @Before
    public void setUp() throws Exception {
        context = new TestContext();
        context.initialize();
        context.updateGtidSet(new GtidSet(""));
    }

    @Test
    public void merge() {
        context.begin("a0780e56-d445-11e9-97b4-58a328e0e9f2:100").commit();
        assertEquals("a0780e56-d445-11e9-97b4-58a328e0e9f2:100", context.fetchGtidSet().toString());
        context.begin("a0780e56-d445-11e9-97b4-58a328e0e9f2:101");
        assertEquals("a0780e56-d445-11e9-97b4-58a328e0e9f2:100", context.fetchGtidSet().toString());
        assertEquals("a0780e56-d445-11e9-97b4-58a328e0e9f2:101", context.fetchGtid());
        context.commit();
        assertEquals("a0780e56-d445-11e9-97b4-58a328e0e9f2:100-101", context.fetchGtidSet().toString());
    }

}