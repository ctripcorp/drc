package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.TimeTraceContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Jan 02, 2020
 */
public class TimeTraceContextTest {

    @Test
    public void simpleUse() throws Exception {
        class TestContext extends AbstractContext implements TimeTraceContext {}
        TestContext context = new TestContext();
        context.initialize();
        context.beginTrace("A");
        context.atTrace("B");
        context.atTrace("C");
        context.endTrace("D");
    }

    @Test
    public void forgetToBegin() throws Exception {
        class TestContext extends AbstractContext implements TimeTraceContext {}
        TestContext context = new TestContext();
        context.initialize();
        context.endTrace("D");
    }

    @Test
    public void transfer() throws Exception {
        class TestContext extends AbstractContext implements TimeTraceContext {}
        TestContext c1 = new TestContext();
        TestContext c2 = new TestContext();
        c1.initialize();
        c2.initialize();

        c1.beginTrace("A");
        c1.atTrace("B");
        c1.endTrace("C");
        c2.updateTrace(c1.fetchTrace());

        assertEquals(c1.fetchDelayMS(), c2.fetchDelayMS());
    }
}