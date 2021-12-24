package com.ctrip.framework.drc.fetcher.resource.context;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/15
 */
public class SequenceNumberContextTest {

    @Test
    public void simpleUse() throws Exception {
        TestContext context = new TestContext();
        context.initialize();
        String message = "nothing goes wrong";
        try {
            context.fetchSequenceNumber();
        } catch (RuntimeException e) {
            message = e.getMessage();
        }
        assertEquals("unavailable context value when fetch(), key: sequence number", message);
        context.updateSequenceNumber(1000);
        assertEquals(1000, context.fetchSequenceNumber());
        context.dispose();
    }

    class TestContext extends AbstractContext implements SequenceNumberContext {
    }

}