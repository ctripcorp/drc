package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.GtidContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 27, 2019
 */
public class GtidContextTest {

    class TestContext extends AbstractContext implements GtidContext {}

    TestContext context;

    @Before
    public void setUp() throws Exception {
        context = new TestContext();
        context.initialize();
    }

    @After
    public void tearDown() throws Exception {
        context.dispose();
        context = null;
    }

    @Test
    public void simpleUse() {
        context.updateGtid("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-108240921");
        assertEquals("9f51357e-1cb1-11e8-a7f9-fa163e00d7c8:1-108240921", context.fetchGtid());
    }
}