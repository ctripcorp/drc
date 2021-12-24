package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import com.ctrip.framework.drc.fetcher.resource.context.AbstractContext;
import com.ctrip.framework.drc.fetcher.resource.context.TableKeyContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Oct 27, 2019
 */
public class TableKeyContextTest {

    class TestContext extends AbstractContext implements TableKeyContext {}

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
        context.updateTableKey(TableKey.from("prod", "hello"));
        assertEquals("`prod`.`hello`", context.fetchTableKey().toString());
    }
}