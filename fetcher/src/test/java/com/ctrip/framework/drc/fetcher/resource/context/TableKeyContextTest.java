package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.schema.data.TableKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @Author limingdong
 * @create 2021/3/15
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