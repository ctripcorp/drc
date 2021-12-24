package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.LogTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-28
 */
public class AbstractMonitorTest {

    private static final String IP = "127.0.0.1";

    private static final int PORT1 = 8383;

    private static final int PORT2 = 8384;

    private static final String USER = "root";

    private static final String PASSWORD = "root";

    private TestMonitor testMonitor = new TestMonitor();

    private Endpoint endpoint1;

    private Endpoint endpoint2;

    private Map<Endpoint, WriteSqlOperatorWrapper> sqlOperatorWrapperMap;

    @Before
    public void setUp() {
        testMonitor.start();
        endpoint1 = new DefaultEndPoint(IP, PORT1, USER, PASSWORD);
        endpoint2 = new DefaultEndPoint(IP, PORT2, USER, PASSWORD);
        sqlOperatorWrapperMap = testMonitor.getSqlOperatorMapper();
    }

    @Test
    public void testGetSqlOperatorWrapper() {
        Assert.assertEquals(0, sqlOperatorWrapperMap.size());

        WriteSqlOperatorWrapper sqlOperatorWrapper = testMonitor.getSqlOperatorWrapper(endpoint1);
        Assert.assertTrue(sqlOperatorWrapper.getLifecycleState().isStarted());
        Assert.assertEquals(1, sqlOperatorWrapperMap.size());
        Assert.assertNotNull(sqlOperatorWrapperMap.get(endpoint1));
        Assert.assertTrue(sqlOperatorWrapperMap.get(endpoint1).getLifecycleState().isStarted());

        sqlOperatorWrapper = testMonitor.getSqlOperatorWrapper(endpoint2);
        Assert.assertTrue(sqlOperatorWrapper.getLifecycleState().isStarted());
        Assert.assertEquals(2, sqlOperatorWrapperMap.size());
        Assert.assertNotNull(sqlOperatorWrapperMap.get(endpoint2));
        Assert.assertTrue(sqlOperatorWrapperMap.get(endpoint2).getLifecycleState().isStarted());

        sqlOperatorWrapper = testMonitor.getSqlOperatorWrapper(endpoint1);
        Assert.assertTrue(sqlOperatorWrapper.getLifecycleState().isStarted());
        Assert.assertEquals(2, sqlOperatorWrapperMap.size());
        Assert.assertNotNull(sqlOperatorWrapperMap.get(endpoint1));
        Assert.assertTrue(sqlOperatorWrapperMap.get(endpoint1).getLifecycleState().isStarted());
    }

    @Test
    public void testCLog() {
        Map<String, String> tags = new HashMap<>() {{
            put("hello", "world");
            put("foo", "bar");
        }};
        // manually check the print out logs
        testMonitor.cLog(tags, "testMsg", LogTypeEnum.INFO, null);
        testMonitor.cLog(tags, "testMsg", LogTypeEnum.ERROR, new Exception("testError"));
        testMonitor.cLog(tags, "testMsg", LogTypeEnum.WARN, null);
    }

    @Test
    public void testGetCLogPrefix() {
        Map<String, String> nullTags = null;
        Assert.assertEquals("", testMonitor.getCLogPrefix(nullTags));

        Map<String, String> tags = new LinkedHashMap<>();
        Assert.assertEquals("", testMonitor.getCLogPrefix(tags));

        tags.put("hello", "world");
        Assert.assertEquals("[[hello=world]]", testMonitor.getCLogPrefix(tags));

        tags.put("foo", "bar");
        Assert.assertEquals("[[hello=world,foo=bar]]", testMonitor.getCLogPrefix(tags));
    }

    @After
    public void tearDown() {
        sqlOperatorWrapperMap.keySet().forEach(endpoint -> testMonitor.removeSqlOperator(endpoint));
        testMonitor.destroy();
    }

    public static final class TestMonitor extends AbstractMonitor {

        public Map<Endpoint, WriteSqlOperatorWrapper> getSqlOperatorMapper() {
            return Collections.unmodifiableMap(sqlOperatorMapper);
        }

        @Override
        public void scheduledTask() {

        }
    }
}
