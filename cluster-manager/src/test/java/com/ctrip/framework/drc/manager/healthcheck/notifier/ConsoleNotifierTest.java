package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.manager.AllTests;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/3/4
 */
public class ConsoleNotifierTest extends AbstractNotifierTest {

    private static final String CLUSTER_NAME = "drcTest-1";

    private ConsoleNotifier consoleNotifier;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        consoleNotifier = ConsoleNotifier.getInstance();
        consoleNotifier.setDomain(AllTests.HTTP_IP + ":" + AllTests.HTTP_PORT);
        consoleNotifier.setClusterName(CLUSTER_NAME);

    }

    @Test
    public void notifyHttp() throws InterruptedException {
        consoleNotifier.notify(dbCluster);
        Thread.sleep(500);
    }
}