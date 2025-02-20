package com.ctrip.framework.drc.messenger;


import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.messenger.activity.event.MqApplierDumpEventActivityTest;
import com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivityTest;
import com.ctrip.framework.drc.messenger.activity.replicator.driver.MqPooledConnectorTest;
import com.ctrip.framework.drc.messenger.container.MqServerContainerTest;
import com.ctrip.framework.drc.messenger.container.controller.MqServerControllerTest;
import com.ctrip.framework.drc.messenger.container.controller.task.WatchKeyedTaskTest;
import com.ctrip.framework.drc.messenger.mq.*;
import com.ctrip.framework.drc.messenger.resource.context.MqTransactionContextResourceTest;
import com.ctrip.framework.drc.messenger.server.LocalApplierServerTest;
import com.ctrip.framework.drc.messenger.server.MessengerWatcherTest;
import com.ctrip.framework.drc.messenger.server.MqServerInClusterTest;
import com.ctrip.framework.drc.messenger.utils.MqDynamicConfigTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Random;


/**
 * @Author Slight
 * Sep 18, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        //Activity
        MqApplierDumpEventActivityTest.class,
        MqMetricsActivityTest.class,

        WatchKeyedTaskTest.class,
        MqServerControllerTest.class,
        MqServerContainerTest.class,

        MqPooledConnectorTest.class,

//        //Event
//        ApplierDeleteRowsEventTest.class,
//        ApplierUpdateRowsEventTest.class,
//        ApplierWriteRowsEventTest.class,

        MqPositionResourceTest.class,
        MqProviderResourceTest.class,

        MqTransactionContextResourceTest.class,

        //Server
        MessengerWatcherTest.class,
        LocalApplierServerTest.class,
        MqServerInClusterTest.class,

        MqDynamicConfigTest.class,
})
public class AllTests {

    public static WireMockServer wireMockServer;

    public static final String HTTP_IP = "127.0.0.1";

    public static int ROW_SIZE = new Random().nextInt(100) + 1;

    @BeforeClass
    public static void setUp() {
        System.setProperty(SystemConfig.MAX_BATCH_EXECUTE_SIZE, String.valueOf(ROW_SIZE * 2 + 1));
        //for http server
        wireMockServer = new WireMockServer();
        wireMockServer.start();
        WireMock.configureFor(HTTP_IP, wireMockServer.port());
    }

    @AfterClass
    public static void tearDown() {
        try {
            wireMockServer.stop();
        } catch (Exception e) {
        }
    }
}
