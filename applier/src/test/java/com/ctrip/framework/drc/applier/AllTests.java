package com.ctrip.framework.drc.applier;

import com.ctrip.framework.drc.applier.activity.event.TransactionTableApplierDumpEventActivityTest;
import com.ctrip.framework.drc.applier.activity.replicator.driver.ApplierPooledConnectorTest;
import com.ctrip.framework.drc.applier.confirmed.ConfirmedTests;
import com.ctrip.framework.drc.applier.container.ApplierServerContainerTest;
import com.ctrip.framework.drc.applier.container.controller.ApplierServerControllerTest;
import com.ctrip.framework.drc.applier.event.*;
import com.ctrip.framework.drc.applier.intergrated.ApplierTest;
import com.ctrip.framework.drc.applier.intergrated.ApplierTestWithMockedEvents;
import com.ctrip.framework.drc.applier.resource.TransactionTableResourceTest;
import com.ctrip.framework.drc.applier.resource.condition.LWMResourceInnerBucketTest;
import com.ctrip.framework.drc.applier.resource.condition.LWMResourceInnerChartTest;
import com.ctrip.framework.drc.applier.resource.condition.LWMResourceTest;
import com.ctrip.framework.drc.applier.resource.condition.ProgressResourceTest;
import com.ctrip.framework.drc.applier.resource.context.*;
import com.ctrip.framework.drc.applier.resource.context.savepoint.DefaultSavepointExecutorTest;
import com.ctrip.framework.drc.applier.resource.context.sql.*;
import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResourceTest;
import com.ctrip.framework.drc.applier.server.ApplierServerInClusterTest;
import com.ctrip.framework.drc.applier.server.ApplierWatcherTest;
import com.ctrip.framework.drc.applier.server.LocalApplierServerTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @Author Slight
 * Sep 18, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        //config
        ProgressResourceTest.class,
        ApplierPooledConnectorTest.class,

        LocalApplierServerTest.class,
        ApplierServerInClusterTest.class,
        ApplierWatcherTest.class,

        //Event
        ApplierDrcTableMapEventTest.class,
        ApplierGtidEventTest.class,
        ApplierTableMapEventTest.class,
        ApplierRowsEventTest.class,
        ApplierWriteRowsEventTest.class,
        ApplierUpdateRowsEventTest.class,
        ApplierDeleteRowsEventTest.class,
        ApplierXidEventTest.class,


        //Resource
        //Condition
        LWMResourceInnerBucketTest.class,
        LWMResourceInnerChartTest.class,
        LWMResourceTest.class,
        //Context
        GtidContextTest.class,
        StatementExecutorResultTest.class,
        //Those who needs MySQL instance.
        TransactionContextResourceTest.class,
        BatchTransactionContextResourceTest.class,
        DefaultSavepointExecutorTest.class,
        BatchPreparedStatementExecutorTest.class,
        PartialTransactionContextResourceTest.class,
        PartialBigTransactionContextResourceTest.class,
        TransactionTableResourceTest.class,
        //SQL
        InsertBuilderTest.class,
        UpdateBuilderTest.class,
        DeleteBuilderTest.class,
        SelectBuilderTest.class,
        SQLUtilTest.class,
        ColumnsContainerTest.class,
        ConditionExtractorTest.class,

        //ConfirmedTests.class,
        ConfirmedTests.class,

        ApplierServerContainerTest.class,
        DataSourceResourceTest.class,

        //integrated
        ApplierTest.class,
        ApplierTestWithMockedEvents.class,
        ApplierServerControllerTest.class,

        //activity
        TransactionTableApplierDumpEventActivityTest.class
})
public class AllTests {

    public static WireMockServer wireMockServer;

    public static final String HTTP_IP = "127.0.0.1";

    @BeforeClass
    public static void setUp() {
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
