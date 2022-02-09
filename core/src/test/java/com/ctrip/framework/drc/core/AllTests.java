package com.ctrip.framework.drc.core;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventTypeTest;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryTypeTest;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSetTest;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeaderTest;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeaderTest;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.util.CharsetConversionTest;
import com.ctrip.framework.drc.core.driver.command.handler.BackupBinlogDumpGtidClientCommandHandlerTest;
import com.ctrip.framework.drc.core.driver.command.handler.BinlogDumpGtidClientCommandHandlerTest;
import com.ctrip.framework.drc.core.driver.command.impl.replicator.ComBackupBinlogDumpGtidCommandTest;
import com.ctrip.framework.drc.core.driver.command.netty.DrcNettyClientPoolTest;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactoryTest;
import com.ctrip.framework.drc.core.driver.command.netty.codec.AuthenticateResultHandlerTest;
import com.ctrip.framework.drc.core.driver.command.netty.codec.HandshakeInitializationHandlerTest;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ConnectGeneratorTest;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacketTest;
import com.ctrip.framework.drc.core.driver.command.packet.client.*;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacketTest;
import com.ctrip.framework.drc.core.driver.command.packet.server.ErrorPacketTest;
import com.ctrip.framework.drc.core.driver.schema.SchemaTests;
import com.ctrip.framework.drc.core.driver.util.MySQLPasswordEncrypterTest;
import com.ctrip.framework.drc.core.filter.aviator.AviatorRegexFilterTest;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparatorTest;
import com.ctrip.framework.drc.core.monitor.column.DelayMonitorColumnTest;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnumTest;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKeyTest;
import com.ctrip.framework.drc.core.server.config.DefaultFileConfigTest;
import com.ctrip.framework.drc.core.server.config.RegistryKeyTest;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDtoTest;
import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDeltaDtoTest;
import com.ctrip.framework.drc.core.server.ha.zookeeper.DrcLeaderElectorTest;
import com.ctrip.framework.drc.core.server.utils.FileUtilTest;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author wenchao.meng
 * <p>
 * Sep 11, 2019
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        //proxy
        DrcNettyClientPoolTest.class,
        ConnectGeneratorTest.class,

        AuthenticateResultHandlerTest.class,
        HandshakeInitializationHandlerTest.class,
        CharsetConversionTest.class,

        // binlog package

        // constant package
        LogEventTypeTest.class,

        // gtid package
        GtidSetTest.class,

        // header package
        LogEventHeaderTest.class,
        RowsEventPostHeaderTest.class,

        // impl package
        AbstractLogEventTest.class,
        AbstractRowsEventTest.class,
//        DeleteRowsEventTest.class,
        DrcErrorLogEventTest.class,
        FormatDescriptionLogEventTest.class,
        GtidLogEventTest.class,
        HeartBeatLogEventTest.class,
        PreviousGtidsLogEventTest.class,
        QueryLogEventTest.class,
        RotateLogEventTest.class,
        RowsEventBinlogImageTest.class,
        RowsEventCharsetTest.class,
        RowsEventCharsetTypeTest.class,
        RowsEventFloatPointType.class,
        RowsEventNumericTypeTest.class,
        RowsEventTimeTypeTest.class,
        RowsQueryLogEventTest.class,
        StopLogEventTest.class,
        TableMapLogEventTest.class,
//        UpdateRowsEventTest.class,
        WriteRowsEventTest.class,
        XidLogEventTest.class,
        DrcSchemaSnapshotLogEventTest.class,
        DrcUuidLogEventTest.class,
        DelayMonitorLogEventTest.class,
        TransactionEventTest.class,
        ParsedDdlLogEventTest.class,

        // command package
        ApplierDumpCommandPacketTest.class,
        AuthSwitchResponsePacketTest.class,
        BinlogDumpGtidCommandPacketTest.class,
        ClientAuthenticationPacketTest.class,
        QueryCommandPacketTest.class,
        RegisterSlaveCommandPacketTest.class,
        ErrorPacketTest.class,
        HeartBeatPacketTest.class,
        DelayMonitorCommandPacketTest.class,

        NettyClientFactoryTest.class,
        // utils package
        CharsetConversionTest.class,
        MySQLPasswordEncrypterTest.class,
        FileUtilTest.class,

        //schema package
        SchemaTests.class,

        //config package
        ApplierConfigDtoTest.class,
        SchemasHistoryDeltaDtoTest.class,

        //ddl
        DrcDdlLogEventTest.class,

        // monitor
        DcRouteComparatorTest.class,

        //config
        DefaultFileConfigTest.class,
        RegistryKeyTest.class,
        ApplierRegistryKeyTest.class,

        DrcLeaderElectorTest.class,

        BackupBinlogDumpGtidClientCommandHandlerTest.class,
        BinlogDumpGtidClientCommandHandlerTest.class,
        ComBackupBinlogDumpGtidCommandTest.class,
        ModuleEnumTest.class,
        QueryTypeTest.class,

        DelayMonitorColumnTest.class,

        // table regex filter
        AviatorRegexFilterTest.class,
})
public class AllTests {

    public static final int SRC_PORT = 3308;

    public static final int ZK_PORT = 2182;

    public static final String IP = "127.0.0.1";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PASSWORD = "";

    private static DB srcDb;

    private static TestingServer server;

    @BeforeClass
    public static void setUp() {
        try {
            //for db
            srcDb = getDb(SRC_PORT);

            //for zookeeper
            server = new TestingServer(ZK_PORT, true);
        } catch (Exception e) {
        }
    }

    @AfterClass
    public static void tearDown()
    {
        try {
            srcDb.stop();
            server.stop();
        } catch (Exception e) {
        }
    }

    private static DB getDb(int port) throws ManagedProcessException {
        DB db = DB.newEmbeddedDB(port);
        db.start();
        db.source("db/init.sql");
        return db;
    }

}
