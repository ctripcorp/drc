package com.ctrip.framework.drc.core;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.ctrip.framework.drc.core.config.RegionConfigTest;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventTypeTest;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryTypeTest;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSetTest;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeaderTest;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeaderTest;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.DatabaseCreateTaskTest;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemeCloneTaskTest;
import com.ctrip.framework.drc.core.driver.binlog.util.CharsetConversionTest;
import com.ctrip.framework.drc.core.driver.command.handler.BinlogDumpGtidClientCommandHandlerTest;
import com.ctrip.framework.drc.core.driver.command.netty.DrcNettyClientPoolTest;
import com.ctrip.framework.drc.core.driver.command.netty.NettyClientFactoryTest;
import com.ctrip.framework.drc.core.driver.command.netty.codec.AuthenticateResultHandlerTest;
import com.ctrip.framework.drc.core.driver.command.netty.codec.HandshakeInitializationHandlerTest;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy.ConnectGeneratorTest;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacketTest;
import com.ctrip.framework.drc.core.driver.command.packet.client.*;
import com.ctrip.framework.drc.core.driver.command.packet.monitor.DelayMonitorCommandPacketTest;
import com.ctrip.framework.drc.core.driver.command.packet.server.ErrorPacketTest;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTaskTest;
import com.ctrip.framework.drc.core.driver.schema.SchemaTests;
import com.ctrip.framework.drc.core.driver.util.MySQLPasswordEncrypterTest;
import com.ctrip.framework.drc.core.meta.DataMediaConfigTest;
import com.ctrip.framework.drc.core.meta.RowsFilterConfigTest;
import com.ctrip.framework.drc.core.meta.comparator.DcRouteComparatorTest;
import com.ctrip.framework.drc.core.monitor.column.DelayMonitorColumnTest;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnumTest;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReportTest;
import com.ctrip.framework.drc.core.monitor.util.IsolateHashCacheTest;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterTypeTest;
import com.ctrip.framework.drc.core.server.common.filter.row.*;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilterTest;
import com.ctrip.framework.drc.core.server.config.ApplierRegistryKeyTest;
import com.ctrip.framework.drc.core.server.config.DefaultFileConfigTest;
import com.ctrip.framework.drc.core.server.config.RegistryKeyTest;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDtoTest;
import com.ctrip.framework.drc.core.server.config.cm.dto.SchemasHistoryDeltaDtoTest;
import com.ctrip.framework.drc.core.server.ha.zookeeper.DrcLeaderElectorTest;
import com.ctrip.framework.drc.core.server.manager.DataMediaManagerTest;
import com.ctrip.framework.drc.core.server.utils.FileUtilTest;
import com.ctrip.framework.drc.core.service.ops.AppNodeTest;
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
        JavaRegexRowsFilterRuleTest.class,
        AviatorRegexRowsFilterRuleTest.class,
        UidRowsFilterRuleTest.class,
        DefaultRuleFactoryTest.class,
        DataMediaManagerTest.class,
        RowsFilterConfigTest.class,
        DataMediaConfigTest.class,
        AbstractRowsFilterRuleTest.class,
        UidConfigurationTest.class,
        RowsFilterTypeTest.class,
        //schema
        SchemeCloneTaskTest.class,
        DatabaseCreateTaskTest.class,
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
        DeleteRowsEventTest.class,
        DrcErrorLogEventTest.class,
        DrcHeartbeatLogEventTest.class,
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
        UpdateRowsEventTest.class,
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
        HeartBeatResponsePacketTest.class,
        DelayMonitorCommandPacketTest.class,

        NettyClientFactoryTest.class,
        // utils package
        CharsetConversionTest.class,
        MySQLPasswordEncrypterTest.class,
        FileUtilTest.class,
        IsolateHashCacheTest.class,

        //schema package
        SchemaTests.class,

        //config package
        ApplierConfigDtoTest.class,
        SchemasHistoryDeltaDtoTest.class,
        RegionConfigTest.class,

        //ddl
        DrcDdlLogEventTest.class,

        // monitor
        DcRouteComparatorTest.class,
        OutboundMonitorReportTest.class,

        //config
        DefaultFileConfigTest.class,
        RegistryKeyTest.class,
        ApplierRegistryKeyTest.class,

        //Http response test
        AppNodeTest.class,

        DrcLeaderElectorTest.class,

        BinlogDumpGtidClientCommandHandlerTest.class,
        ModuleEnumTest.class,
        QueryTypeTest.class,

        DelayMonitorColumnTest.class,

        // table regex filter
        AviatorRegexFilterTest.class,

        // QueryTask
        ExecutedGtidQueryTaskTest.class,

        // Write row field with MySQL number type
        WriteFieldTinyTypeTest.class,
        WriteFieldTinyUnsignedTypeTest.class,
        WriteFieldShortTypeTest.class,
        WriteFieldShortUnsignedTypeTest.class,
        WriteFieldInt24TypeTest.class,
        WriteFieldInt24UnsignedTypeTest.class,
        WriteFieldLongTypeTest.class,
        WriteFieldLongUnsignedTypeTest.class,
        WriteFieldLongLongTypeTest.class,
        WriteFieldLongLongUnsignedTypeTest.class,
        WriteFieldBitMeta1TypeTest.class,
        WriteFieldBitMeta2TypeTest.class,
        WriteFieldBitMeta3TypeTest.class,
        WriteFieldBitMeta4TypeTest.class,
        WriteFieldBitMeta5TypeTest.class,
        WriteFieldBitMeta6TypeTest.class,
        WriteFieldBitMeta7TypeTest.class,
        WriteFieldBitMeta8TypeTest.class,

        // Write row field with MySQL float type
        WriteFieldFloatTypeTest.class,
        WriteFieldFloatUnsignedTypeTest.class,
        WriteFieldDoubleTypeTest.class,
        WriteFieldDoubleUnsignedTypeTest.class,
        WriteFieldNewdecimalOffset0TypeTest.class,
        WriteFieldNewdecimalOffset1TypeTest.class,
        WriteFieldNewdecimalOffset2TypeTest.class,
        WriteFieldNewdecimalOffset3TypeTest.class,
        WriteFieldNewdecimalOffset4TypeTest.class,

        // Write row field with MySQL character type
        WriteFieldCharVarTypeTest.class,
        WriteFieldCharTypeTest.class,
        WriteFieldBinaryVarTypeTest.class,
        WriteFieldBinaryTypeTest.class,
        WriteFieldBlobTinyTypeTest.class,
        WriteFieldBlobTypeTest.class,
        WriteFieldBlobMediumTypeTest.class,
        WriteFieldBlobLongTypeTest.class,
        WriteFieldTextTinyTypeTest.class,
        WriteFieldTextTypeTest.class,
        WriteFieldTextMediumTypeTest.class,
        WriteFieldTextLongTypeTest.class,

        // Write row field with MySQL time type
        WriteFieldDatetime2Meta0TypeTest.class,
        WriteFieldDatetime2Meta2TypeTest.class,
        WriteFieldDatetime2Meta4TypeTest.class,
        WriteFieldDatetime2Meta6TypeTest.class,
        WriteFieldTime2Meta0TypeTest.class,
        WriteFieldTime2Meta2TypeTest.class,
        WriteFieldTime2Meta4TypeTest.class,
        WriteFieldTime2Meta6TypeTest.class,
        WriteFieldTimestamp2Meta0TypeTest.class,
        WriteFieldTimestamp2Meta2TypeTest.class,
        WriteFieldTimestamp2Meta4TypeTest.class,
        WriteFieldTimestamp2Meta6TypeTest.class,
        WriteFieldYearTypeTest.class,
        WriteFieldDateTypeTest.class,
        WriteFieldEnumMeta1TypeTest.class,
        WriteFieldEnumMeta2TypeTest.class

})
public class AllTests {
    /**
     {"rowsFilters":[{"mode":"%s","tables":"drc1.insert1","parameters":{"columns":["id","one"],"fetchMode":0,"context":"%s"},"configs":{"parameters":[{"columns":["id","one"],"fetchMode":0,"context":"%s","userFilterMode":"uid"}]}}],"talbePairs":[{"source":"sourceTableName1","target":"targetTableName1"},{"source":"sourceTableName2","target":"targetTableName2"}]}
     */

    public static final String ROW_FILTER_PROPERTIES = "{\n" +
            "  \"rowsFilters\": [\n" +
            "    {\n" +
            "      \"mode\": \"%s\",\n" +
            "      \"tables\": \"drc1.insert1\",\n" +
            "      \"parameters\": {\n" +
            "        \"columns\": [\n" +
            "          \"id\",\n" +
            "          \"one\"\n" +
            "        ],\n" +
            "        \"fetchMode\": 0,\n" +
            "        \"context\": \"%s\"\n" +
            "      },\n" +
            "      \"configs\": {\n" +
            "        \"parameters\": [\n" +
            "          {\n" +
            "            \"columns\": [\n" +
            "              \"id\",\n" +
            "              \"one\"\n" +
            "            ],\n" +
            "            \"fetchMode\": 0,\n" +
            "            \"context\": \"%s\",\n" +
            "            \"userFilterMode\": \"uid\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"talbePairs\": [\n" +
            "    {\n" +
            "      \"source\": \"sourceTableName1\",\n" +
            "      \"target\": \"targetTableName1\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"source\": \"sourceTableName2\",\n" +
            "      \"target\": \"targetTableName2\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    public static int ZK_PORT = 2182;

    public static int SRC_PORT = 3308;

    public static String IP = "127.0.0.1";

    public static String MYSQL_USER = "root";

    public static String MYSQL_PASSWORD = "";

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
            SRC_PORT = 3306;
            MYSQL_PASSWORD = "123456";
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
