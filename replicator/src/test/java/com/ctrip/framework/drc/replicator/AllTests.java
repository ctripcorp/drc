package com.ctrip.framework.drc.replicator;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.replicator.container.ReplicatorServerContainerTest;
import com.ctrip.framework.drc.replicator.container.config.TableFilterConfigurationTest;
import com.ctrip.framework.drc.replicator.impl.inbound.converter.ReplicatorByteBufConverterTest;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorConnectionTest;
import com.ctrip.framework.drc.replicator.impl.inbound.event.EventTransactionCacheTest;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorLogEventHandlerTest;
import com.ctrip.framework.drc.replicator.impl.inbound.event.ReplicatorTableMapLogEventTest;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.*;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.DdlIndexFilterTest;
import com.ctrip.framework.drc.replicator.impl.inbound.filter.transaction.TransactionTableFilterTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.SchemaManagerFactoryTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.index.IndexExtractorTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.parse.DdlParserTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.RetryTaskTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.SchemeApplyTaskTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.SchemeClearTaskTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.SchemeCloneTaskTest;
import com.ctrip.framework.drc.replicator.impl.inbound.transaction.BackupTransactionEventTest;
import com.ctrip.framework.drc.replicator.impl.monitor.DefaultMonitorManagerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.MySQLMasterServerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegionTest;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.FileRegionMessageSizeEstimatorTest;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ApplierRegisterCommandHandlerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.DelayMonitorCommandHandlerTest;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.ReplicatorMasterHandlerTest;
import com.ctrip.framework.drc.replicator.store.FilePersistenceEventStoreTest;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManagerTest;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultIndexFileManagerTest;
import com.ctrip.framework.drc.replicator.store.manager.gtid.DefaultGtidManagerTest;
import com.ctrip.framework.drc.replicator.store.manager.gtid.GtidConsumerTest;
import com.wix.mysql.EmbeddedMysql;
import ctrip.framework.drc.mysql.EmbeddedDb;
import org.apache.curator.test.TestingServer;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Created by @author zhuYongMing on 2019/9/18.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        FilterChainFactoryTest.class,
        TableFilterConfigurationTest.class,
        ReplicatorServerContainerTest.class,
        FileRegionMessageSizeEstimatorTest.class,
        BinlogFileRegionTest.class,
        SchemaManagerFactoryTest.class,
        ApplierRegisterCommandHandlerTest.class,
        DefaultIndexFileManagerTest.class,
        DefaultFileManagerTest.class,
        EventTransactionCacheTest.class,
        DefaultGtidManagerTest.class,
        BackupTransactionEventTest.class,

        IndexExtractorTest.class,
        ReplicatorConnectionTest.class,
        // impl package
        // inbound package
        // converter package
        ReplicatorByteBufConverterTest.class,
//        // event package
        ReplicatorLogEventHandlerTest.class,
//
        FilePersistenceEventStoreTest.class,
        GtidConsumerTest.class,
        MySQLMasterServerTest.class,
//
        // filters
        EventReleaseFilterTest.class,
        PersistPostFilterTest.class,
        DelayMonitorFilterTest.class,
        TransactionDefaultMonitorManagerFilterTest.class,
        EventTypeFilterTest.class,
        UuidFilterTest.class,
        DdlFilterTest.class,
        BlackTableNameFilterTest.class,
        DdlIndexFilterTest.class,

        //transaction filters
        TransactionTableFilterTest.class,

        // ddl
        DdlParserTest.class,
        RetryTaskTest.class,
//        DbInitTaskTest.class,
        SchemeCloneTaskTest.class,
        SchemeClearTaskTest.class,
        SchemeApplyTaskTest.class,

        DefaultMonitorManagerTest.class,
        ReplicatorTableMapLogEventTest.class,
        ReplicatorMasterHandlerTest.class,
        DelayMonitorCommandHandlerTest.class

})
public class AllTests {

    public static final int SRC_PORT = 13309;

    public static final String SRC_IP = "127.0.0.1";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PASSWORD = "";

    private static final String CREATE_TABLE1 = "CREATE TABLE `drc1`.`t1` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final String CREATE_TABLE2 = "CREATE TABLE `drc1`.`t` (\n" +
            "                        `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "                        `one` varchar(30) DEFAULT \"one\",\n" +
            "                        `two` varchar(1000) DEFAULT \"two\",\n" +
            "                        `three` char(30),\n" +
            "                        `four` char(255),\n" +
            "                        `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "                        PRIMARY KEY (`id`)\n" +
            "                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final String CREATE_TABLE3 = "CREATE TABLE `drc2`.`t2` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `name` varchar(15) NOT NULL DEFAULT 'name',\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
            "  `addcol` varchar(55) DEFAULT 'make it pass' COMMENT '添加普通',\n" +
            "  `drc_id` int(11) NOT NULL DEFAULT '1',\n" +
            "  `addcol1` varchar(64) DEFAULT 'default_addcol1' COMMENT 'test',\n" +
            "  `addcol2` varchar(64) DEFAULT 'default_addcol2' COMMENT 'test',\n" +
            "  `drc_id_test` int(11) NOT NULL DEFAULT '123',\n" +
            "  `drc_char_test` char(30) DEFAULT 'char',\n" +
            "  `drc_tinyint_test` tinyint(5) DEFAULT '12',\n" +
            "  `drc_bigint_test` bigint(100) DEFAULT '120',\n" +
            "  `drc_integer_test` int(50) DEFAULT '11',\n" +
            "  `drc_mediumint_test` mediumint(15) DEFAULT '12345',\n" +
            "  `drc_time6_test` time(6) DEFAULT '02:02:02.000000',\n" +
            "  `drc_datetime3_test` datetime(3) DEFAULT '2019-01-01 01:01:01.000',\n" +
            "  `drc_year_test` year(4) DEFAULT '2020',\n" +
            "  `drc_binary200_test` binary(200) DEFAULT 'binary200\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0',\n" +
            "  `drc_varbinary1800_test` varbinary(1800) DEFAULT 'varbinary1800',\n" +
            "  `drc_numeric10_4_test` decimal(10,4) DEFAULT '100.0000',\n" +
            "  `drc_float_test` float DEFAULT '12',\n" +
            "  `drc_double_test` double DEFAULT '123',\n" +
            "  `drc_double10_4_test` double(10,4) DEFAULT '123.1245',\n" +
            "  `hourly_rate` decimal(10,2) NOT NULL DEFAULT '1.00',\n" +
            "  `drc_bit4_test` bit(4) DEFAULT b'111',\n" +
            "  `drc_real_test` double DEFAULT '234',\n" +
            "  `drc_real10_4_test` double(10,4) DEFAULT '23.4000',\n" +
            "  PRIMARY KEY (`id`,`name`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=49 DEFAULT CHARSET=utf8";

    private static final String CREATE_TABLE4 = "CREATE TABLE `drc3`.`order_category` (\n" +
            "  `ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',\n" +
            "  `BasicOrderID` bigint(20) DEFAULT NULL COMMENT '关联表basicorder主键ID',\n" +
            "  `OrderID` bigint(20) DEFAULT NULL COMMENT '订单编号',\n" +
            "  `BizType` smallint(6) DEFAULT NULL COMMENT '业务类型（OrderDbType）',\n" +
            "  `UID` varchar(20) DEFAULT NULL COMMENT '用户名',\n" +
            "  `Category` smallint(6) DEFAULT NULL COMMENT '订单分类（1-点评，2-出行，3-支付）',\n" +
            "  `StatusCode` smallint(6) DEFAULT NULL COMMENT '状态码（0-未发生，1-已发生）',\n" +
            "  `StartDate` datetime(3) DEFAULT NULL COMMENT '开始时间（点评-可点评开始时间，出行-无，支付-无）',\n" +
            "  `EndDate` datetime(3) DEFAULT NULL COMMENT '截止时间（点评-点评截止时间，出行-出行时间，支付-无）',\n" +
            "  `DataChange_CreateTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间戳（精确到毫秒）',\n" +
            "  `DataChange_LastTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '时间戳（精确到毫秒）',\n" +
            "  PRIMARY KEY (`ID`),\n" +
            "  UNIQUE KEY `UNI_BasicOrderId_Category` (`BasicOrderID`,`Category`),\n" +
            "  KEY `IDX_UID` (`UID`),\n" +
            "  KEY `idx_DataChange_LastTime` (`DataChange_LastTime`),\n" +
            "  KEY `idx_uid_orderid` (`UID`,`OrderID`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=1210300339 DEFAULT CHARSET=utf8 COMMENT='订单分类'";

    private static EmbeddedMysql srcDb;

    private static TestingServer server;

    public static int previous_gtidset_interval = 1024 * 5;

    @BeforeClass
    public static void setUp() {
        System.setProperty(SystemConfig.PREVIOUS_GTID_INTERVAL, String.valueOf(previous_gtidset_interval));
        try {
            server = new TestingServer(12181, true);

            //for db
            srcDb = new EmbeddedDb().mysqlServer(SRC_PORT);
            DataSource dataSource = DataSourceManager.getInstance().getDataSource(new DefaultEndPoint(SRC_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD));
            try (Connection connection = dataSource.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("create database drc1;");
                    statement.execute(CREATE_TABLE1);
                    statement.execute(CREATE_TABLE2);

                    statement.execute("create database drc2;");
                    statement.execute(CREATE_TABLE3);

                    statement.execute("create database drc3;");
                    statement.execute(CREATE_TABLE4);
                }
            }

        } catch (Exception e) {
            System.out.println(e.getStackTrace());
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

}
