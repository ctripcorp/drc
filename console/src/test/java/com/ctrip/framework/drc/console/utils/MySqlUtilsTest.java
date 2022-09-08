package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl.ALLMATCH;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-25
 */
public class MySqlUtilsTest {

    private static Logger logger = LoggerFactory.getLogger(MySqlUtilsTest.class);

    private static DataSource dataSource;

    private static final int CI_PORT3306 = 3306;

    private static final String CI_MYSQL_IP = "127.0.0.1";

    private static final String CI_MYSQL_USER = "root";

    private static final String CI_MYSQL_PASSWORD = "123456";

    private static Endpoint endpointCi3306;

    private static final String CREATE_CONFIGDB = "create database if not exists configdb;";

    private static final String USE_CONFIGDB = "use configdb";

    private static final String CREATE_TABLE_APPROVED_TRUNCATELIST = "CREATE TABLE if not exists `approved_truncatelist` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `db_name` varchar(100) DEFAULT NULL,\n" +
            "  `table_name` varchar(100) DEFAULT NULL,\n" +
            "  `proposer_name` varchar(30) DEFAULT NULL,\n" +
            "  `insert_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8";

    private static final String TRUNCATE_APPROVED_TRUNCATELIST = "TRUNCATE TABLE `configdb`.`approved_truncatelist`;";

    private static final String INSERT_APPROVED_TRUNCATELIST = "insert into approved_truncatelist(db_name, table_name) values('testdb', 'testtable');";

    private static final String CREATE_DB = "create database drcmonitordb;";

    private static final String USE_DB = "use drcmonitordb;";

    private static final String CREATE_TABLE1 = "CREATE TABLE `delaymonitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_TABLE2 = "CREATE TABLE `delay monitor` (\n" +
            "  `id2` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime2` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id2`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_TABLE3 = "CREATE TABLE `delaymonitor3` (\n" +
            "  `pid` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `update_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`pid`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_TABLE_WITHOUT_UPDATE = "CREATE TABLE `drcmonitordb`.`delaymonitorno` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_TABLE_WITHOUT_PK_UK = "CREATE TABLE `emptydb`.`delaymonitornopkuk` (\n" +
            "  `id` bigint(20) NOT NULL,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_DEL_TABLE="CREATE TABLE `_delaymonitor_del` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    private static final String CREATE_DEL_DB = "create database _del_drcmonitordb;";

    private static final String USE_DEL_DB = "use _del_drcmonitordb;";

    private static final String CREATE_EMPTY_DB = "create database emptydb;";

    private static final String CREATE_EMPTYDB_TABLE = "CREATE TABLE `emptydb`.`delaymonitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

    @Before
    public void setUp() throws InterruptedException {
        AllTests.dropAllCiDb();
        endpointCi3306 = new DefaultEndPoint(CI_MYSQL_IP, CI_PORT3306, CI_MYSQL_USER, CI_MYSQL_PASSWORD);
        initDb(endpointCi3306);
    }

    @Test
    public void testGetTables() throws InterruptedException {
        Thread.sleep(500);
        List<MySqlUtils.TableSchemaName> tableSchemaNames = MySqlUtils.getDefaultTables(endpointCi3306);
        List<String> actual = tableSchemaNames.stream().map(MySqlUtils.TableSchemaName::toString).collect(Collectors.toList());

        List<String> expected = new ArrayList<>() {{
            add("`drcmonitordb`.`delaymonitor`");
            add("`drcmonitordb`.`delay monitor`");
            add("`drcmonitordb`.`delaymonitor3`");
        }};
        Assert.assertEquals(3, actual.size());
        Assert.assertTrue(expected.contains(actual.get(0)));
        Assert.assertTrue(expected.contains(actual.get(1)));
        Assert.assertTrue(expected.contains(actual.get(2)));
    }

    @Test
    public void testGetCreateTblStmts() throws InterruptedException {
        Thread.sleep(500);
        Map<String, String> createTblStmts = MySqlUtils.getDefaultCreateTblStmts(endpointCi3306,new AviatorRegexFilter(ALLMATCH));
        Assert.assertEquals(3, createTblStmts.keySet().size());
        List<String> expected = new ArrayList<>() {{
            add(CREATE_TABLE1.toLowerCase().replaceAll("\r|\n", ""));
            add(CREATE_TABLE2.toLowerCase().replaceAll("\r|\n", ""));
            add(CREATE_TABLE3.toLowerCase().replaceAll("\r|\n", ""));
        }};
        createTblStmts.values().forEach(stmt -> {
            Assert.assertTrue(expected.contains(stmt));
        });
    }

    @Test
    public void testGetDefaultDelayMonitorConfigs() {
        Map<String, DelayMonitorConfig> defaultDelayMonitorConfigs = MySqlUtils.getDefaultDelayMonitorConfigs(endpointCi3306);
        Assert.assertEquals(2, defaultDelayMonitorConfigs.keySet().size());
        DelayMonitorConfig delayMonitorConfig1 = defaultDelayMonitorConfigs.get("`drcmonitordb`.`delaymonitor`");
        DelayMonitorConfig delayMonitorConfig2 = defaultDelayMonitorConfigs.get("`drcmonitordb`.`delay monitor`");
        DelayMonitorConfig delayMonitorConfig3 = defaultDelayMonitorConfigs.get("`drcmonitordb`.`delaymonitor3`");
        Assert.assertNull(delayMonitorConfig1);
        Assert.assertNotNull(delayMonitorConfig2);
        Assert.assertNotNull(delayMonitorConfig3);
        System.out.println(delayMonitorConfig1);
        System.out.println(delayMonitorConfig2);
        System.out.println(delayMonitorConfig3);
        Assert.assertEquals("drcmonitordb", delayMonitorConfig2.getSchema());
        Assert.assertEquals("delay monitor", delayMonitorConfig2.getTable());
        Assert.assertEquals("id2", delayMonitorConfig2.getKey());
        Assert.assertEquals("datachange_lasttime2", delayMonitorConfig2.getOnUpdate());
        Assert.assertEquals("drcmonitordb", delayMonitorConfig3.getSchema());
        Assert.assertEquals("delaymonitor3", delayMonitorConfig3.getTable());
        Assert.assertEquals("pid", delayMonitorConfig3.getKey());
        Assert.assertEquals("update_time", delayMonitorConfig3.getOnUpdate());
    }

    @Test
    public void testFilterStmt() {
        String rouchStmt = "CREATE TABLE `insert1` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'update time',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=33596 DEFAULT CHARSET=utf8";
        String expected = "create table `insert1` (\n" +
                "  `id` int(11) not null auto_increment,\n" +
                "  `one` varchar(30) default 'one',\n" +
                "  `two` varchar(1000) default 'two',\n" +
                "  `three` char(30) default null,\n" +
                "  `four` char(255) default null,\n" +
                "  `datachange_lasttime` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),\n" +
                "  primary key (`id`)\n" +
                ") engine=innodb default charset=utf8";
        expected = expected.replaceAll("\r|\n", "");
        String actual = MySqlUtils.filterStmt(rouchStmt, endpointCi3306, "drc1.insert1");
        Assert.assertEquals(expected, actual);

        rouchStmt = "CREATE TABLE `insert1` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'update time',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8/*some comments*/";
        expected = "create table `insert1` (\n" +
                "  `id` int(11) not null auto_increment,\n" +
                "  `one` varchar(30) default 'one',\n" +
                "  `two` varchar(1000) default 'two',\n" +
                "  `three` char(30) default null,\n" +
                "  `four` char(255) default null,\n" +
                "  `datachange_lasttime` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),\n" +
                "  primary key (`id`)\n" +
                ") engine=innodb default charset=utf8";
        expected = expected.replaceAll("\r|\n", "");
        actual = MySqlUtils.filterStmt(rouchStmt, endpointCi3306, "drc1.insert1");
        Assert.assertEquals(expected, actual);

        rouchStmt = "CREATE TABLE `machine_operate_log` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id',\n" +
                "  `app_code` varchar(20) NOT NULL DEFAULT '' COMMENT 'app_code',\n" +
                "  `machine_ip` varchar(20) NOT NULL DEFAULT '' COMMENT '机器ip',\n" +
                "  `operate_ip` varchar(20) DEFAULT NULL COMMENT '操作者ip',\n" +
                "  `operator` varchar(20) NOT NULL DEFAULT '' COMMENT '操作者',\n" +
                "  `data` varchar(2000) DEFAULT NULL COMMENT '操作内容',\n" +
                "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                "  PRIMARY KEY (`id`,`create_time`),\n" +
                "  KEY `app_ip` (`app_code`,`machine_ip`),\n" +
                "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=3196760 DEFAULT CHARSET=utf8mb4 COMMENT='机器拉入拉出操作日志表[[分区表类型:M,保留数:1,分区列:create_time]][[分区表类型:D,保留数:30,分区列:create_time]]'\n" +
                "/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(create_time))\n" +
                "(PARTITION p20201010 VALUES LESS THAN (1602345600) ENGINE = InnoDB,\n" +
                " PARTITION p20201011 VALUES LESS THAN (1602432000) ENGINE = InnoDB,\n" +
                " PARTITION p20201012 VALUES LESS THAN (1602518400) ENGINE = InnoDB,\n" +
                " PARTITION p20201013 VALUES LESS THAN (1602604800) ENGINE = InnoDB,\n" +
                " PARTITION p20201014 VALUES LESS THAN (1602691200) ENGINE = InnoDB,\n" +
                " PARTITION p20201015 VALUES LESS THAN (1602777600) ENGINE = InnoDB,\n" +
                " PARTITION p20201016 VALUES LESS THAN (1602864000) ENGINE = InnoDB,\n" +
                " PARTITION p20201017 VALUES LESS THAN (1602950400) ENGINE = InnoDB,\n" +
                " PARTITION p20201018 VALUES LESS THAN (1603036800) ENGINE = InnoDB,\n" +
                " PARTITION p20201019 VALUES LESS THAN (1603123200) ENGINE = InnoDB,\n" +
                " PARTITION p20201020 VALUES LESS THAN (1603209600) ENGINE = InnoDB,\n" +
                " PARTITION p20201021 VALUES LESS THAN (1603296000) ENGINE = InnoDB,\n" +
                " PARTITION p20201022 VALUES LESS THAN (1603382400) ENGINE = InnoDB,\n" +
                " PARTITION p20201023 VALUES LESS THAN (1603468800) ENGINE = InnoDB,\n" +
                " PARTITION p20201024 VALUES LESS THAN (1603555200) ENGINE = InnoDB,\n" +
                " PARTITION p20201025 VALUES LESS THAN (1603641600) ENGINE = InnoDB,\n" +
                " PARTITION p20201026 VALUES LESS THAN (1603728000) ENGINE = InnoDB,\n" +
                " PARTITION p20201027 VALUES LESS THAN (1603814400) ENGINE = InnoDB,\n" +
                " PARTITION p20201028 VALUES LESS THAN (1603900800) ENGINE = InnoDB,\n" +
                " PARTITION p20201029 VALUES LESS THAN (1603987200) ENGINE = InnoDB,\n" +
                " PARTITION p20201030 VALUES LESS THAN (1604073600) ENGINE = InnoDB,\n" +
                " PARTITION p20201031 VALUES LESS THAN (1604160000) ENGINE = InnoDB,\n" +
                " PARTITION p20201101 VALUES LESS THAN (1604246400) ENGINE = InnoDB,\n" +
                " PARTITION p20201102 VALUES LESS THAN (1604332800) ENGINE = InnoDB,\n" +
                " PARTITION p20201103 VALUES LESS THAN (1604419200) ENGINE = InnoDB,\n" +
                " PARTITION p20201104 VALUES LESS THAN (1604505600) ENGINE = InnoDB,\n" +
                " PARTITION p20201105 VALUES LESS THAN (1604592000) ENGINE = InnoDB,\n" +
                " PARTITION p20201106 VALUES LESS THAN (1604678400) ENGINE = InnoDB,\n" +
                " PARTITION p20201107 VALUES LESS THAN (1604764800) ENGINE = InnoDB,\n" +
                " PARTITION p20201108 VALUES LESS THAN (1604851200) ENGINE = InnoDB,\n" +
                " PARTITION p20201109 VALUES LESS THAN (1604937600) ENGINE = InnoDB,\n" +
                " PARTITION p20201110 VALUES LESS THAN (1605024000) ENGINE = InnoDB,\n" +
                " PARTITION p20201111 VALUES LESS THAN (1605110400) ENGINE = InnoDB,\n" +
                " PARTITION p20201112 VALUES LESS THAN (1605196800) ENGINE = InnoDB,\n" +
                " PARTITION p20201113 VALUES LESS THAN (1605283200) ENGINE = InnoDB,\n" +
                " PARTITION p20201114 VALUES LESS THAN (1605369600) ENGINE = InnoDB,\n" +
                " PARTITION p20201115 VALUES LESS THAN (1605456000) ENGINE = InnoDB,\n" +
                " PARTITION p20201116 VALUES LESS THAN (1605542400) ENGINE = InnoDB,\n" +
                " PARTITION p20201117 VALUES LESS THAN (1605628800) ENGINE = InnoDB,\n" +
                " PARTITION p20201118 VALUES LESS THAN (1605715200) ENGINE = InnoDB,\n" +
                " PARTITION p20201119 VALUES LESS THAN (1605801600) ENGINE = InnoDB,\n" +
                " PARTITION p20201120 VALUES LESS THAN (1605888000) ENGINE = InnoDB,\n" +
                " PARTITION p20201121 VALUES LESS THAN (1605974400) ENGINE = InnoDB,\n" +
                " PARTITION p20201122 VALUES LESS THAN (1606060800) ENGINE = InnoDB,\n" +
                " PARTITION p20201123 VALUES LESS THAN (1606147200) ENGINE = InnoDB,\n" +
                " PARTITION p20201124 VALUES LESS THAN (1606233600) ENGINE = InnoDB,\n" +
                " PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */";
        expected = "CREATE TABLE `machine_operate_log` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                "  `app_code` varchar(20) NOT NULL DEFAULT '',\n" +
                "  `machine_ip` varchar(20) NOT NULL DEFAULT '',\n" +
                "  `operate_ip` varchar(20) DEFAULT NULL,\n" +
                "  `operator` varchar(20) NOT NULL DEFAULT '',\n" +
                "  `data` varchar(2000) DEFAULT NULL,\n" +
                "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
                "  PRIMARY KEY (`id`,`create_time`),\n" +
                "  KEY `app_ip` (`app_code`,`machine_ip`),\n" +
                "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        expected = expected.toLowerCase().replaceAll("\r|\n", "");
        logger.info("expected: {}", expected);
        actual = MySqlUtils.filterStmt(rouchStmt, endpointCi3306, "q.machine");
        Assert.assertEquals(expected, actual);

        rouchStmt = "CREATE TABLE `insert1` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `one` varchar(30) DEFAULT 'one',\n" +
                "  `two` varchar(1000) DEFAULT 'two',\n" +
                "  `three` char(30) DEFAULT NULL,\n" +
                "  `four` char(255) DEFAULT NULL,\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'update time',\n" +
                "  PRIMARY KEY (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8/*some comments*/";
        expected = "create table `insert1` (\n" +
                "  `id` int(11) not null auto_increment,\n" +
                "  `one` varchar(30) default 'one',\n" +
                "  `two` varchar(1000) default 'two',\n" +
                "  `three` char(30) default null,\n" +
                "  `four` char(255) default null,\n" +
                "  `datachange_lasttime` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),\n" +
                "  primary key (`id`)\n" +
                ") engine=innodb default charset=utf8";
        expected = expected.replaceAll("\r|\n", "");
        actual = MySqlUtils.filterStmt(rouchStmt, endpointCi3306, "drc1.insert1");
        Assert.assertEquals(expected, actual);

        rouchStmt = "CREATE TABLE `m_notifyplanb` (\n" +
                "  `ID` bigint(19) NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "  `UniqueNo` varchar(256) DEFAULT NULL COMMENT '消息唯一序列号',\n" +
                "  `BusinessOrder` varchar(64) DEFAULT NULL COMMENT '业务订单号',\n" +
                "  `SubscribeId` bigint(19) DEFAULT NULL COMMENT '订阅端ID',\n" +
                "  `Url` varchar(512) DEFAULT NULL COMMENT '消息目的地',\n" +
                "  `Plugin` varchar(64) DEFAULT NULL COMMENT '消息处理插件',\n" +
                "  `ChannelEndpoint` varchar(128) DEFAULT NULL COMMENT '消息处理通道',\n" +
                "  `Topic` varchar(128) DEFAULT NULL COMMENT '消息主题',\n" +
                "  `Content` varchar(7000) DEFAULT NULL COMMENT '消息内容（JSON）',\n" +
                "  `Group` int(5) DEFAULT '0' COMMENT '消息组别',\n" +
                "  `Deadline` datetime DEFAULT NULL COMMENT '最迟发送时间',\n" +
                "  `RetryCount` int(3) DEFAULT '0' COMMENT '消息发送重试次数',\n" +
                "  `Operation` smallint(3) DEFAULT '0' COMMENT '操作类型',\n" +
                "  `Status` smallint(3) DEFAULT '0' COMMENT '状态\n" +
                "          /** 新建通知任务 */\n" +
                "                  NEW = 0,\n" +
                "                          /** 第一阶段提交，这时通知还不能发 */\n" +
                "                                  PRE = 1,\n" +
                "                                          /** 第二阶段提交，通知可以发了 */\n" +
                "                                                  COMMITE = 2,\n" +
                "                                                          /** 通知成功 */\n" +
                "                                                                  SUCCESS = 4',\n" +
                "  `DataChange_CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',\n" +
                "  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录最后更新时间',\n" +
                "  PRIMARY KEY (`ID`,`DataChange_LastTime`),\n" +
                "  KEY `index_lasttime` (`DataChange_LastTime`),\n" +
                "  KEY `index_uniqueno_stauts` (`UniqueNo`(255),`Status`),\n" +
                "  KEY `index_deadline_status_retrycount` (`Deadline`,`RetryCount`,`Status`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=24708001049 DEFAULT CHARSET=utf8 COMMENT='推送注册信息后备表，防止异步系统中产生的线程终止问题导致消息丢失'\n" +
                "/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(DataChange_LastTime))\n" +
                "(PARTITION p20201112 VALUES LESS THAN (1605196800) ENGINE = InnoDB,\n" +
                " PARTITION p20201113 VALUES LESS THAN (1605283200) ENGINE = InnoDB,\n" +
                " PARTITION p20201114 VALUES LESS THAN (1605369600) ENGINE = InnoDB,\n" +
                " PARTITION p20201115 VALUES LESS THAN (1605456000) ENGINE = InnoDB,\n" +
                " PARTITION p20201116 VALUES LESS THAN (1605542400) ENGINE = InnoDB,\n" +
                " PARTITION p20201117 VALUES LESS THAN (1605628800) ENGINE = InnoDB,\n" +
                " PARTITION p20201118 VALUES LESS THAN (1605715200) ENGINE = InnoDB,\n" +
                " PARTITION p20201119 VALUES LESS THAN (1605801600) ENGINE = InnoDB,\n" +
                " PARTITION p20201120 VALUES LESS THAN (1605888000) ENGINE = InnoDB,\n" +
                " PARTITION p20201121 VALUES LESS THAN (1605974400) ENGINE = InnoDB,\n" +
                " PARTITION p20201122 VALUES LESS THAN (1606060800) ENGINE = InnoDB,\n" +
                " PARTITION p20201123 VALUES LESS THAN (1606147200) ENGINE = InnoDB,\n" +
                " PARTITION p20201124 VALUES LESS THAN (1606233600) ENGINE = InnoDB,\n" +
                " PARTITION p20201125 VALUES LESS THAN (1606320000) ENGINE = InnoDB,\n" +
                " PARTITION p20201126 VALUES LESS THAN (1606406400) ENGINE = InnoDB,\n" +
                " PARTITION p20201127 VALUES LESS THAN (1606492800) ENGINE = InnoDB,\n" +
                " PARTITION p20201128 VALUES LESS THAN (1606579200) ENGINE = InnoDB,\n" +
                " PARTITION p20201129 VALUES LESS THAN (1606665600) ENGINE = InnoDB,\n" +
                " PARTITION p20201130 VALUES LESS THAN (1606752000) ENGINE = InnoDB,\n" +
                " PARTITION p20201201 VALUES LESS THAN (1606838400) ENGINE = InnoDB,\n" +
                " PARTITION p20201202 VALUES LESS THAN (1606924800) ENGINE = InnoDB,\n" +
                " PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */";
        expected = "CREATE TABLE `m_notifyplanb` (\n" +
                "  `ID` bigint(19) NOT NULL AUTO_INCREMENT,\n" +
                "  `UniqueNo` varchar(256) DEFAULT NULL,\n" +
                "  `BusinessOrder` varchar(64) DEFAULT NULL,\n" +
                "  `SubscribeId` bigint(19) DEFAULT NULL,\n" +
                "  `Url` varchar(512) DEFAULT NULL,\n" +
                "  `Plugin` varchar(64) DEFAULT NULL,\n" +
                "  `ChannelEndpoint` varchar(128) DEFAULT NULL,\n" +
                "  `Topic` varchar(128) DEFAULT NULL,\n" +
                "  `Content` varchar(7000) DEFAULT NULL,\n" +
                "  `Group` int(5) DEFAULT '0',\n" +
                "  `Deadline` datetime DEFAULT NULL,\n" +
                "  `RetryCount` int(3) DEFAULT '0',\n" +
                "  `Operation` smallint(3) DEFAULT '0',\n" +
                "  `Status` smallint(3) DEFAULT '0',\n" +
                "  `DataChange_CreateTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (`ID`,`DataChange_LastTime`),\n" +
                "  KEY `index_lasttime` (`DataChange_LastTime`),\n" +
                "  KEY `index_uniqueno_stauts` (`UniqueNo`(255),`Status`),\n" +
                "  KEY `index_deadline_status_retrycount` (`Deadline`,`RetryCount`,`Status`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        expected = expected.toLowerCase();
        expected = expected.replaceAll("\r|\n", "");
        actual = MySqlUtils.filterStmt(rouchStmt, endpointCi3306, "drc1.insert1");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCheckOnUpdate() {
        List<String> tables = MySqlUtils.checkOnUpdate(endpointCi3306, null);
        Assert.assertEquals(0, tables.size());

        doWrite(CREATE_TABLE_WITHOUT_UPDATE);
        tables = MySqlUtils.checkOnUpdate(endpointCi3306, null);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("drcmonitordb.delaymonitorno", tables.get(0));

        doWrite(CREATE_EMPTYDB_TABLE);
        tables = MySqlUtils.checkOnUpdate(endpointCi3306, Arrays.asList("drcmonitordb"));
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("drcmonitordb.delaymonitorno", tables.get(0));

        tables = MySqlUtils.checkOnUpdate(endpointCi3306, Arrays.asList("emptydb"));
        Assert.assertEquals(0, tables.size());

        tables = MySqlUtils.checkOnUpdate(endpointCi3306, Arrays.asList("drcmonitordb", "emptydb"));
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("drcmonitordb.delaymonitorno", tables.get(0));
    }

    @Test
    public void testCheckOnUpdateKey() {
        List<String> tables = MySqlUtils.checkOnUpdateKey(endpointCi3306);
        Assert.assertEquals(2, tables.size());
        Assert.assertTrue(tables.contains("`drcmonitordb`.`delay monitor`"));
        Assert.assertTrue(tables.contains("`drcmonitordb`.`delaymonitor3`"));
    }

    @Test
    public void testCheckUniqOrPrimary() {
        List<String> tables = MySqlUtils.checkUniqOrPrimary(endpointCi3306, null);
        Assert.assertEquals(0, tables.size());

        doWrite(CREATE_TABLE_WITHOUT_PK_UK);
        tables = MySqlUtils.checkUniqOrPrimary(endpointCi3306, Arrays.asList("drcmonitordb"));
        Assert.assertEquals(0, tables.size());

        tables = MySqlUtils.checkUniqOrPrimary(endpointCi3306, Arrays.asList("emptydb"));
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("emptydb.delaymonitornopkuk", tables.get(0));

        tables = MySqlUtils.checkUniqOrPrimary(endpointCi3306, Arrays.asList("drcmonitordb", "emptydb"));
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("emptydb.delaymonitornopkuk", tables.get(0));
    }

    @Test
    public void testCheckUniqOrPrimary2() {
        List<String> tables = MySqlUtils.checkUniqOrPrimary(endpointCi3306, Arrays.asList("drc"));
        System.out.println(tables);
    }

    @Test
    public void testCheckGtidMode() {
        String gtidMode = MySqlUtils.checkGtidMode(endpointCi3306);
        Assert.assertNotNull(gtidMode);
        Assert.assertTrue(Arrays.asList("ON", "OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE").contains(gtidMode));
    }

    @Test
    public void testCheckBinlogTransactionDependency() {
        String binlogTransactionDependency = MySqlUtils.checkBinlogTransactionDependency(endpointCi3306);
        Assert.assertNotNull(binlogTransactionDependency);
        System.out.println(binlogTransactionDependency);
        Assert.assertTrue(Arrays.asList("COMMIT_ORDER", "WRITESET_SESSION", "WRITESET").contains(binlogTransactionDependency));
    }

    @Test
    public void testCheckApprovedTruncateTableList() {
        List<String> approvedTruncateTableList = MySqlUtils.checkApprovedTruncateTableList(endpointCi3306,true);
        Assert.assertNotNull(approvedTruncateTableList);
        System.out.println(approvedTruncateTableList);
        Assert.assertEquals(1, approvedTruncateTableList.size());
        Assert.assertTrue(approvedTruncateTableList.contains("`testdb`.`testtable`"));
    }

    @Test
    public void testGetExecutedGtid() {
        String executedGtid = MySqlUtils.getExecutedGtid(endpointCi3306);
        System.out.println("executedGtid: " + executedGtid);
    }

    @Test
    public void testCheckBinlogMode() {
        String binlogMode = MySqlUtils.checkBinlogMode(endpointCi3306);
        System.out.println("binlogmode: " + binlogMode);
    }
    @Test
    public void testCheckBinlogFormat() {
        String binlogFormat = MySqlUtils.checkBinlogFormat(endpointCi3306);
        System.out.println("binlogFormat: " + binlogFormat);
    }
    @Test
    public void testCheckBinlogVersion() {
        String binlogVersion1 = MySqlUtils.checkBinlogVersion(endpointCi3306);
        System.out.println("binlogVersion1: " + binlogVersion1);
    }
    @Test
    public void testCheckAutoIncrementStep() {
        Integer autoIncrementStep = MySqlUtils.checkAutoIncrementStep(endpointCi3306);
        System.out.println("autoIncrementStep: " + autoIncrementStep);
    }
    @Test
    public void testCheckAutoIncrementOffset() {
        Integer autoIncrementOffset = MySqlUtils.checkAutoIncrementOffset(endpointCi3306);
        System.out.println("autoIncrementOffset: " + autoIncrementOffset);
    }
    @Test
    public void testCheckDrcTables() {
        Integer drcTables = MySqlUtils.checkDrcTables(endpointCi3306);
        System.out.println("drcTables: " + drcTables);
    }
    @Test
    public void testAccount() {
        MySqlUtils.testAccount(endpointCi3306);
    }

    @Test
    public void testCheckTablesWithFilter() {
        doWrite(CREATE_EMPTYDB_TABLE);
        List<TableCheckVo> checkVos = MySqlUtils.checkTablesWithFilter(endpointCi3306, ".*");
        for (TableCheckVo vo: checkVos) {
            System.out.println(vo);
        }
    }

    @After
    public void tearDown() {
        DataSourceManager.getInstance().clearDataSource(endpointCi3306);
    }

    private static void initDb(Endpoint endpoint) {
        dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_CONFIGDB);
            stmt.execute(USE_CONFIGDB);
            stmt.execute(CREATE_TABLE_APPROVED_TRUNCATELIST);
            stmt.execute(TRUNCATE_APPROVED_TRUNCATELIST);
            stmt.executeUpdate(INSERT_APPROVED_TRUNCATELIST);
            stmt.execute(CREATE_DB);
            stmt.execute(USE_DB);
            stmt.execute(CREATE_TABLE1);
            stmt.execute(CREATE_TABLE2);
            stmt.execute(CREATE_TABLE3);
            stmt.execute(CREATE_DEL_TABLE);
            stmt.execute(CREATE_DEL_DB);
            stmt.execute(USE_DEL_DB);
            stmt.execute(CREATE_TABLE1);
            stmt.execute(CREATE_EMPTY_DB);
        } catch (Exception e) {
            logger.error("init db error: ", e);
        }
    }

    private static void doWrite(String sql) {
        dataSource = DataSourceManager.getInstance().getDataSource(endpointCi3306);
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception e) {
            logger.error("init db error:", e);
        }
    }
}
