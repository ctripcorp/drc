package com.ctrip.framework.drc.replicator.impl.inbound.schema.parse;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2025/7/21 16:35
 */
public class DdlParserTest {

    @Test
    public void testCreate() {
        String queryString = "CREATE TABLE test_table ( `ID` int(11) )";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXISTS test_db.test_table ( `ID` int(11) )";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "CREATE TABLE IF NOT EXISTS `test_table` ( `ID` int(11) )";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "CREATE TABLE  `test_db`.`test_table` (  `ID` int(10) unsigned NOT NULL )";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "CREATE table `slave_backup`.`bin_log_backup` like bin_log";
        result = DdlParser.parse(queryString, "bak").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("slave_backup", result.getSchemaName());
        Assert.assertEquals("bin_log_backup", result.getTableName());

        queryString = "CREATE DEFINER=sco*erce@% PROCEDURE SC_CPN_CODES_SAVE_ACTION(IN cosmosPassportId CHAR(32), IN orderId CHAR(32), IN codeIds TEXT) BEGIN SET @orderId = orderId; SET @timeNow = NOW(); START TRANSACTION; DELETE FROMsc_ord_couponWHEREORDER_ID= @orderId; SET @i=1; SET @numbers = FN_GET_ELEMENTS_COUNT(codeIds, '|'); WHILE @i <= @numbers DO SET @codeId = FN_FIND_ELEMENT_BYINDEX(codeIds, '|', @i); SET @orderCodeId = UUID32(); INSERT INTOsc_ord_coupon(ID,CREATE_BY,CREATE_TIME,UPDATE_BY,UPDATE_TIME,ORDER_ID,CODE_ID`) VALUES(@orderCodeId, cosmosPassportId, @timeNow, cosmosPassportId, @timeNow, @orderId, @codeId); SET @i = @i + 1; END WHILE; COMMIT; END";
        result = DdlParser.parse(queryString, "bak").get(0);
        Assert.assertEquals(QueryType.QUERY.QUERY, result.getType());

        queryString = "CREATE TABLE performance_schema.cond_instances(`ID` int(10) unsigned NOT NULL ) ";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("performance_schema", result.getSchemaName());
        Assert.assertEquals("cond_instances", result.getTableName());
        Assert.assertNull(result.getTableCharset());


        queryString = "CREATE TABLE `insert7` (`id` int(11) NOT NULL AUTO_INCREMENT,`one` varchar(30) DEFAULT 'one',`two` varchar(1000) DEFAULT 'two',`three` char(30) DEFAULT NULL,`four` char(255) DEFAULT NULL,`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',PRIMARY KEY (`id`)) ENGINE=InnoDB";
        String newQueryString = DdlParser.appendTableCharset(queryString, null);
        Assert.assertEquals(queryString, newQueryString);
        newQueryString = DdlParser.appendTableCharset(queryString, "utf8");
        Assert.assertEquals(queryString + DdlParser.getAppendCharset("utf8"), newQueryString);

        queryString = "CREATE TABLE `insert7` (`id` int(11) NOT NULL AUTO_INCREMENT,`one` varchar(30) DEFAULT 'one',`two` varchar(1000) DEFAULT 'two',`three` char(30) DEFAULT NULL,`four` char(255) DEFAULT NULL,`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',PRIMARY KEY (`id`)) ENGINE=InnoDB;";
        newQueryString = DdlParser.appendTableCharset(queryString, null);
        Assert.assertEquals(queryString, newQueryString);
        newQueryString = DdlParser.appendTableCharset(queryString, "utf8");
        String expectQueryString = "CREATE TABLE `insert7` (`id` int(11) NOT NULL AUTO_INCREMENT,`one` varchar(30) DEFAULT 'one',`two` varchar(1000) DEFAULT 'two',`three` char(30) DEFAULT NULL,`four` char(255) DEFAULT NULL,`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',PRIMARY KEY (`id`)) ENGINE=InnoDB CHARSET=utf8;";
        Assert.assertEquals(expectQueryString, newQueryString);
    }

    @Test
    public void testDrop() {
        String queryString = "DROP TABLE test_table";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "DROP TABLE IF EXISTS test.test_table;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "DROP TABLE IF EXISTS  \"test\".`test_table`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "DROP TABLE IF EXISTS  test_db.test_table , test_db_test";
        result = DdlParser.parse(queryString, "test").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());
        result = DdlParser.parse(queryString, "test").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test", result.getSchemaName());
        Assert.assertEquals("test_db_test", result.getTableName());

        queryString = "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `temp_trip_uids`.`temp_trip_id`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("temp_trip_uids", result.getSchemaName());
        Assert.assertEquals("temp_trip_id", result.getTableName());
    }

    @Test
    public void testAlert() {
        String queryString = "alter table test_table drop index emp_name";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());

        queryString = "alter table test_db.test_table drop index emp_name";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());

        queryString = "alter table  test_db.`test_table` drop index emp_name;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());

        queryString = "alter table test_db.test_table drop index emp_name , add index emp_name(id)";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals(QueryType.DINDEX, result.getType());
        Assert.assertEquals("test_db", result.getSchemaName());

        result = DdlParser.parse(queryString, "test_db").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals(QueryType.CINDEX, result.getType());
        Assert.assertEquals("test_db", result.getSchemaName());

        // test schemaName
        queryString = "alter table  test_db.`test_table` drop index emp_name;";
        result = DdlParser.parse(queryString, "test_db3").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());

        queryString = "alter table  `test_table` drop index emp_name;";
        result = DdlParser.parse(queryString, "test_db3").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db3", result.getSchemaName());

        queryString = "alter table  test_db.`test_table` drop index emp_name;";
        result = DdlParser.parse(queryString, null).get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());
    }

    @Test
    public void testTruncate() {
        String queryString = "truncate table test_table";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "truncate table test_db.test_table";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "truncate   test_db.`test_table` ";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "truncate   test_db.test_table , test_db_test ";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        result = DdlParser.parse(queryString, "test_db").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db_test", result.getTableName());
    }

    @Test
    public void testRename() {
        String queryString = "rename table test_table to test_table2";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getOriSchemaName());
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());

        queryString = "rename table test_db.test_table to test_db2.test_table2";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getOriSchemaName());
        Assert.assertEquals("test_db2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());

        queryString = "rename  table  `test_db`.`test_table` to `test_db2`.`test_table2`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getOriSchemaName());
        Assert.assertEquals("test_db2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());

        queryString = "rename  table  `test_db`.`test_table` to `test_db2`.`test_table2` , `test_db1`.`test_table1` to `test_db3`.`test_table3`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getOriSchemaName());
        Assert.assertEquals("test_db2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());
        result = DdlParser.parse(queryString, "test_db").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db1", result.getOriSchemaName());
        Assert.assertEquals("test_db3", result.getSchemaName());
        Assert.assertEquals("test_table1", result.getOriTableName());
        Assert.assertEquals("test_table3", result.getTableName());

        // 正则匹配test case

        queryString = "rename table totl_mark to totl_mark2";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getOriSchemaName());
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("totl_mark", result.getOriTableName());
        Assert.assertEquals("totl_mark2", result.getTableName());

        queryString = "rename table totl.test_table to totl2.test_table2";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());

        queryString = "rename  table  `totl`.`test_table` to `totl2`.`test_table2`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());

        queryString = "rename  table  `totl`.`test_table` to `totl2`.`test_table2` , `totl1`.`test_table1` to `totl3`.`test_table3`;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl", result.getOriSchemaName());
        Assert.assertEquals("totl2", result.getSchemaName());
        Assert.assertEquals("test_table", result.getOriTableName());
        Assert.assertEquals("test_table2", result.getTableName());
        result = DdlParser.parse(queryString, "test_db").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("totl1", result.getOriSchemaName());
        Assert.assertEquals("totl3", result.getSchemaName());
        Assert.assertEquals("test_table1", result.getOriTableName());
        Assert.assertEquals("test_table3", result.getTableName());

        queryString = "rename /* gh-ost */ table `ghostdb`.`test1g` to `ghostdb`.`_test1g_del`, `ghostdb`.`_test1g_gho` to `ghostdb`.`test1g`;";
        result = DdlParser.parse(queryString, "ghostdb").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("ghostdb", result.getOriSchemaName());
        Assert.assertEquals("ghostdb", result.getSchemaName());
        Assert.assertEquals("test1g", result.getOriTableName());
        Assert.assertEquals("_test1g_del", result.getTableName());
        result = DdlParser.parse(queryString, "ghostdb").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("ghostdb", result.getOriSchemaName());
        Assert.assertEquals("ghostdb", result.getSchemaName());
        Assert.assertEquals("_test1g_gho", result.getOriTableName());
        Assert.assertEquals("test1g", result.getTableName());

    }

    @Test
    public void testIndex() {
        String queryString = "CREATE UNIQUE INDEX index_1 ON test_table(id,x)";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "create index idx_qca_cid_mcid on q_contract_account (contract_id,main_contract_id)";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("q_contract_account", result.getTableName());

        queryString = "DROP INDEX index_str ON test_table";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_db", result.getSchemaName());
        Assert.assertEquals("test_table", result.getTableName());
    }

    @Test
    public void testDb() {
        String queryString = "create database db1";
        DdlResult result = DdlParser.parse(queryString, "db0").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("db1", result.getSchemaName());

        queryString = "drop database db1";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("db1", result.getSchemaName());
    }

    /**
     * use drc4;
     * CREATE TABLE drc1.test_identity (
     * `id` int(11) NOT NULL AUTO_INCREMENT,
     * `one` varchar(30) DEFAULT 'one',
     * `two` varchar(1000) DEFAULT 'two',
     * `three` char(30) DEFAULT NULL,
     * `four` char(255) DEFAULT NULL,
     * `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'time',
     * PRIMARY KEY (`id`)
     * );
     */
    @Test
    public void testWrongSchema() {
        String queryString = "CREATE TABLE drc1.test_identity (" +
                "`id` int(11) NOT NULL AUTO_INCREMENT," +
                "`one` varchar(30) DEFAULT 'one'," +
                "`two` varchar(1000) DEFAULT 'two'," +
                "`three` char(30) DEFAULT NULL," +
                "`four` char(255) DEFAULT NULL," +
                "`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT 'time'," +
                "PRIMARY KEY (`id`)" +
                ");";
        DdlResult result = DdlParser.parse(queryString, "drc4").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("drc1", result.getSchemaName());
    }

    @Test
    public void testPartitionAlter() {
        String queryString = "ALTER TABLE trb4 truncate PARTITION p1;";
        DdlResult result = DdlParser.parse(queryString, "db0").get(0);
        Assert.assertEquals(result.getType(), QueryType.ALTER);
    }

    @Test
    public void testCreateDbEscaped() {
        String queryString = " CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */";
        DdlResult result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertNull(result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());

        queryString = " CREATE DATABASE test_db /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertNull(result.getTableName());
        Assert.assertEquals("test_db", result.getSchemaName());
    }

    @Test
    public void test() {
        String queryString = "CREATE TABLE `sales_order_flight_detail_8` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',\n" +
                "  `orderid` bigint NOT NULL DEFAULT '0' COMMENT '订单号',\n" +
                "  `sales_order_id` bigint NOT NULL DEFAULT '0' COMMENT '销货单号ID',\n" +
                "  `segment` int NOT NULL DEFAULT '0' COMMENT '行程段ID',\n" +
                "  `sequence` int NOT NULL DEFAULT '0' COMMENT '航段号',\n" +
                "  `flight_no` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '航班号',\n" +
                "  `depart_airport_code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '出发机场code',\n" +
                "  `arrive_airport_code` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '到达机场code',\n" +
                "  `depart_time` datetime DEFAULT NULL COMMENT '出发时间',\n" +
                "  `arrive_time` datetime DEFAULT NULL COMMENT '到达时间',\n" +
                "  `relation_segment` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '关联的行程段',\n" +
                "  `depart_terminal` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '出发航站楼',\n" +
                "  `arrival_terminal` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '到达航站楼',\n" +
                "  `seat_class` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '仓等',\n" +
                "  `sub_class` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '子仓位',\n" +
                "  `flight_style` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '机型信息',\n" +
                "  `stops` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '经停信息',\n" +
                "  `stop_name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '经停名称',\n" +
                "  `datachange_createtime` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                "  `stop_info` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '经停信息（Json格式）',\n" +
                "  `d_city_id` bigint DEFAULT '0' COMMENT '出发城市ID',\n" +
                "  `a_city_id` bigint DEFAULT '0' COMMENT '到达城市ID',\n" +
                "  `airline_code` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '原始航司',\n" +
                "  `sub_airline_code` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '子航司',\n" +
                "  `userdata_location` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '用于存储数据写入的初始区域，可能的值样例:HK,US,KR,JP,TW,SG,TH,MY,AU,FR等;国内的默认为空值; 如有出海需求，海外数据建议添加该字段',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `uniq_salesorderid_segment_sequence` (`sales_order_id`,`segment`,`sequence`),\n" +
                "  KEY `ix_salesorderid` (`sales_order_id`) /*!80000 INVISIBLE */,\n" +
                "  KEY `ix_DataChange_LastTime` (`datachange_lasttime`),\n" +
                "  KEY `ix_segment` (`segment`),\n" +
                "  KEY `ix_sequence` (`sequence`),\n" +
                "  KEY `ix_orderid` (`orderid`),\n" +
                "  KEY `idx_depart_time_flight_no` (`depart_time`,`flight_no`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=25746 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='机票航班信息表'\n";

        DdlResult ddlResult = DdlParser.parse(queryString, "tourorderresourceshard05db").get(0);
        Assert.assertEquals(ddlResult.getSchemaName(), "tourorderresourceshard05db");
        Assert.assertEquals(ddlResult.getTableName(), "sales_order_flight_detail_8");
        System.out.println(ddlResult);

    }

    @Test
    public void testParseDdl() {
        String queryString = "CREATE TABLE `test_invisible` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
                "  `name` varchar(62) NOT NULL DEFAULT'name' COMMENT 'name',\n" +
                "  `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',\n" +
                "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `idx_name` (`name`) /*!80000 INVISIBLE */,\n" +
                "  KEY `ix_datachange_lasttime` (`datachange_lasttime`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb3 COMMENT='test_invisible'";

        parseDdl(queryString, "testdb");
    }

    private void parseDdl(String queryString, String db) {
        DdlResult ddlResult = DdlParser.parse(queryString, db).get(0);
        Assert.assertEquals(ddlResult.getSchemaName(), "testdb");
        Assert.assertEquals(ddlResult.getTableName(), "test_invisible");
    }
}
