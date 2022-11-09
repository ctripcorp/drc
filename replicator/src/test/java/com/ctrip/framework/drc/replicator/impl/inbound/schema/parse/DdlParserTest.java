package com.ctrip.framework.drc.replicator.impl.inbound.schema.parse;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2020/2/24
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

        queryString = "alter table test_db.test_table drop index emp_name";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "alter table  test_db.`test_table` drop index emp_name;";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());

        queryString = "alter table test_db.test_table drop index emp_name , add index emp_name(id)";
        result = DdlParser.parse(queryString, "test_db").get(0);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals(QueryType.DINDEX, result.getType());

        result = DdlParser.parse(queryString, "test_db").get(1);
        Assert.assertNotNull(result);
        Assert.assertEquals("test_table", result.getTableName());
        Assert.assertEquals(QueryType.CINDEX, result.getType());
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
     *  use drc4;
     *  CREATE TABLE drc1.test_identity (
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
}
