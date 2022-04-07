package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaExtractor;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/7
 */
public class DatabaseCreateTaskTest extends AbstractSchemaTest {

    private List<String> dbs = Lists.newArrayList();

    private static final String DB_NAME = "drc_ut_db";

    private List<String> tables = Lists.newArrayList();

    private static final String TABLE_CREATE = "CREATE TABLE `drc_ut_db`.`t`(`id` int(11) NOT NULL AUTO_INCREMENT,`one` varchar(30) DEFAULT \"one\",`two` varchar(1000) DEFAULT \"two\",`three` char(30),`four` char(255),`datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;";

    private static final String TABLE_NAME = "t";

    @Before
    public void setUp() throws Exception {
        dbs.add(DB_NAME);
        tables.add(TABLE_CREATE);
        super.setUp();
    }

    @Test
    public void testDbCreateAndDrop() throws Exception {
        Connection connection = null;
        try {
            connection = inMemoryDataSource.getConnection();

            List<String> databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertFalse(databases.contains(DB_NAME));

            // create db
            boolean res = abstractSchemaTask.call();
            Assert.assertTrue(res);

            databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertTrue(databases.contains(DB_NAME));

            // create table
            List<String> tableNames = Lists.newArrayList();
            try (Statement statement = connection.createStatement()) {
                statement.execute("use drc_ut_db");
                try (ResultSet resultSet = statement.executeQuery("SHOW TABLES;")) {
                    tableNames = SchemaExtractor.extractValues(resultSet, null);
                }
            }
            Assert.assertFalse(tableNames.contains(TABLE_NAME));

            AbstractSchemaTask task = new TableCreateTask(tables, inMemoryEndpoint, inMemoryDataSource);
            res = task.call();
            Assert.assertTrue(res);

            try (Statement statement = connection.createStatement()) {
                statement.execute("use drc_ut_db;");
                try (ResultSet resultSet = statement.executeQuery("SHOW TABLES;")) {
                    tableNames = SchemaExtractor.extractValues(resultSet, null);
                }
            }
            Assert.assertTrue(tableNames.contains(TABLE_NAME));

            // drop db
            task = new DatabaseDropTask(dbs, inMemoryEndpoint, inMemoryDataSource);
            res = task.call();
            Assert.assertTrue(res);

            databases = query(connection, MySQLConstants.SHOW_DATABASES_QUERY);
            Assert.assertFalse(databases.contains(DB_NAME));
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    protected AbstractSchemaTask getAbstractSchemaTask() {
        return new DatabaseCreateTask(dbs, inMemoryEndpoint, inMemoryDataSource);
    }
}