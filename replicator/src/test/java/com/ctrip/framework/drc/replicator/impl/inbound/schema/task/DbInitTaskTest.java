package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.wix.mysql.EmbeddedMysql;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbInitTaskTest {

    private int PORT = 8989;

    private EmbeddedMysql embeddedMysql;

    private static final String CREATE_DB = "CREATE DATABASE drc1";

    private static final String CREATE_TABLE = "CREATE TABLE `drc1`.`members` (\n" +
            "    firstname VARCHAR(25) NOT NULL,\n" +
            "    lastname VARCHAR(25) NOT NULL,\n" +
            "    username VARCHAR(16) NOT NULL,\n" +
            "    email VARCHAR(35),\n" +
            "    joined DATE NOT NULL\n" +
            ")PARTITION BY RANGE( YEAR(joined) ) (\n" +
            "    PARTITION p0 VALUES LESS THAN (1960),\n" +
            "    PARTITION p1 VALUES LESS THAN (1970),\n" +
            "    PARTITION p2 VALUES LESS THAN (1980),\n" +
            "    PARTITION p3 VALUES LESS THAN (1990),\n" +
            "    PARTITION p4 VALUES LESS THAN MAXVALUE\n" +
            ");";

    private static final String SHOW_DB = "show create table drc1.members";

    @Test
    public void testDbInitTask() throws SQLException {
        embeddedMysql = new RetryTask<>(new DbInitTask(PORT, "ut_cluster")).call();
        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", PORT, "root", "");
        DataSource dataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_DB);
                statement.execute(CREATE_TABLE);
                try (ResultSet resultSet = statement.executeQuery(SHOW_DB)) {
                    Assert.assertNotNull(resultSet);
                    Assert.assertTrue(resultSet.next());
                    String res = resultSet.getString(1);
                    Assert.assertEquals(res, "members");
                    res = resultSet.getString(2);
                    Assert.assertTrue(res.contains("CREATE TABLE"));
                }

            }
        } catch (Exception e) {
            // nothing to be done
        }

    }

    @After
    public void tearDown() {
        try {
            embeddedMysql.stop();
        } catch (Exception e) {
        }
    }
}
