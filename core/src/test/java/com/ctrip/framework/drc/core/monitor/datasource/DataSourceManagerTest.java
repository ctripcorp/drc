package com.ctrip.framework.drc.core.monitor.datasource;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/12/26 上午12:03.
 */
public class DataSourceManagerTest {

    private DataSourceManager dataSourceManager = DataSourceManager.getInstance();

    @Test
    public void getDataSource() {

        Endpoint srcEndpoint = new DefaultEndPoint("127.0.0.1", 3306, "root", "root");
        DataSource dataSource1 = dataSourceManager.getDataSource(srcEndpoint);
        DataSource dataSource2 = dataSourceManager.getDataSource(srcEndpoint);
        Assert.assertEquals(dataSource1, dataSource2);

        dataSourceManager.clearDataSource(srcEndpoint);
        dataSource2 = dataSourceManager.getDataSource(srcEndpoint);
        Assert.assertNotEquals(dataSource1, dataSource2);

    }

    @Test
    public void testBigTransaction() throws SQLException {
        String updateSQL = "update `drc1`.`insert1` set `one`=\"one1\" where `one`=\"one\" and `id`=%d;";
        Endpoint srcEndpoint = new DefaultEndPoint("127.0.0.1", 3306, "root", "root");
        DataSource dataSource1 = dataSourceManager.getDataSource(srcEndpoint);
        List<Long> ids = Lists.newArrayList();
        long conflictId = 0;
        try (Connection connection = dataSource1.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select `id` from `drc1`.`insert1`");
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    ids.add(id);
                    if (ids.size() == 500) {
                        conflictId = id;
                    }
                }
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("begin;");
                for (Long id : ids) {
                    String realSQL = String.format(updateSQL, id);
                    statement.execute(realSQL);
                    if (conflictId == id.longValue()) {
                        statement.execute(realSQL);
                    }
                }
                statement.execute("commit;");
            }
        }

    }
}