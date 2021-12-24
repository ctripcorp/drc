package com.ctrip.framework.drc.replicator.impl.inbound.schema.index;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.replicator.AllTests;
import com.ctrip.framework.drc.replicator.MockTest;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @Author limingdong
 * @create 2020/7/2
 */
public class IndexExtractorTest extends MockTest {

    private Endpoint endpoint = new DefaultEndPoint(AllTests.SRC_IP, AllTests.SRC_PORT, AllTests.MYSQL_USER, AllTests.MYSQL_PASSWORD);

    private static final String DB = "drc3";

    private static final String TABLE = "order_category";

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void extractIndex() {
        DataSource remoteDataSource = DataSourceManager.getInstance().getDataSource(endpoint);
        try (Connection connection = remoteDataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                String sql = String.format(MySQLSchemaManager.INDEX_QUERY, DB, TABLE);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    List<List<String>> ids = IndexExtractor.extractIndex(resultSet);
                    Assert.assertTrue(ids.size() == 2);
                    List<String> primary = ids.get(0);
                    Assert.assertEquals(primary.size(), 1);
                    Assert.assertEquals(primary.get(0), "ID");

                    List<String> uniq = ids.get(1);
                    Assert.assertEquals(uniq.size(), 2);
                    Assert.assertEquals(uniq.get(0), "BasicOrderID");
                    Assert.assertEquals(uniq.get(1), "Category");
                }
            }
        } catch (SQLException e) {
        }

    }
}