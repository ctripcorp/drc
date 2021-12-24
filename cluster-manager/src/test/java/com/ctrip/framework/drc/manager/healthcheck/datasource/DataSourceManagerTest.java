package com.ctrip.framework.drc.manager.healthcheck.datasource;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Test;

import static com.ctrip.framework.drc.manager.AllTests.*;

/**
 * Created by mingdongli
 * 2019/12/26 上午12:03.
 */
public class DataSourceManagerTest {

    private DataSourceManager dataSourceManager = DataSourceManager.getInstance();

    @Test
    public void getDataSource() {

        Endpoint srcEndpoint = new DefaultEndPoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD);
        DataSource dataSource1 = dataSourceManager.getDataSource(srcEndpoint);
        DataSource dataSource2 = dataSourceManager.getDataSource(srcEndpoint);
        Assert.assertEquals(dataSource1, dataSource2);

        dataSourceManager.clearDataSource(srcEndpoint);
        dataSource2 = dataSourceManager.getDataSource(srcEndpoint);
        Assert.assertNotEquals(dataSource1, dataSource2);

    }
}