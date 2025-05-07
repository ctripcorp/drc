package com.ctrip.framework.drc.core.driver.pool;

import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolConfiguration;

import java.sql.SQLException;

/**
 * Created by jixinwang on 2020/12/4
 */
public class DrcTomcatDataSource extends DataSource {


    public DrcTomcatDataSource(PoolConfiguration poolProperties) {
        super(poolProperties);
    }

    public ConnectionPool createPool() throws SQLException {
        if (pool != null) {
            return pool;
        } else {
            return pCreatePool();
        }
    }

    private synchronized ConnectionPool pCreatePool() throws SQLException {
        if (pool != null) {
            return pool;
        } else {
            pool = new DrcConnectionPool(poolProperties);
            return pool;
        }
    }
}
