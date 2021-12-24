package com.ctrip.framework.drc.applier.resource.mysql;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author Slight
 * May 14, 2020
 */
public interface DataSource {

    static DataSource wrap(javax.sql.DataSource dataSource) {
        return new DataSourceResource().wrap(dataSource);
    }

    Connection getConnection() throws SQLException;
}
