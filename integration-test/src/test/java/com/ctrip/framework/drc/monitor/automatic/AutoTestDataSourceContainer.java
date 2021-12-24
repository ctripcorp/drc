package com.ctrip.framework.drc.monitor.automatic;

import com.ctrip.framework.drc.core.server.config.SystemConfig;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.util.Map;

/**
 * @Author Slight
 * Dec 16, 2019
 */
public interface AutoTestDataSourceContainer extends Map<Integer, DataSource> {

    default void close(int... ports) {
        for (int port : ports) {
            close(port);
        }
    }

    default void close(int port) {
        get(port).close();
    }

    default void init(int... ports) {
        for (int port : ports) {
            init(port);
        }
    }

    default void init(int port) {
        PoolProperties properties = new PoolProperties();
        properties.setUrl("jdbc:mysql://127.0.0.1:" + port + "?allowMultiQueries=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8");
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("root");
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=5000");
        properties.setMaxActive(626);
        DataSource dataSource = new DataSource();
        dataSource.setPoolProperties(properties);
        put(port, dataSource);
    }
}
