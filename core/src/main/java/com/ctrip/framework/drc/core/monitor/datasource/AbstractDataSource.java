package com.ctrip.framework.drc.core.monitor.datasource;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * @Author limingdong
 * @create 2020/12/10
 */
public abstract class AbstractDataSource {

    public static void setCommonProperty(PoolProperties poolProperties) {
        poolProperties.setDriverClassName("com.mysql.jdbc.Driver");

        poolProperties.setValidationQuery("SELECT 1");
        poolProperties.setValidationQueryTimeout(1);
        poolProperties.setTestWhileIdle(false);
        poolProperties.setTestOnBorrow(true);
        poolProperties.setTestOnReturn(false);
        poolProperties.setTimeBetweenEvictionRunsMillis(5 * 1000);
        poolProperties.setMinEvictableIdleTimeMillis(30 * 1000);
        poolProperties.setLogValidationErrors(false);
    }

    public static void configureMonitorProperties(Endpoint endpoint, PoolProperties poolProperties) {
        if(endpoint instanceof MySqlEndpoint) {
            MySqlEndpoint mySqlEndpoint = (MySqlEndpoint) endpoint;
            poolProperties.setMaxActive(mySqlEndpoint.isMaster() ? 6 : 100);
            poolProperties.setMaxIdle(poolProperties.getMaxActive());
        }
    }
}
