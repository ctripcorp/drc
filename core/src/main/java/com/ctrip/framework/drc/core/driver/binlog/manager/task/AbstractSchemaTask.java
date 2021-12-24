package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public abstract class AbstractSchemaTask implements NamedCallable<Boolean> {

    protected Endpoint inMemoryEndpoint;

    protected DataSource inMemoryDataSource;

    public AbstractSchemaTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        this.inMemoryEndpoint = inMemoryEndpoint;
        this.inMemoryDataSource = inMemoryDataSource;
    }

    @Override
    public void afterException(Throwable t) {
        NamedCallable.super.afterException(t);
        DataSourceManager.getInstance().clearDataSource(inMemoryEndpoint);
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
        NamedCallable.DDL_LOGGER.warn("[Clear] datasource and recreate for {}", inMemoryEndpoint);
    }
}
