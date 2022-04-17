package com.ctrip.framework.drc.performance.impl;

import com.ctrip.framework.drc.core.driver.binlog.manager.AbstractSchemaManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.TableInfo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.Map;

/**
 * Created by jixinwang on 2021/8/18
 */
public class ApplierTestSchemaManager extends AbstractSchemaManager {

    public ApplierTestSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName);
        this.baseEndpointEntity = baseEndpointEntity;
        logger.info("[Schema] port is {}", port);
    }

    @Override
    protected void doInitialize() {
        DefaultEndPoint endPoint = (DefaultEndPoint) endpoint;
        inMemoryEndpoint = new DefaultEndPoint(endPoint.getIp(), endPoint.getPort(), endPoint.getUser(), endPoint.getPassword());
        inMemoryDataSource = DataSourceManager.getInstance().getDataSource(inMemoryEndpoint);
    }


    @Override
    public TableInfo find(String schema, String table) {
        return null;
    }

    @Override
    public Map<String, Map<String, String>> snapshot() {
        return null;
    }

    @Override
    public void clone(Endpoint endpoint) {

    }

    @Override
    public TableInfo queryTableInfoByIS(DataSource dataSource, String schema, String table) {
        return null;
    }

    @Override
    public void persistColumnInfo(TableInfo tableInfo, boolean writeDirect) {

    }

    @Override
    public void persistDdl(String dbName, String tableName, String queryString) {

    }

    @Override
    public void update(Object args, Observable observable) {

    }
}
