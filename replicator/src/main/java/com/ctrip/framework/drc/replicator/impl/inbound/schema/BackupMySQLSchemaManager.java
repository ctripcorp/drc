package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupMySQLSchemaManager extends MySQLSchemaManager {

    public BackupMySQLSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName, baseEndpointEntity);
    }

    @Override
    public void clone(Endpoint endpoint) {
        // nothing to be done
    }
}
