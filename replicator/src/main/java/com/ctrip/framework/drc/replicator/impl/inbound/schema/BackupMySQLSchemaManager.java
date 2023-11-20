package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.monitor.entity.BaseEndpointEntity;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2020/5/11
 */
public class BackupMySQLSchemaManager extends MySQLSchemaManager {

    /**
     * backup replicator only recover once
     */
    private boolean recovered = false;

    public BackupMySQLSchemaManager(Endpoint endpoint, int applierPort, String clusterName, BaseEndpointEntity baseEndpointEntity) {
        super(endpoint, applierPort, clusterName, baseEndpointEntity);
    }

    @Override
    public boolean shouldRecover(boolean fromLatestLocalBinlog) {
        if (fromLatestLocalBinlog) {
            // slave should recover from master drc ddl events, not local binlog
            DDL_LOGGER.info("[Recovery Skip][Backup] should not recover from last binlog for {} (Version: {})", registryKey, embeddedDbVersion);
            return false;
        }

        return super.shouldRecover(fromLatestLocalBinlog);
    }

    @Override
    public boolean clone(Endpoint endpoint) {
        // nothing to be done
        return true;
    }
}
