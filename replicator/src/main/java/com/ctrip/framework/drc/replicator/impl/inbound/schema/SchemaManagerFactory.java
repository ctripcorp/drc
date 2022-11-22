package com.ctrip.framework.drc.replicator.impl.inbound.schema;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author limingdong
 * @create 2020/10/12
 */
public class SchemaManagerFactory {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManagerFactory.class);

    // replicator master -> MySQLSchemaManager; replicator slave -> BackupMySQLSchemaManager;
    // when slave switch master, no need to clone schema, so using original BackupMySQLSchemaManager is OK
    private static Map<String, MySQLSchemaManager> schemaManagerMap = Maps.newConcurrentMap();

    public static synchronized MySQLSchemaManager getOrCreateMySQLSchemaManager(ReplicatorConfig replicatorConfig) {
        String clusterName = replicatorConfig.getRegistryKey();
        MySQLSchemaManager mySQLSchemaManager = schemaManagerMap.get(clusterName);
        if (mySQLSchemaManager != null) {
            logger.info("return previous MySQLSchemaManager for {}", clusterName);
            return mySQLSchemaManager;
        }
        MySQLSlaveConfig mySQLSlaveConfig = replicatorConfig.getMySQLSlaveConfig();
        boolean isMaster = mySQLSlaveConfig.isMaster();

        if (isMaster) {
            if ("true".equalsIgnoreCase(System.getProperty(SystemConfig.REPLICATOR_LOCAL_SCHEMA_MANAGER))) {
                mySQLSchemaManager = new LocalSchemaManager(mySQLSlaveConfig.getEndpoint(), replicatorConfig.getApplierPort(), clusterName, replicatorConfig.getBaseEndpointEntity());
            } else {
                mySQLSchemaManager = new MySQLSchemaManager(mySQLSlaveConfig.getEndpoint(), replicatorConfig.getApplierPort(), clusterName, replicatorConfig.getBaseEndpointEntity());
            }
        } else {
            mySQLSchemaManager = new BackupMySQLSchemaManager(mySQLSlaveConfig.getEndpoint(), replicatorConfig.getApplierPort(), clusterName, replicatorConfig.getBaseEndpointEntity());
        }

        schemaManagerMap.put(clusterName, mySQLSchemaManager);

        return mySQLSchemaManager;
    }

    public static synchronized MySQLSchemaManager remove(String clusterName) {
        return schemaManagerMap.remove(clusterName);
    }

    public static synchronized void clear() {
        for (Map.Entry<String, MySQLSchemaManager> entry : schemaManagerMap.entrySet()) {
            try {
                if (!DynamicConfig.getInstance().getIndependentEmbeddedMySQLSwitch(entry.getKey())) {
                    LifecycleHelper.stopIfPossible(entry.getValue());
                    LifecycleHelper.disposeIfPossible(entry.getValue());
                }
            } catch (Exception e) {
            }
        }
        schemaManagerMap.clear();
    }
}
