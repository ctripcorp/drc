package com.ctrip.framework.drc.replicator.impl.inbound;

import com.ctrip.framework.drc.core.driver.AbstractMySQLSlave;
import com.ctrip.framework.drc.core.driver.MySQLConnection;
import com.ctrip.framework.drc.core.driver.MySQLConnector;
import com.ctrip.framework.drc.core.driver.MySQLSlave;
import com.ctrip.framework.drc.core.driver.binlog.LogEventHandler;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.SchemaManager;
import com.ctrip.framework.drc.core.driver.config.MySQLSlaveConfig;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.BackupReplicatorConnection;
import com.ctrip.framework.drc.replicator.impl.inbound.driver.ReplicatorConnection;

/**
 * Created by mingdongli
 * 2019/9/21 上午11:06.
 */
public class ReplicatorSlaveServer extends AbstractMySQLSlave implements MySQLSlave {

    protected MySQLConnector connector;

    protected GtidManager gtidManager;

    protected SchemaManager schemaManager;

    public ReplicatorSlaveServer(MySQLSlaveConfig mySQLSlaveConfig, MySQLConnector connector, SchemaManager schemaManager) {
        super(mySQLSlaveConfig);
        this.connector = connector;
        this.schemaManager = schemaManager;
    }

    @Override
    protected MySQLConnection newConnection(MySQLSlaveConfig mySQLSlaveConfig, LogEventHandler eventHandler) {
        if (mySQLSlaveConfig.isMaster()) {
            return new ReplicatorConnection(mySQLSlaveConfig, eventHandler, connector, gtidManager, schemaManager);
        } else {
            return new BackupReplicatorConnection(mySQLSlaveConfig, eventHandler, connector, gtidManager, schemaManager);

        }
    }

    public void setGtidManager(GtidManager gtidManager) {
        this.gtidManager = gtidManager;
    }
}
