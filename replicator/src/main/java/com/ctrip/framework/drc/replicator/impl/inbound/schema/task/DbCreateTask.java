package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLVariablesConfiguration;
import com.wix.mysql.distribution.Version;
import ctrip.framework.drc.mysql.DbKey;
import ctrip.framework.drc.mysql.EmbeddedDb;

import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;
import static com.ctrip.framework.drc.core.server.utils.FileUtil.deleteDirectory;
import static ctrip.framework.drc.mysql.EmbeddedDb.mysqlInstanceDir;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class DbCreateTask implements NamedCallable<MySQLInstance> {

    private int port;

    private String registryKey;

    private Version version;

    public DbCreateTask(int port, String registryKey) {
        this.port = port;
        this.registryKey = registryKey;
        this.version = DynamicConfig.getInstance().getEmbeddedMySQLUpgradeTo8Switch(registryKey) ? Version.v8_0_32 : Version.v5_7_23;
    }

    public DbCreateTask(int port, String registryKey, Version version) {
        this.port = port;
        this.registryKey = registryKey;
        this.version = version;
    }

    @Override
    public MySQLInstance call() throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("embedded.db.create." + version.getMajorVersion(), registryKey, () -> {
            Map<String, Object> variables = MySQLVariablesConfiguration.getInstance().getVariables(registryKey, version);
            return new MySQLInstanceCreator(new EmbeddedDb().mysqlServer(new DbKey(registryKey, port), variables, version));
        });
    }

    @Override
    public void afterException(Throwable t) {
        DDL_LOGGER.info("DbCreateTask error for " + registryKey + ": " + t.getMessage(), t);
    }
}
