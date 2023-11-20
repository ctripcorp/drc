package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldProcess;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldStarter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredRuntimeConfigBuilder;
import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;
import ctrip.framework.drc.mysql.utils.ClassUtils;
import de.flapdoodle.embed.process.config.IRuntimeConfig;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;
import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static ctrip.framework.drc.mysql.EmbeddedDb.mysqlInstanceDir;
import static ctrip.framework.drc.mysql.EmbeddedDb.src_path;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class DbRestoreTask implements NamedCallable<MySQLInstance> {

    private int port;

    private String registryKey;

    private Version version;

    public DbRestoreTask(int port, String registryKey) {
        this.port = port;
        this.registryKey = registryKey;
        this.version = DynamicConfig.getInstance().getEmbeddedMySQLUpgradeTo8Switch(registryKey) ? Version.v8_0_32 : Version.v5_7_23;
    }

    public DbRestoreTask(int port, String registryKey, Version version) {
        this.port = port;
        this.registryKey = registryKey;
        this.version = version;
    }

    @Override
    public void afterException(Throwable t) {
        DDL_LOGGER.info("DbRestoreTask error for " + registryKey + ": " + t.getMessage(), t);
    }

    @Override
    public MySQLInstance call() throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction("embedded.db.restore." + version.getMajorVersion(), registryKey, () -> {
            MysqldConfig mysqldConfig = MysqldConfig.aMysqldConfig(version)
                    .withPort(port)
                    .withCharset(Charset.UTF8)
                    .withTempDir(mysqlInstanceDir(registryKey, port)).build();


            String path = ClassUtils.getDefaultClassLoader().getResource(src_path).getPath();
            DownloadConfig downloadConfig = aDownloadConfig().withCacheDir(path).build();

            IRuntimeConfig runtimeConfig = new RestoredRuntimeConfigBuilder().defaults(mysqldConfig, downloadConfig).build();
            RestoredMysqldStarter mysqldStarter = new RestoredMysqldStarter(runtimeConfig);
            RestoredMysqldProcess mysqldProcess = mysqldStarter.prepare(mysqldConfig).start();
            return new MySQLInstanceExisting(mysqldProcess);
        });
    }
}
