package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldProcess;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldStarter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredRuntimeConfigBuilder;
import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import ctrip.framework.drc.mysql.utils.ClassUtils;
import de.flapdoodle.embed.process.config.IRuntimeConfig;

import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static com.wix.mysql.distribution.Version.v8_latest;
import static ctrip.framework.drc.mysql.EmbeddedDb.mysqlInstanceDir;
import static ctrip.framework.drc.mysql.EmbeddedDb.src_path;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public class DbRestoreTask implements NamedCallable<MySQLInstance> {

    private int port;

    private String registryKey;

    public DbRestoreTask(int port, String registryKey) {
        this.port = port;
        this.registryKey = registryKey;
    }

    @Override
    public MySQLInstance call() throws Exception {
        // todo by yongnian: 2023/9/21 testing
        MysqldConfig mysqldConfig = MysqldConfig.aMysqldConfig(v8_latest)
                .withPort(port)
                .withCharset(Charset.UTF8)
                .withTempDir(mysqlInstanceDir(registryKey, port)).build();


        String path = ClassUtils.getDefaultClassLoader().getResource(src_path).getPath();
        DownloadConfig downloadConfig = aDownloadConfig().withCacheDir(path).build();

        IRuntimeConfig runtimeConfig = new RestoredRuntimeConfigBuilder().defaults(mysqldConfig, downloadConfig).build();
        RestoredMysqldStarter mysqldStarter = new RestoredMysqldStarter(runtimeConfig);
        RestoredMysqldProcess mysqldProcess = mysqldStarter.prepare(mysqldConfig).start();
        return new MySQLInstanceExisting(mysqldProcess);
    }
}
