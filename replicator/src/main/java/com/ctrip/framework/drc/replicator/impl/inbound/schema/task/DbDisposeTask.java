package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.util.MySQLConstants;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldProcess;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredMysqldStarter;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.task.restore.RestoredRuntimeConfigBuilder;
import com.wix.mysql.config.Charset;
import com.wix.mysql.config.DownloadConfig;
import com.wix.mysql.config.MysqldConfig;
import com.wix.mysql.distribution.Version;
import ctrip.framework.drc.mysql.utils.ClassUtils;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.wix.mysql.config.DownloadConfig.aDownloadConfig;
import static ctrip.framework.drc.mysql.EmbeddedDb.mysqlInstanceDir;
import static ctrip.framework.drc.mysql.EmbeddedDb.src_path;

public class DbDisposeTask implements NamedCallable<DbDisposeTask.Result> {
    private static final Logger logger = LoggerFactory.getLogger(DbDisposeTask.class);

    private final int port;

    private final String registryKey;

    private final Version version;


    public enum Result {
        NO_NEED,
        SUCCESS,
        FAIL
    }

    public DbDisposeTask(int port, String registryKey, Version targetVersion) {
        this.port = port;
        this.registryKey = registryKey;
        this.version = targetVersion;
    }

    @Override
    public Result call() throws Exception {
        Version existingMysqlVersion = getExistingMysqlVersion(port);
        if (existingMysqlVersion == null) {
            logger.info("[DB DISPOSE] no process found, skip dispose for {}", registryKey);
            return Result.NO_NEED;
        }
        if (existingMysqlVersion == version) {
            logger.info("[DB DISPOSE] version match({}), skip dispose for {}", version, registryKey);
            return Result.NO_NEED;
        }

        logger.info("[DB DISPOSE] do dispose db for {}", registryKey);
        // kill existing process
        MysqldConfig mysqldConfig = MysqldConfig.aMysqldConfig(existingMysqlVersion)
                .withPort(port)
                .withCharset(Charset.UTF8)
                .withTempDir(mysqlInstanceDir(registryKey, port)).build();


        String path = Objects.requireNonNull(ClassUtils.getDefaultClassLoader().getResource(src_path)).getPath();
        DownloadConfig downloadConfig = aDownloadConfig().withCacheDir(path).build();

        IRuntimeConfig runtimeConfig = new RestoredRuntimeConfigBuilder().defaults(mysqldConfig, downloadConfig).build();
        RestoredMysqldStarter mysqldStarter = new RestoredMysqldStarter(runtimeConfig);
        RestoredMysqldProcess mysqldProcess = mysqldStarter.prepare(mysqldConfig).start();

        mysqldProcess.destroy();
        return Result.SUCCESS;
    }


    /**
     * @return Version.v8_0_32 or Version.v5_7_23 (null if not exist)
     * @see Version
     */
    public static Version getExistingMysqlVersion(int port) throws IOException, InterruptedException {
        if (!MySQLConstants.isUsed(port)) {
            return null;
        }
        // 1. list pid (bounded to port)
        String command = String.format("lsof -t -i :%d", port);
        Process proc = Runtime.getRuntime().exec(command);

        proc.waitFor();
        try (
                InputStream inputStream = proc.getInputStream();
                InputStreamReader in = new InputStreamReader(inputStream);
                BufferedReader stdInput = new BufferedReader(in)
        ) {
            List<Long> list = new ArrayList<>();
            String s;
            while ((s = stdInput.readLine()) != null) {
                list.add(Long.valueOf(s));
            }
            // 2. get mysql version from command
            List<ProcessHandle> collect = ProcessHandle.allProcesses().filter(e -> list.contains(e.pid())).collect(Collectors.toList());
            for (ProcessHandle processHandle : collect) {
                // command: /opt/data/drc/unit_test.mhaName-18383/mysql-8.0/bin/mysqld
                Optional<Version> version = processHandle
                        .info().command()
                        .map(e -> e.contains("mysql-8.0") ? Version.v8_0_32 : Version.v5_7_23);
                if (version.isPresent()) {
                    return version.get();
                }
            }
            List<ProcessHandle.Info> infoList = collect.stream().map(ProcessHandle::info).collect(Collectors.toList());
            logger.warn("port({}) is bound, but cannot found existing mysql process. command: {}; process: {}", port, command, infoList);
            return null;
        }
    }
}
