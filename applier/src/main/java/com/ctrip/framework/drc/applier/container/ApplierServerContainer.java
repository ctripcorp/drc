package com.ctrip.framework.drc.applier.container;

import com.ctrip.framework.drc.applier.activity.event.ApplierDumpEventActivity;
import com.ctrip.framework.drc.applier.mq.MqPositionResource;
import com.ctrip.framework.drc.applier.resource.mysql.DataSourceResource;
import com.ctrip.framework.drc.applier.resource.position.TransactionTableResource;
import com.ctrip.framework.drc.applier.server.*;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.common.AbstractResourceManager;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.io.Files;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author Slight
 * Nov 07, 2019
 */
@Component
public class ApplierServerContainer extends AbstractResourceManager implements ApplicationRunner {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ExecutorService taskExecutorService = ThreadUtils.newCachedThreadPool("ApplierServerContainer");

    protected ConcurrentHashMap<String, ApplierServerInCluster> servers
            = new ConcurrentHashMap<>();

    private ApplierWatcher watcher;

    public boolean addServer(ApplierConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        if (servers.containsKey(clusterKey)) {
            logger.info("applier servers contains {}", clusterKey);
            ApplierServerInCluster activeServer = servers.get(clusterKey);
            if (activeServer.config.equalsIgnoreProperties(config)) {
                if (!activeServer.config.equalsProperties(config)) {
                    ApplierServerInCluster server = servers.get(clusterKey);
                    ApplierDumpEventActivity dumpEventActivity = server.getDumpEventActivity();
                    if (dumpEventActivity != null) {
                        dumpEventActivity.changeProperties(config);
                        logger.info("new properties received, going to reconnect, old config: {}\n new config: {}", activeServer.config, config);
                    } else {
                        logger.info("new properties received, dumpEventActivity is null");
                    }
                } else {
                    logger.info("old config equals new config: {}", config);
                    return false;
                }
            } else {
                logger.info("same cluster, new config received, going to stop & dispose old applier server: " +
                        "\n" + activeServer.config + "\n" + config);
                doRemoveServer(activeServer);
                doAddServer(config);
                return true;
            }
        }

        logger.info("applier servers does not contain {}", clusterKey);
        createFile(clusterKey);
        doAddServer(config);
        return true;
    }

    protected void doAddServer(ApplierConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        ApplierServerInCluster newServer = getApplierServer(config);
        newServer.initialize();
        newServer.start();
        logger.info("applier servers put cluster: {}", clusterKey);
        ApplierServerInCluster server = servers.put(clusterKey, newServer);
        if (server != null && server.canStop()) {
            logger.warn("remove redundant instance for: {}, removed config: {}, added config: {}", clusterKey, server.getConfig(), config);
            doRemoveServer(server);
        }
    }

    protected ApplierServerInCluster getApplierServer(ApplierConfigDto config) throws Exception {
        ApplyMode applyMode = ApplyMode.getApplyMode(config.getApplyMode());
        logger.info("start to add applier servers for {}, apply mode is: {}", config.getRegistryKey(), applyMode.getName());
        switch (applyMode) {
            case transaction_table:
                return new TransactionTableApplierServerInCluster(config);
            case mq:
                return new MqServerInCluster(config);
            default:
                return new ApplierServerInCluster(config);
        }
    }

    private void doRemoveServer(ApplierServer server) throws Exception {
        if (server != null && !server.isDisposed()) {
            server.stop();
            server.dispose();
        }
    }

    public void removeServer(String registryKey, boolean deleted) throws Exception {
        ApplierServer server = servers.remove(registryKey);
        try {
            doRemoveServer(server);
        } catch (Exception e) {
            logger.error("remove server:{} for registryKey:{} error", server, registryKey);
        }
        if (deleted) {
            clearResource(registryKey);
        }
    }

    private void createFile(String registryKey) {
        taskExecutorService.submit(() -> {
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.applier.file.touch", registryKey, new Task() {
                @Override
                public void go() {
                    try {
                        File file = new File(SystemConfig.APPLIER_PATH + FilenameUtils.normalize(registryKey));
                        if (!file.exists()) {
                            Files.createParentDirs(file);
                            Files.touch(file);
                            logger.info("create file for {}", registryKey);
                        } else {
                            logger.info("skip touch file for {}", registryKey);
                        }
                    } catch (Throwable t) {
                        logger.error("[Touch] file for {} error", registryKey, t);
                    }
                }
            });

        });
    }

    private void clearResource(String registryKey) {
        LeaderElector leaderElector = zkLeaderElectors.remove(registryKey);
        deleteFile(registryKey);
        if (leaderElector != null) {
            try {
                leaderElector.stop();
            } catch (Exception e) {
                logger.error("LeaderElector stop error for {}", registryKey, e);
            }
        }
    }

    private void deleteFile(String registryKey) {
        try {
            File file = new File(SystemConfig.APPLIER_PATH + FilenameUtils.normalize(registryKey));
            file.delete();
            logger.info("delete file for {}", registryKey);
        } catch (Exception e) {
            logger.info("delete {} error", registryKey, e);
        }
    }

    public synchronized LeaderElector registerServer(String registryKey) {  // Idempotent
        createFile(registryKey);
        LeaderElector leaderElector = getLeaderElector(registryKey);
        logger.info("[Register] {} end", registryKey);
        return leaderElector;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        register(null);
        watcher = new ApplierWatcher(this, servers);
        watcher.initialize();
        watcher.start();
    }

    private void register(String dir) {
        if (StringUtils.isBlank(dir)) {
            dir = SystemConfig.APPLIER_PATH;
        }
        File logDir = new File(dir);
        File[] files = logDir.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                registerServer(file.getName());
            }
        }
    }

    /**
     * 1、close mysql connection
     * 2、close zk
     */
    @Override
    public void release() {
        try {
            int serverSize = servers.size();
            logger.info("start close {} datasources", serverSize);
            if (serverSize == 0) {
                return;
            }
            CountDownLatch countDownLatch = new CountDownLatch(serverSize);
            for (Map.Entry<String, ApplierServerInCluster> entry : servers.entrySet()) { //fix set gtid_next slow
                String registryKey = entry.getKey();
                taskExecutorService.submit(new ResourceReleaseTask(entry.getKey(), entry.getValue(), zkLeaderElectors.get(registryKey), countDownLatch));
            }
            boolean timeout = countDownLatch.await(1, TimeUnit.SECONDS);
            logger.info("release all resources with timeout of 1 second:{}", timeout);
            taskExecutorService.shutdown();
        } catch (Throwable t) {
            logger.error("await error", t);
        }
    }

    class ResourceReleaseTask implements Runnable {

        private String registryKey;

        private ApplierServerInCluster serverInCluster;

        private LeaderElector leaderElector;

        private CountDownLatch countDownLatch;

        public ResourceReleaseTask(String registryKey, ApplierServerInCluster serverInCluster, LeaderElector leaderElector, CountDownLatch countDownLatch) {
            this.registryKey = registryKey;
            this.serverInCluster = serverInCluster;
            this.leaderElector = leaderElector;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            try {
                DataSourceResource dataSourceResource = serverInCluster.getDataSourceResource(); //resource just has initialize and dispose
                if (dataSourceResource != null) {
                    dataSourceResource.dispose();
                    logger.info("close datasource for {}", registryKey);
                }

                // merge transaction table if need
                TransactionTableResource transactionTableResource = serverInCluster.getTransactionTableResource();
                if (transactionTableResource != null) {
                    transactionTableResource.dispose();
                    logger.info("dispose transaction for {} before shutdown", registryKey);
                }

                // persist mq position
                MqPositionResource mqPositionResource = serverInCluster.getMqPositionResource();
                if (mqPositionResource != null) {
                    mqPositionResource.dispose();
                    logger.info("dispose mq position for {} before shutdown", registryKey);
                }

                if (leaderElector != null) {
                    leaderElector.stop();
                    leaderElector.dispose();
                }
            } catch (Throwable t) {
                logger.error("lifecycle stop error", t);
            } finally {
                countDownLatch.countDown();
            }
        }
    }
}
