package com.ctrip.framework.drc.messenger.container;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.InfoDto;
import com.ctrip.framework.drc.core.server.config.SystemConfig;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.MessengerInfoDto;
import com.ctrip.framework.drc.fetcher.container.FetcherServerContainer;
import com.ctrip.framework.drc.fetcher.resource.context.MqPosition;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.framework.drc.messenger.server.KafkaServerInCluster;
import com.ctrip.framework.drc.messenger.server.MessengerWatcher;
import com.ctrip.framework.drc.messenger.server.MqServerInCluster;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.monitor.Task;
import com.google.common.io.Files;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author Slight
 * Nov 07, 2019
 */
@Component
public class MqServerContainer extends FetcherServerContainer {

    private MessengerWatcher watcher;

    protected FetcherServer getFetcherServer(FetcherConfigDto config) throws Exception {
        ApplyMode applyMode = ApplyMode.getApplyMode(config.getApplyMode());
        logger.info("start to add messenger servers for {}, apply mode is: {}", config.getRegistryKey(), applyMode.getName());
        switch (applyMode) {
            case mq:
            case db_mq:
                return new MqServerInCluster((MessengerConfigDto)config);
            case kafka:
                return new KafkaServerInCluster((MessengerConfigDto)config);
            default:
                throw new Exception("not support applyMode");
        }
    }

    protected void deleteFile(String registryKey) {
        try {
            File file = new File(SystemConfig.MESSENGER_PATH + FilenameUtils.normalize(registryKey));
            file.delete();
            logger.info("delete file for {}", registryKey);
        } catch (Exception e) {
            logger.info("delete {} error", registryKey, e);
        }
    }


    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        register(null);
        watcher = new MessengerWatcher(this, servers);
        watcher.initialize();
        watcher.start();
    }

    private void register(String dir) {
        if (StringUtils.isBlank(dir)) {
            dir = SystemConfig.MESSENGER_PATH;
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
            for (Map.Entry<String, FetcherServer> entry : servers.entrySet()) { //fix set gtid_next slow
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

    @Override
    protected void createFile(String registryKey) {
        taskExecutorService.submit(() -> {
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.messenger.file.touch", registryKey, new Task() {
                @Override
                public void go() {
                    try {
                        File file = new File(SystemConfig.MESSENGER_PATH + FilenameUtils.normalize(registryKey));
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

    @Override
    public List<MessengerInfoDto> getInfo() {
        List<MessengerInfoDto> list = new ArrayList<>();
        // applier master
        for (FetcherServer server : servers.values()) {
            MessengerInfoDto dto = new MessengerInfoDto();
            dto.setIp(server.config.ip);
            dto.setPort(server.config.port);
            dto.setRegistryKey(server.config.getRegistryKey());
            dto.setReplicatorIp(server.config.replicator.ip);
            dto.setDbInfo(server.config.target);
            dto.setMaster(true);
            list.add(dto);
        }
        Set<String> masters = list.stream().map(InfoDto::getRegistryKey).collect(Collectors.toSet());
        // applier slave
        for (String registryKey : zkLeaderElectors.keySet()) {
            if (masters.contains(registryKey)) {
                continue;
            }
            MessengerInfoDto dto = new MessengerInfoDto();
            dto.setRegistryKey(registryKey);
            dto.setMaster(false);
            list.add(dto);
        }
        return list;
    }

    class ResourceReleaseTask extends AbstractResourceReleaseTask {

        public ResourceReleaseTask(String registryKey, FetcherServer serverInCluster, LeaderElector leaderElector, CountDownLatch countDownLatch) {
            super(registryKey, serverInCluster, leaderElector, countDownLatch);
        }

        @Override
        public void run() {
            try {
                // persist mq position
                MqPosition mqPosition;
                ApplyMode applyMode = ApplyMode.getApplyMode(serverInCluster.config.getApplyMode());
                switch (applyMode) {
                    case mq:
                    case db_mq:
                        mqPosition = ((MqServerInCluster) serverInCluster).getMqPositionResource();
                        break;
                    case kafka:
                        mqPosition = ((KafkaServerInCluster) serverInCluster).getMqPositionResource();
                        break;
                    default:
                        throw new Exception("not support applyMode");
                }

                if (mqPosition != null) {
                    mqPosition.release();
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
