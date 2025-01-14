package com.ctrip.framework.drc.fetcher.container;

import com.ctrip.framework.drc.core.server.common.AbstractResourceManager;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherConfigDto;
import com.ctrip.framework.drc.core.server.config.applier.dto.FetcherInfoDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.fetcher.activity.event.FetcherDumpEventActivity;
import com.ctrip.framework.drc.fetcher.server.FetcherServer;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by shiruixin
 * 2024/11/1 17:38
 */
public abstract class FetcherServerContainer extends AbstractResourceManager implements ApplicationRunner {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, ReentrantLock> cachedLocks = new ConcurrentHashMap<>();

    protected ExecutorService taskExecutorService = ThreadUtils.newCachedThreadPool("FetcherServerContainer");

    protected ConcurrentHashMap<String, FetcherServer> servers
            = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, FetcherServer> getServers() {
        return servers;
    }

    public boolean addServer(FetcherConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        if (servers.containsKey(clusterKey)) {
            logger.info("fetcher servers contains {}", clusterKey);
            FetcherServer activeServer = servers.get(clusterKey);
            if (activeServer.config.equalsIgnoreProperties(config)) {
                if (!activeServer.config.equalsProperties(config)) {
                    FetcherServer server = servers.get(clusterKey);
                    FetcherDumpEventActivity dumpEventActivity = server.getDumpEventActivity();
                    if (dumpEventActivity != null) {
                        dumpEventActivity.changeProperties(config);
                        logger.info("new properties received, going to reconnect, old config: {}\n new config: {}", activeServer.config, config);
                        return true;
                    } else {
                        logger.info("new properties received, dumpEventActivity is null");
                    }
                } else {
                    logger.info("old config equals new config: {}", config);
                    return false;
                }
            } else {
                logger.info("same cluster, new config received, going to stop & dispose old fetcher server: " +
                        "\n" + activeServer.config + "\n" + config);
                doRemoveServer(activeServer);
                doAddServer(config);
                return true;
            }
        }

        logger.info("fetcher servers does not contain {}", clusterKey);
        createFile(clusterKey);
        doAddServer(config);
        return true;
    }

    protected void doAddServer(FetcherConfigDto config) throws Exception {
        String clusterKey = config.getRegistryKey();
        FetcherServer newServer = getFetcherServer(config);
        newServer.initialize();
        newServer.start();
        logger.info("fetcher servers put cluster: {}", clusterKey);
        FetcherServer server = servers.put(clusterKey, newServer);
        if (server != null && server.canStop()) {
            logger.warn("remove redundant instance for: {}, removed config: {}, added config: {}", clusterKey, server.getConfig(), config);
            doRemoveServer(server);
        }
    }

    protected abstract FetcherServer getFetcherServer(FetcherConfigDto config) throws Exception;


    private void doRemoveServer(FetcherServer server) throws Exception {
        if (server != null && !server.isDisposed()) {
            server.stop();
            server.dispose();
        } else {
            String name = server == null ? null : server.getName();
            String status = server == null ? null : server.getPhaseName();
            logger.info("ignore server remove, name: {}, status: {}", name, status);
        }
    }

    public void removeServer(String registryKey, boolean deleted) throws Exception {
        FetcherServer server = servers.remove(registryKey);
        try {
            doRemoveServer(server);
        } catch (Exception e) {
            logger.error("remove server:{} for registryKey:{} error", server, registryKey);
        }
        if (deleted) {
            clearResource(registryKey);
        }
    }


    protected abstract void createFile(String registryKey);

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

    protected abstract void deleteFile(String registryKey);

    public LeaderElector registerServer(String registryKey) {  // Idempotent
        Lock lock = cachedLocks.computeIfAbsent(registryKey, key -> new ReentrantLock());
        lock.lock();
        try {
            createFile(registryKey);
            LeaderElector leaderElector = getLeaderElector(registryKey);
            logger.info("[Register] {} end", registryKey);
            return leaderElector;
        } finally {
            lock.unlock();
        }
    }

    public abstract void run(ApplicationArguments applicationArguments) throws Exception;

    /**
     * 1、close mysql connection
     * 2、close zk
     */
    abstract public void release();

    public boolean containServer(String registryKey) {
        return servers.containsKey(registryKey);
    }

    public FetcherServer getServer(String registryKey) {
        return servers.get(registryKey);
    }

    abstract public List< ? extends FetcherInfoDto> getInfo();

    public abstract class AbstractResourceReleaseTask implements Runnable {
        protected String registryKey;

        protected FetcherServer serverInCluster;

        protected LeaderElector leaderElector;

        protected CountDownLatch countDownLatch;

        public AbstractResourceReleaseTask(String registryKey, FetcherServer serverInCluster, LeaderElector leaderElector, CountDownLatch countDownLatch) {
            this.registryKey = registryKey;
            this.serverInCluster = serverInCluster;
            this.leaderElector = leaderElector;
            this.countDownLatch = countDownLatch;
        }

    }

}
