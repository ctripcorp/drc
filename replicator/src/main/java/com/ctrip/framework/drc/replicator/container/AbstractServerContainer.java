package com.ctrip.framework.drc.replicator.container;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.exception.DrcServerException;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.DrcServer;
import com.ctrip.framework.drc.core.server.common.AbstractResourceManager;
import com.ctrip.framework.drc.core.server.config.replicator.ReplicatorConfig;
import com.ctrip.framework.drc.core.server.container.ComponentRegistryHolder;
import com.ctrip.framework.drc.core.server.container.ServerContainer;
import com.ctrip.framework.drc.replicator.ReplicatorContainerApplication;
import com.ctrip.framework.drc.replicator.ReplicatorServer;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.MySQLSchemaManager;
import com.ctrip.framework.drc.replicator.impl.inbound.schema.SchemaManagerFactory;
import com.ctrip.framework.drc.replicator.store.manager.file.DefaultFileManager;
import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.framework.drc.core.driver.command.packet.ResultCode.PORT_ALREADY_EXIST;
import static com.ctrip.framework.drc.core.driver.command.packet.ResultCode.SERVER_ALREADY_EXIST;

/**
 * @Author limingdong
 * @create 2020/1/3
 */
public abstract class AbstractServerContainer extends AbstractResourceManager implements ServerContainer<ReplicatorConfig, ApiResult>, ApplicationRunner {

    private Map<String, Integer> runningPorts = Maps.newConcurrentMap();

    private Map<String, DrcServer> drcServers = Maps.newConcurrentMap();

    protected ApplicationContext context;

    @Override
    public ApiResult addServer(ReplicatorConfig config) {
        String registryKey = config.getRegistryKey();
        int port = config.getApplierPort();
        if (!drcServers.containsKey(registryKey)) {
            Set<Integer> ports = Sets.newHashSet(runningPorts.values());
            if (ports.contains(port)) {
                logger.error("[Used] port {} for cluster {}", port, registryKey);
                return ApiResult.getInstance(Boolean.FALSE, PORT_ALREADY_EXIST.getCode(), String.valueOf(getMaxNotInUsedPort()));
            }
            new RetryTask<>(new RegisterTask(config), 2).call();
            return ApiResult.getInstance(Boolean.TRUE, ResultCode.HANDLE_SUCCESS.getCode(), ResultCode.HANDLE_SUCCESS.getMessage());
        }

        logger.error("[Add] replicator {} fail due to server already exist in port {}", registryKey, port);
        return ApiResult.getInstance(Boolean.FALSE, SERVER_ALREADY_EXIST.getCode(), SERVER_ALREADY_EXIST.getMessage());
    }

    protected abstract ReplicatorServer getReplicatorServer(ReplicatorConfig config);

    private int getMaxNotInUsedPort() {
        int maxPort = -1;
        for (int p : runningPorts.values()) {
            if (p > maxPort) {
                maxPort = p;
            }
        }
        return maxPort + 1;
    }

    private void registerReplicators() {
        List<String> replicators = DefaultFileManager.getReplicators(null);
        for (String replicator : replicators) {
            if (replicator.startsWith(".")) {
                continue;
            }
            getLeaderElector(replicator);
            logger.info("[INACTIVE] {} to zookeeper", replicator);
        }
    }

    @Override
    public void removeServer(String registryKey, boolean closeLeaderElector) {
        try {
            DrcServer drcServer = drcServers.get(registryKey);
            if (drcServer != null) {
                logger.info("[Deregister] {} start", registryKey);
                deRegister(drcServer);
            }
            removeReplicatorCache(registryKey);  //no need to close zk
            if (closeLeaderElector) {
                LeaderElector zkLeaderElector = zkLeaderElectors.remove(registryKey);
                if (zkLeaderElector != null) {
                    zkLeaderElector.stop();
                    zkLeaderElector.dispose();
                }
                stopMySQLSchemaManager(registryKey);
                deleteFile(registryKey);
            }
        } catch (Throwable t) {
            throw new DrcServerException(
                    new ErrorMessage<>(ResultCode.UNKNOWN_ERROR,
                            String.format("Add server for cluster %s failed", registryKey)), t);
        }
    }

    private void stopMySQLSchemaManager(String registryKey) {
        MySQLSchemaManager mySQLSchemaManager = SchemaManagerFactory.remove(registryKey);
        if (mySQLSchemaManager != null) {
            try {
                LifecycleHelper.stopIfPossible(mySQLSchemaManager);
                LifecycleHelper.disposeIfPossible(mySQLSchemaManager);
            } catch (Exception e) {
                logger.error("[MySQLSchemaManager] stop for {} error", registryKey, e);
            }
        }
    }

    private void deleteFile(String registryKey) {
        File file = new File(DefaultFileManager.LOG_PATH + registryKey);
        if (file == null) {
            return;
        }
        File[] files = file.listFiles();
        if (files == null) {
            return;
        }
        for (File f : files) {
            f.delete();
        }
        file.delete();
    }

    private void removeReplicatorCache(String registryKey) {
        DrcServer drcServer = drcServers.remove(registryKey);
        if (drcServer != null) {
            runningPorts.remove(registryKey);
        }
    }

    private void register(ReplicatorServer replicatorServer) throws Exception {
        ComponentRegistry registry = ComponentRegistryHolder.getComponentRegistry();
        registry.add(replicatorServer);
    }

    private void deRegister(DrcServer drcServer) throws Exception {
        ComponentRegistry registry = ComponentRegistryHolder.getComponentRegistry();
        registry.remove(drcServer);
    }

    private void cacheServer(String key, int port, ReplicatorServer replicatorServer) {
        drcServers.put(key, replicatorServer);
        runningPorts.put(key, port);
        logger.info("[Register] {} to drcServers and  runningPorts with port {}", key, port);
    }

    @Override
    public Endpoint getUpstreamMaster(String registryKey) {
        DrcServer drcServer = drcServers.get(registryKey);
        if (drcServer == null) {
            return null;
        }
        return drcServer.getUpstreamMaster();
    }

    @Override
    public synchronized ApiResult register(String registryKey, int port) {
        try {
            if(zkLeaderElectors.get(registryKey) != null) {
                logger.info("[zkLeaderElectors] {} error, already elect leader", registryKey);
                return ApiResult.getInstance(Boolean.FALSE, SERVER_ALREADY_EXIST.getCode(), SERVER_ALREADY_EXIST.getMessage());
            }
            if(drcServers.get(registryKey) != null){
                logger.info("[Register] {} error, already cached in drcServers", registryKey);
                return ApiResult.getInstance(Boolean.FALSE, SERVER_ALREADY_EXIST.getCode(), SERVER_ALREADY_EXIST.getMessage());
            }
            Set<Integer> ports = Sets.newHashSet(runningPorts.values());
            if (ports.contains(port)) {
                logger.warn("[Used] port {} for cluster {}", port, registryKey);
                return ApiResult.getInstance(Boolean.FALSE, PORT_ALREADY_EXIST.getCode(), String.valueOf(getMaxNotInUsedPort()));
            }
            getLeaderElector(registryKey);  //register when restart or called by register
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        } catch (Exception e) {
            logger.error("[checkExists] for {} error", registryKey, e);
        }

        return ApiResult.getFailInstance(Boolean.FALSE);
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        logger.info("[Registry] register Component in AbstractServerContainer");
        ReplicatorContainerApplication.initComponentRegistry((ConfigurableApplicationContext) context);
        registerReplicators();
        logger.info("[Start] register replicator to zk");
    }

    class RegisterTask implements NamedCallable<Boolean> {

        private ReplicatorConfig config;

        private ReplicatorServer replicatorServer;

        public RegisterTask(ReplicatorConfig config) {
            this.config = config;
        }

        @Override
        public Boolean call() throws Exception {
            String registryKey = config.getRegistryKey();
            replicatorServer = getReplicatorServer(config);
            register(replicatorServer);
            cacheServer(registryKey, config.getApplierPort(), replicatorServer);
            getLogger().info("[Add] replicator {} successfully at port {}", registryKey, port);
            DefaultEventMonitorHolder.getInstance().logEvent("Register", registryKey);
            return true;
        }

        @Override
        public void afterFail() {
            NamedCallable.super.afterFail();
            getLogger().error("[Add] server {} error", config.getRegistryKey());
        }

        @Override
        public void afterException(Throwable t) {
            NamedCallable.super.afterException(t);
            releaseResource();
        }

        private void releaseResource() {
            try {
                stopMySQLSchemaManager(config.getRegistryKey());
                if (replicatorServer != null) {
                    deRegister(replicatorServer);
                }
            } catch (Throwable t) {
                getLogger().error("releaseResource error", t);
            }
        }

        @Override
        public Logger getLogger() {
            return logger;
        }
    }
}
