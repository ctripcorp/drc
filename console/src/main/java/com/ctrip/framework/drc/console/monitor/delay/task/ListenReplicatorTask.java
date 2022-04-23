package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.comparator.ListeningReplicatorComparator;
import com.ctrip.framework.drc.console.monitor.comparator.ReplicatorWrapperComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorPooledConnector;
import com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer;
import com.ctrip.framework.drc.console.pojo.ReplicatorMonitorWrapper;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.console.service.impl.DrcMaintenanceServiceImpl;
import com.ctrip.framework.drc.console.service.impl.ModuleCommunicationServiceImpl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.monitor.entity.BaseEntity;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-13
 * STEP 3
 */
@Order(1)
@Component
@DependsOn("dbClusterSourceProvider")
public class ListenReplicatorTask {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private ExecutorService monitorMasterRExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-masterR");

    private Map<String, ReplicatorWrapper> replicatorWrappers = Maps.newConcurrentMap();

    // key: id(aka registryKey)
    private Map<String, StaticDelayMonitorHolder> delayMonitorHolderMap = Maps.newConcurrentMap();

    private Set<String> processingListenServer = Sets.newConcurrentHashSet();

    private static final String MYSQL_DELAY_MESUREMENT = "fx.drc.delay.mysql";

    private static final String DRC_DELAY_MESUREMENT = "fx.drc.delay";

    private static final int INITIAL_DELAY = 5;

    private static final int PERIOD = 35;

    private static final String WARN = "warn";

    private static final String ERROR = "error";

    private static final String DEBUG = "debug";

    private static final String INFO = "info";

    private static final String CLOG_TAGS = "[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},routeInfo={}]]";

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private ModuleCommunicationServiceImpl moduleCommunicationService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private PeriodicalUpdateDbTask periodicalUpdateDbTask;

    @Autowired
    private MonitorService monitorService;

    /**
     * periodical check the master replicator
     */
    private ScheduledExecutorService updateListenReplicatorScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("update-listen-replicator-scheduledExecutorService");

    private ExecutorService handleChangeExecutor = ThreadUtils.newFixedThreadPool(OsUtils.getCpuCount(), "handle-listen-replicator-change-executor");

    @PostConstruct
    private void listen() {
        listenReplicator();
        ListenReplicatorMonitors();
    }

    private void listenReplicator() {
        updateListenReplicatorScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getListenReplicatorSwitch())) {
                        logger.info("[[monitor=delaylisten]] update listen replicator");
                        try {
                            updateListenReplicators();
                            pollDetectReplicators();
                        } catch (Throwable t) {
                            logger.error("[[monitor=delaylisten]] update listen replicator error, exception", t);
                        }
                    }
                } catch (Throwable t) {
                    logger.error("[[monitor=delaylisten]] listen replicator schedule error", t);
                }
            }
        }, INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    protected void setReplicatorWrappers(Map<String, ReplicatorWrapper> replicatorWrappers) {
        this.replicatorWrappers = replicatorWrappers;
    }

    private void ListenReplicatorMonitors() {
        if (!SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getListenReplicatorMonitorSwitch())) {
            return;
        }

        logger.info("[[monitor=delaylisten]] start listen replicator monitor");
        List<ReplicatorMonitorWrapper> replicatorMonitors = dbClusterSourceProvider.getReplicatorMonitorsInLocalDc();
        replicatorMonitors.forEach(wrapper -> {
            DelayMonitorSlaveConfig config = generateConfig(wrapper, MYSQL_DELAY_MESUREMENT);
            StaticDelayMonitorHolder delayMonitorHolder = new StaticDelayMonitorHolder(config, periodicalUpdateDbTask, consoleConfig.getDelayExceptionTime());
            try {
                delayMonitorHolder.initialize();
                delayMonitorHolder.start();
            } catch (Exception e) {
                log(config, "initialize and start error", ERROR, e);
            }
        });
    }

    private synchronized boolean markProcessingListenServer(String clusterId) {
        if (processingListenServer.contains(clusterId)) {
            return false;
        } else {
            processingListenServer.add(clusterId);
            return true;
        }
    }

    private void clearProcessingListenServer(String clusterId) {
        processingListenServer.remove(clusterId);
    }

    protected void addListenServer(String clusterId, ReplicatorWrapper replicatorWrapper) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] add replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (!delayMonitorHolderMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    DelayMonitorSlaveConfig config = generateConfig(replicatorWrapper, DRC_DELAY_MESUREMENT);
                    StaticDelayMonitorHolder delayMonitorHolder = new StaticDelayMonitorHolder(config, periodicalUpdateDbTask, consoleConfig.getDelayExceptionTime());
                    delayMonitorHolder.initialize();
                    delayMonitorHolder.start();
                    cacheServer(clusterId, replicatorWrapper, delayMonitorHolder);
                    logger.info("[[monitor=delaylisten]] add replicator listen success for cluster: {},", clusterId);
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] add replicator listen error for cluster: {},", clusterId, e);
                } finally {
                    clearProcessingListenServer(clusterId);
                }
            });
        } else {
            clearProcessingListenServer(clusterId);
        }
    }

    private void cacheServer(String clusterId, ReplicatorWrapper replicatorWrapper, StaticDelayMonitorHolder delayMonitorHolder) {
        delayMonitorHolderMap.put(clusterId, delayMonitorHolder);
        replicatorWrappers.put(clusterId, replicatorWrapper);
    }

    protected void removeListenServer(String clusterId) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] remove replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorHolderMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(clusterId);
                    if (delayMonitorHolder != null) {
                        delayMonitorHolder.stop();
                        delayMonitorHolder.dispose();
                        removeListenServerCache(clusterId);
                        logger.info("[[monitor=delaylisten]] remove replicator listen success for cluster: {},", clusterId);
                    }
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] remove replicator listen error for cluster: {},", clusterId, e);
                } finally {
                    clearProcessingListenServer(clusterId);
                }
            });
        } else {
            clearProcessingListenServer(clusterId);
        }
    }

    private void removeListenServerCache(String clusterId) {
        delayMonitorHolderMap.remove(clusterId);
        replicatorWrappers.remove(clusterId);
    }

    protected void modifyListenServer(String clusterId, ReplicatorWrapper newReplicatorWrapper) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] modify replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorHolderMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(clusterId);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorHolder.getConfig();
                    DelayMonitorSlaveConfig newConfig = generateConfig(newReplicatorWrapper, DRC_DELAY_MESUREMENT);
                    if (!oldConfig.equals(newConfig)) {
                        delayMonitorHolder.setConfig(newConfig);
                        restartListenServer(clusterId);
                        logger.info("[[monitor=delaylisten]] modify replicator listen success for cluster: {},", clusterId);
                    }
                    replicatorWrappers.put(clusterId, newReplicatorWrapper);
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] modify replicator listen error for cluster: {},", clusterId, e);
                } finally {
                    clearProcessingListenServer(clusterId);
                }
            });
        } else {
            clearProcessingListenServer(clusterId);
        }
    }

    public void switchListenReplicator(String clusterId, String newReplicatorIp, int newReplicatorPort) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] switch replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorHolderMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(clusterId);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorHolder.getConfig();
                    if (!oldConfig.getIp().equalsIgnoreCase(newReplicatorIp) || oldConfig.getPort() != newReplicatorPort) {
                        logger.info("[[monitor=delaylisten]] switch replicator listen for cluster: {}, for old endpoint({}:{}),", clusterId, oldConfig.getIp(), oldConfig.getPort());
                        Endpoint endpoint = new DefaultEndPoint(newReplicatorIp, newReplicatorPort);
                        logger.info("[[monitor=delaylisten]] switch replicator listen for cluster: {}, for new endpoint({}:{}),", clusterId, endpoint.getHost(), endpoint.getPort());
                        oldConfig.setEndpoint(endpoint);
                        restartListenServer(clusterId);
                        updateMasterReplicatorInDb(delayMonitorHolder.getConfig(), newReplicatorIp);
                        logger.info("[[monitor=delaylisten]] switch replicator listen success for cluster: {},", clusterId);
                    } else {
                        log(oldConfig, "ignore switch for old replicator endpoint(" + oldConfig.getIp() + ":" + oldConfig.getPort() + ") equals new replicator ip", INFO, null);
                    }
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] switch replicator listen error for cluster: {},", clusterId, e);
                } finally {
                    clearProcessingListenServer(clusterId);
                }
            });
        } else {
            clearProcessingListenServer(clusterId);
        }
    }

    public void restartListenServer(String clusterId) {
        StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(clusterId);
        if (delayMonitorHolder != null) {
            try {
                delayMonitorHolder.stop();
                delayMonitorHolder.dispose();
                delayMonitorHolder.initialize();
                delayMonitorHolder.start();
                logger.info("[[monitor=delaylisten]] restart replicator listen success for cluster: {},", clusterId);
            } catch (Exception e) {
                logger.error("[[monitor=delaylisten]] restart replicator listen error for cluster: {},", clusterId, e);
            }
        }
    }

    private DelayMonitorSlaveConfig generateConfig(DrcReplicatorWrapper wrapper, String measurement) {
        DelayMonitorSlaveConfig config = new DelayMonitorSlaveConfig();
        config.setDc(wrapper.getDcName());
        config.setDestDc(wrapper.getDestDcName());
        config.setCluster(wrapper.getClusterName());
        config.setMha(wrapper.getMhaName());
        config.setDestMha(wrapper.getDestMhaName());
        config.setRegistryKey(RegistryKey.from(wrapper.getClusterName(), wrapper.getDestMhaName()));
        Endpoint endpoint = new DefaultEndPoint(wrapper.getIp(), wrapper.getPort());
        config.setEndpoint(endpoint);
        config.setMeasurement(measurement);
        List<Route> routes = wrapper.getRoutes();
        Route route = RouteUtils.random(routes);
        config.setRouteInfo(route == null ? StringUtils.EMPTY : (route.routeProtocol() + " " + ProxyEndpoint.PROXY_SCHEME.TCP.name()));
        return config;
    }

    @VisibleForTesting
    protected void updateListenReplicators() throws SQLException {
        List<String> mhaNamesToBeMonitored = monitorService.getMhaNamesToBeMonitored();
        Map<String, ReplicatorWrapper> theNewestReplicatorWrappers = dbClusterSourceProvider.getReplicatorsNotInLocalDc(mhaNamesToBeMonitored);
        checkReplicatorWrapperChange(replicatorWrappers, theNewestReplicatorWrappers);
    }

    private void checkReplicatorWrapperChange(Map<String, ReplicatorWrapper> current, Map<String, ReplicatorWrapper> future) {
        ListeningReplicatorComparator comparator = new ListeningReplicatorComparator(current, future);
        comparator.compare();
        handleReplicatorWrapperChange(comparator, future);
    }

    private void handleReplicatorWrapperChange(ListeningReplicatorComparator comparator,
                                               Map<String, ReplicatorWrapper> future) {

        for (String added : comparator.getAdded()) {
            ReplicatorWrapper replicatorWrapperToAdd = future.get(added);
            addListenServer(added, replicatorWrapperToAdd);
        }

        for (String removed : comparator.getRemoved()) {
            removeListenServer(removed);
        }

        for (@SuppressWarnings("rawtypes") MetaComparator modifiedComparator : comparator.getMofified()) {
            ReplicatorWrapperComparator replicatorWrapperComparator = (ReplicatorWrapperComparator) modifiedComparator;
            String modified = replicatorWrapperComparator.getDbClusterId();
            ReplicatorWrapper newReplicatorWrapper = future.get(modified);
            modifyListenServer(modified, newReplicatorWrapper);
        }
    }

    private void pollDetectReplicators() {
        for (String id : delayMonitorHolderMap.keySet()) {
            StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(id);
            DelayMonitorSlaveConfig config = delayMonitorHolder.getConfig();
            logger.debug("request CM for real master R for {} in {}", id, config.getDestDc());
            Replicator activeReplicator = moduleCommunicationService.getActiveReplicator(config.getDestDc(), id);
            if (null != activeReplicator) {
                String ip = activeReplicator.getIp();
                Integer applierPort = activeReplicator.getApplierPort();
                log(config, "realR ip:" + ip + ", port: " + applierPort, INFO, null);
                switchListenReplicator(id, ip, applierPort);
            } else {
                log(config, "Fail to get realR(oldR is in tags), check next round", INFO, null);
            }
        }
    }

    protected void updateMasterReplicatorInDb(DelayMonitorSlaveConfig config, String newReplicatorIp) {
        String mhaName = config.getDestMha();
        String oldIp = config.getIp();
        monitorMasterRExecutorService.submit(() -> {
            if (drcMaintenanceService.updateMasterReplicator(mhaName, newReplicatorIp)) {
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.master", String.format("%s-%s", oldIp, newReplicatorIp));
                String measurement = "fx.drc.replicator.master";
                BaseEntity baseEntity = new BaseEntity(0L, "unset", config.getDestDc(), config.getCluster(), mhaName, RegistryKey.from(config.getCluster(), config.getDestMha()));
                DefaultReporterHolder.getInstance().reportResetCounter(baseEntity.getTags(), 1L, measurement);
            }
        });
    }

    private static void log(DelayMonitorSlaveConfig config, String msg, String types, Exception e) {
        String prefix = CLOG_TAGS + msg;
        switch (types) {
            case WARN:
                logger.warn(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
            case ERROR:
                logger.error(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo(), e);
                break;
            case DEBUG:
                logger.debug(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
            case INFO:
                logger.info(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
        }
    }

    public static final class StaticDelayMonitorHolder extends AbstractLifecycle {

        private StaticDelayMonitorServer server;

        private DelayMonitorSlaveConfig config;

        private PeriodicalUpdateDbTask periodicalUpdateDbTask;

        private long delayExceptionTime;

        public StaticDelayMonitorHolder(DelayMonitorSlaveConfig config, PeriodicalUpdateDbTask periodicalUpdateDbTask, long delayExceptionTime) {
            this.config = config;
            this.periodicalUpdateDbTask = periodicalUpdateDbTask;
            this.delayExceptionTime = delayExceptionTime;
        }

        @Override
        protected void doInitialize() throws Exception {
            String routeInfo = config.getRouteInfo();
            if (StringUtils.isNotBlank(routeInfo)) {
                ProxyRegistry.registerProxy(config.getIp(), config.getPort(), routeInfo);
            }
            server = new StaticDelayMonitorServer(config, new DelayMonitorPooledConnector(config.getEndpoint()), periodicalUpdateDbTask, delayExceptionTime);
            server.initialize();
            log(config, "initialized server", INFO, null);
        }

        @Override
        protected void doStart() throws Exception {
            server.start();
            log(config, "started server", INFO, null);
        }

        @Override
        protected void doStop() throws Exception {
            server.stop();
            log(config, "stopped server", INFO, null);
        }

        @Override
        protected void doDispose() throws Exception {
            if (StringUtils.isNotBlank(config.getRouteInfo())) {
                ProxyRegistry.unregisterProxy(config.getIp(), config.getPort());
            }
            server.dispose();
            log(config, "disposed server", INFO, null);
        }

        public DelayMonitorSlaveConfig getConfig() {
            return config;
        }

        public void setConfig(DelayMonitorSlaveConfig config) {
            this.config = config;
        }
    }
}
