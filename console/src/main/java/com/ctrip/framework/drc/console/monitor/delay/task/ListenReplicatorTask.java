package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.comparator.ListeningReplicatorComparator;
import com.ctrip.framework.drc.console.monitor.comparator.ReplicatorWrapperComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorPooledConnector;
import com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer;
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
import com.ctrip.xpipe.api.endpoint.Endpoint;
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


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-13
 * STEP 3
 */
@Order(1)
@Component("listenReplicatorTask")
@DependsOn("dbClusterSourceProvider")
public class ListenReplicatorTask extends AbstractLeaderAwareMonitor {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");

    private ExecutorService monitorMasterRExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-masterR");

    private Map<String, ReplicatorWrapper> replicatorWrappers = Maps.newConcurrentMap();

    // key: id(aka registryKey)
    private Map<String, StaticDelayMonitorServer> delayMonitorServerMap = Maps.newConcurrentMap();

    private Set<String> processingListenServer = Sets.newConcurrentHashSet();

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
    
    private ExecutorService handleChangeExecutor = ThreadUtils.newFixedThreadPool(OsUtils.getCpuCount(), "handle-listen-replicator-change-executor");

    @Override
    public void initialize() {
        setInitialDelay(INITIAL_DELAY);
        setPeriod(PERIOD);
        setTimeUnit(TimeUnit.SECONDS);
        super.initialize();
    }

    @Override
    public void scheduledTask() {
        try {
            if (isRegionLeader) {
                if ((SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getListenReplicatorSwitch()))) {
                    logger.info("[[monitor=delaylisten]] is Leader, going to listen all replicator");
                    updateListenReplicators();
                    pollDetectReplicators();
                } else {
                    logger.warn("[[monitor=delaylisten]] is Leader, but listen all replicator switch is off");
                }
            } else {
                logger.info("[[monitor=delaylisten]] not leader,going to close all delayMonitorServers");
                for (String clusterId : delayMonitorServerMap.keySet()) {
                    removeListenServer(clusterId);
                }
                DefaultReporterHolder.getInstance().removeRegister(DRC_DELAY_MESUREMENT);
            }
        } catch (Throwable t) {
            logger.error("[[monitor=delaylisten]] listen replicator schedule error", t);
        }
    }
    
    
    @VisibleForTesting
    protected void setReplicatorWrappers(Map<String, ReplicatorWrapper> replicatorWrappers) {
        this.replicatorWrappers = replicatorWrappers;
    }

    @VisibleForTesting
    protected void setDelayMonitorServerMap(Map<String, StaticDelayMonitorServer> delayMonitorServerMap) {
        this.delayMonitorServerMap = delayMonitorServerMap;
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
        if (!delayMonitorServerMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] add replicator listen start for cluster: {},", clusterId);
                    DelayMonitorSlaveConfig config = generateConfig(replicatorWrapper, DRC_DELAY_MESUREMENT);
                    StaticDelayMonitorServer delayMonitorServer = createDelayMonitorServer(config);
                    delayMonitorServer.initialize();
                    delayMonitorServer.start();
                    cacheServer(clusterId, replicatorWrapper, delayMonitorServer);
                    logger.info("[[monitor=delaylisten]] add replicator listen success for cluster: {},config is: {}", clusterId,config);
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

    @VisibleForTesting
    protected StaticDelayMonitorServer createDelayMonitorServer(DelayMonitorSlaveConfig config) {
        return new StaticDelayMonitorServer(config, new DelayMonitorPooledConnector(config.getEndpoint()), periodicalUpdateDbTask, consoleConfig.getDelayExceptionTime());
    }

    private void cacheServer(String clusterId, ReplicatorWrapper replicatorWrapper, StaticDelayMonitorServer delayMonitorServer) {
        delayMonitorServerMap.put(clusterId, delayMonitorServer);
        replicatorWrappers.put(clusterId, replicatorWrapper);
    }

    protected void removeListenServer(String clusterId) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] remove replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorServerMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] remove replicator listen start for cluster: {},", clusterId);
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServerMap.get(clusterId);
                    if (delayMonitorServer != null) {
                        delayMonitorServer.stop();
                        delayMonitorServer.dispose();
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
        delayMonitorServerMap.remove(clusterId);
        replicatorWrappers.remove(clusterId);
    }

    protected void modifyListenServer(String clusterId, ReplicatorWrapper newReplicatorWrapper) {
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] modify replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorServerMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] modify replicator listen start for cluster: {},", clusterId);
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServerMap.get(clusterId);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorServer.getConfig();
                    DelayMonitorSlaveConfig newConfig = generateConfig(newReplicatorWrapper, DRC_DELAY_MESUREMENT);
                    if (newConfig.equals(oldConfig)) {
                        logger.info("[[monitor=delaylisten]] modify replicator listen fail for cluster: {} for same config,", clusterId);
                    } else {
                        restartListenServer(clusterId, newConfig);
                        logger.info("[[monitor=delaylisten]] modify replicator listen success for cluster: {},new config is: {}", clusterId,newConfig);
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
        if (delayMonitorServerMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServerMap.get(clusterId);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorServer.getConfig();
                    if (!oldConfig.getIp().equalsIgnoreCase(newReplicatorIp) || oldConfig.getPort() != newReplicatorPort) {
                        logger.info("[[monitor=delaylisten]] switch replicator listen for cluster: {}, old endpoint({}:{}), new endpoint({}:{})", clusterId, oldConfig.getIp(), oldConfig.getPort(), newReplicatorIp, newReplicatorPort);
                        DelayMonitorSlaveConfig newConfig = oldConfig.clone();
                        Endpoint newEndpoint = new DefaultEndPoint(newReplicatorIp, newReplicatorPort);
                        newConfig.setEndpoint(newEndpoint);
                        restartListenServer(clusterId, newConfig);
                        updateMasterReplicatorInDb(delayMonitorServer.getConfig(), newReplicatorIp);
                        logger.info("[[monitor=delaylisten]] switch replicator listen success for cluster: {},new Config is:{}", clusterId,newConfig);
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

    public void restartListenServer(String clusterId, DelayMonitorSlaveConfig newConfig) {
        StaticDelayMonitorServer oldDelayMonitorServer = delayMonitorServerMap.get(clusterId);
        if (oldDelayMonitorServer == null) {
            logger.error("[[monitor=delaylisten]] restart replicator listen error for cluster: {} because old delay monitor server not exist", clusterId);
            return;
        }
        try {
            oldDelayMonitorServer.stop();
            oldDelayMonitorServer.dispose();
            StaticDelayMonitorServer newDelayMonitorServer = createDelayMonitorServer(newConfig);
            newDelayMonitorServer.initialize();
            newDelayMonitorServer.start();
            delayMonitorServerMap.put(clusterId, newDelayMonitorServer);
            logger.info("[[monitor=delaylisten]] restart replicator listen success for cluster: {},", clusterId);
        } catch (Exception e) {
            logger.error("[[monitor=delaylisten]] restart replicator listen error for cluster: {},", clusterId, e);
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
        List<String> mhasToBeMonitored = monitorService.getMhaNamesToBeMonitored();
        Map<String, ReplicatorWrapper> theNewestReplicatorWrappers = dbClusterSourceProvider.getReplicatorsNeeded(mhasToBeMonitored);
        checkReplicatorWrapperChange(replicatorWrappers, theNewestReplicatorWrappers);
    }

    private void checkReplicatorWrapperChange(Map<String, ReplicatorWrapper> current, Map<String, ReplicatorWrapper> future) {
        ListeningReplicatorComparator comparator = new ListeningReplicatorComparator(current, future);
        comparator.compare();
        handleReplicatorWrapperChange(comparator, future);
    }

    private void handleReplicatorWrapperChange(ListeningReplicatorComparator comparator,
                                               Map<String, ReplicatorWrapper> future) {

        logger.info("handle change for added size: {}, removed size: {}, modified size: {}", comparator.getAdded().size(), comparator.getRemoved().size(), comparator.getMofified().size());
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
        for (String id : delayMonitorServerMap.keySet()) {
            StaticDelayMonitorServer delayMonitorServer = delayMonitorServerMap.get(id);
            DelayMonitorSlaveConfig config = delayMonitorServer.getConfig();
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
    
}
