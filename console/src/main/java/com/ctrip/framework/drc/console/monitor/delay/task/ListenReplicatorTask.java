package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.AbstractLeaderAwareMonitor;
import com.ctrip.framework.drc.console.monitor.comparator.ListeningReplicatorComparator;
import com.ctrip.framework.drc.console.monitor.comparator.ReplicatorWrapperComparator;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorSlaveConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DrcReplicatorWrapper;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.driver.DelayMonitorPooledConnector;
import com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer;
import com.ctrip.framework.drc.console.param.MhaReplicatorEntity;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.meta.comparator.MetaComparator;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @author shenhaibo
 * @version 1.0 date: 2019-12-13 STEP 3
 */
@Order(1)
@Component("listenReplicatorTask")
@DependsOn({"metaProviderV2"})
public class ListenReplicatorTask extends AbstractLeaderAwareMonitor {

    private static final Logger logger = LoggerFactory.getLogger("delayMonitorLogger");
    private static final String WARN = "warn";
    private static final String ERROR = "error";
    private static final String DEBUG = "debug";
    private static final String INFO = "info";
    private static final String CLOG_TAGS = "[[monitor=delay,direction={}({}):{}({}),cluster={},replicator={}:{},measurement={},routeInfo={}]]";

    private static final String DRC_DELAY_MESUREMENT = "fx.drc.delay";
    private static final int INITIAL_DELAY = 5;
    private static final int PERIOD = 35;

    private final ExecutorService monitorMasterRExecutorService = ThreadUtils.newSingleThreadExecutor(getClass().getSimpleName() + "-masterR");

    private final ExecutorService handleChangeExecutor = ThreadUtils.newFixedThreadPool(OsUtils.getCpuCount(), "handle-listen-replicator-change-executor");

    // key: slave's ip:port
    private final Map<String, ReplicatorWrapper> slaveReplicatorWrappers = Maps.newConcurrentMap();
    private final Map<String, StaticDelayMonitorServer> slaveReplicatorDelayMonitorServerMap = Maps.newConcurrentMap();
    private final Set<String> processingListenServer = Sets.newConcurrentHashSet();

    // key: id(aka registryKey)
    private Map<String, ReplicatorWrapper> replicatorWrappers = Maps.newConcurrentMap();
    private Map<String, StaticDelayMonitorServer> delayMonitorServerMap = Maps.newConcurrentMap();

    @Autowired
    private CacheMetaService cacheMetaService;
    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private PeriodicalUpdateDbTask periodicalUpdateDbTask;
    @Autowired
    private PeriodicalUpdateDbTaskV2 periodicalUpdateDbTaskV2;
    @Autowired
    private MonitorServiceV2 monitorServiceV2;
    @Autowired
    private CentralService centralService;
    @Autowired
    private ResourceService resourceService;

    private static void log(DelayMonitorSlaveConfig config, String msg, String types, Exception e) {
        String prefix = CLOG_TAGS + msg;
        switch (types) {
            case WARN:
                logger.warn(prefix, config.getMha(), config.getDc(), config.getDestMha(),
                        config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(),
                        config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
            case ERROR:
                logger.error(prefix, config.getMha(), config.getDc(), config.getDestMha(),
                        config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(),
                        config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo(), e);
                break;
            case DEBUG:
                logger.debug(prefix, config.getMha(), config.getDc(), config.getDestMha(),
                        config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(),
                        config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
            case INFO:
                logger.info(prefix, config.getMha(), config.getDc(), config.getDestMha(),
                        config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(),
                        config.getEndpoint().getPort(), config.getMeasurement(), config.getRouteInfo());
                break;
        }
    }

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
                    pollDetectReplicatorsV2();
                    updateListenReplicatorSlaves();
                } else {
                    logger.warn("[[monitor=delaylisten]] is Leader, but listen all replicator switch is off");
                }
            } else {
                logger.info("[[monitor=delaylisten]] not leader,going to close all delayMonitorServers");
                for (String clusterId : delayMonitorServerMap.keySet()) {
                    removeListenServer(clusterId, replicatorWrappers, delayMonitorServerMap);
                }
                for (String address : slaveReplicatorDelayMonitorServerMap.keySet()) {
                    removeListenServer(address, slaveReplicatorWrappers, slaveReplicatorDelayMonitorServerMap);
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
    protected void setDelayMonitorServerMap(
            Map<String, StaticDelayMonitorServer> delayMonitorServerMap) {
        this.delayMonitorServerMap = delayMonitorServerMap;
    }

    private synchronized boolean markProcessingListenServer(String replicatorUniqueKey) {
        if (processingListenServer.contains(replicatorUniqueKey)) {
            return false;
        } else {
            processingListenServer.add(replicatorUniqueKey);
            return true;
        }
    }

    private void clearProcessingListenServer(String replicatorUniqueKey) {
        processingListenServer.remove(replicatorUniqueKey);
    }

    protected void addListenServer(String replicatorUniqueKey,
                                   ReplicatorWrapper replicatorWrapper,
                                   Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                   Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        if (!markProcessingListenServer(replicatorUniqueKey)) {
            logger.error("[[monitor=delaylisten]] add replicator listen fail for cluster: {} due to already in processingListenServer", replicatorUniqueKey);
            return;
        }
        if (!delayMonitorServersHolder.containsKey(replicatorUniqueKey)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] add replicator listen start for cluster: {},", replicatorUniqueKey);
                    DelayMonitorSlaveConfig config = generateConfig(replicatorWrapper, DRC_DELAY_MESUREMENT);
                    StaticDelayMonitorServer delayMonitorServer = createDelayMonitorServer(config);
                    delayMonitorServer.initialize();
                    delayMonitorServer.start();
                    cacheServer(replicatorUniqueKey, replicatorWrapper, delayMonitorServer, replicatorWrappersHolder, delayMonitorServersHolder);
                    logger.info("[[monitor=delaylisten]] add replicator listen success for cluster: {},config is: {}", replicatorUniqueKey, config);
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] add replicator listen error for cluster: {},", replicatorUniqueKey, e);
                } finally {
                    clearProcessingListenServer(replicatorUniqueKey);
                }
            });
        } else {
            clearProcessingListenServer(replicatorUniqueKey);
        }
    }

    @VisibleForTesting
    protected StaticDelayMonitorServer createDelayMonitorServer(DelayMonitorSlaveConfig config) {
        return new StaticDelayMonitorServer(
                config,
                new DelayMonitorPooledConnector(config.getEndpoint()),
                periodicalUpdateDbTask,
                periodicalUpdateDbTaskV2,
                centralService,
                consoleConfig.getDelayExceptionTime()
        );
    }

    private void cacheServer(String replicatorUniqueKey, ReplicatorWrapper replicatorWrapper,
                             StaticDelayMonitorServer delayMonitorServer,
                             Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                             Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        replicatorWrappersHolder.put(replicatorUniqueKey, replicatorWrapper);
        delayMonitorServersHolder.put(replicatorUniqueKey, delayMonitorServer);
    }

    protected void removeListenServer(String replicatorUniqueKey,
                                      Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                      Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        if (!markProcessingListenServer(replicatorUniqueKey)) {
            logger.error("[[monitor=delaylisten]] remove replicator listen fail for cluster: {} due to already in processingListenServer", replicatorUniqueKey);
            return;
        }
        if (delayMonitorServersHolder.containsKey(replicatorUniqueKey)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] remove replicator listen start for cluster: {},", replicatorUniqueKey);
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServersHolder.get(replicatorUniqueKey);
                    if (delayMonitorServer != null) {
                        delayMonitorServer.stop();
                        delayMonitorServer.dispose();
                        removeListenServerCache(replicatorUniqueKey, replicatorWrappersHolder, delayMonitorServersHolder);
                        logger.info("[[monitor=delaylisten]] remove replicator listen success for cluster: {},", replicatorUniqueKey);
                    }
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] remove replicator listen error for cluster: {},", replicatorUniqueKey, e);
                } finally {
                    clearProcessingListenServer(replicatorUniqueKey);
                }
            });
        } else {
            clearProcessingListenServer(replicatorUniqueKey);
        }
    }

    private void removeListenServerCache(String replicatorUniqueKey,
                                         Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                         Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        replicatorWrappersHolder.remove(replicatorUniqueKey);
        delayMonitorServersHolder.remove(replicatorUniqueKey);
    }

    protected void modifyListenServer(String replicatorUniqueKey,
                                      ReplicatorWrapper newReplicatorWrapper,
                                      Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                      Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        if (!markProcessingListenServer(replicatorUniqueKey)) {
            logger.error("[[monitor=delaylisten]] modify replicator listen fail for cluster: {} to already in processingListenServer", replicatorUniqueKey);
            return;
        }
        if (delayMonitorServersHolder.containsKey(replicatorUniqueKey)) {
            handleChangeExecutor.submit(() -> {
                try {
                    logger.info("[[monitor=delaylisten]] modify replicator listen start for cluster: {},", replicatorUniqueKey);
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServersHolder.get(replicatorUniqueKey);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorServer.getConfig();
                    DelayMonitorSlaveConfig newConfig = generateConfig(newReplicatorWrapper, DRC_DELAY_MESUREMENT);
                    if (newConfig.equals(oldConfig)) {
                        logger.info("[[monitor=delaylisten]] modify replicator listen fail for cluster: {} for same config,", replicatorUniqueKey);
                    } else {
                        restartListenServer(replicatorUniqueKey, newConfig, delayMonitorServersHolder);
                        logger.info("[[monitor=delaylisten]] modify replicator listen success for cluster: {},new config is: {}", replicatorUniqueKey, newConfig);
                    }
                    replicatorWrappersHolder.put(replicatorUniqueKey, newReplicatorWrapper);
                } catch (Exception e) {
                    logger.error("[[monitor=delaylisten]] modify replicator listen error for cluster: {},", replicatorUniqueKey, e);
                } finally {
                    clearProcessingListenServer(replicatorUniqueKey);
                }
            });
        } else {
            clearProcessingListenServer(replicatorUniqueKey);
        }
    }

    public void switchListenReplicator(String clusterId, String newReplicatorIp, int newReplicatorPort) {
        logger.info("[[monitor=delaylisten]] switchListenReplicator for cluster: {}, {}:{}", clusterId, newReplicatorIp, newReplicatorPort);
        if (!markProcessingListenServer(clusterId)) {
            logger.error("[[monitor=delaylisten]] switch replicator listen fail for cluster: {} due to already in processingListenServer", clusterId);
            return;
        }
        if (delayMonitorServerMap.containsKey(clusterId)) {
            handleChangeExecutor.submit(() -> {
                try {
                    StaticDelayMonitorServer delayMonitorServer = delayMonitorServerMap.get(clusterId);
                    DelayMonitorSlaveConfig oldConfig = delayMonitorServer.getConfig();
                    updateMasterReplicatorIfChange(delayMonitorServer.getConfig().getDestMha(), newReplicatorIp);
                    if (!oldConfig.getIp().equalsIgnoreCase(newReplicatorIp) || oldConfig.getPort() != newReplicatorPort) {
                        logger.info("[[monitor=delaylisten]] switch replicator listen for cluster: {}, old endpoint({}:{}), new endpoint({}:{})",
                                clusterId, oldConfig.getIp(), oldConfig.getPort(), newReplicatorIp, newReplicatorPort);
                        DelayMonitorSlaveConfig newConfig = oldConfig.clone();
                        Endpoint newEndpoint = new DefaultEndPoint(newReplicatorIp, newReplicatorPort);
                        newConfig.setEndpoint(newEndpoint);
                        restartListenServer(clusterId, newConfig, delayMonitorServerMap);
                        logger.info("[[monitor=delaylisten]] switch replicator listen success for cluster: {},new Config is:{}", clusterId, newConfig);
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
            logger.warn("[[monitor=delaylisten]] delayMonitorServerMap not contain: {}", clusterId);
            clearProcessingListenServer(clusterId);
        }
    }

    public void restartListenServer(String replicatorUniqueKey, DelayMonitorSlaveConfig newConfig, Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        StaticDelayMonitorServer oldDelayMonitorServer = delayMonitorServersHolder.get(replicatorUniqueKey);
        if (oldDelayMonitorServer == null) {
            logger.error("[[monitor=delaylisten]] restart replicator listen error for cluster: {} because old delay monitor server not exist", replicatorUniqueKey);
            return;
        }
        try {
            oldDelayMonitorServer.stop();
            oldDelayMonitorServer.dispose();
            StaticDelayMonitorServer newDelayMonitorServer = createDelayMonitorServer(newConfig);
            newDelayMonitorServer.initialize();
            newDelayMonitorServer.start();
            delayMonitorServersHolder.put(replicatorUniqueKey, newDelayMonitorServer);
            logger.info("[[monitor=delaylisten]] restart replicator listen success for cluster: {},", replicatorUniqueKey);
        } catch (Exception e) {
            logger.error("[[monitor=delaylisten]] restart replicator listen error for cluster: {},", replicatorUniqueKey, e);
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
    protected void filterMasterReplicatorV2(Map<String, List<ReplicatorWrapper>> allReplicators) {
        List<ReplicatorWrapper> allReplicatorWrappers = Lists.newArrayList();
        allReplicators.values().forEach(allReplicatorWrappers::addAll);
        Map<String, Set<String>> dc2ReplicatorIps = allReplicatorWrappers.stream()
                .collect(Collectors.groupingBy(ReplicatorWrapper::getDcName, Collectors.mapping(ReplicatorWrapper::getIp, Collectors.toSet())));
        Map<String, ReplicatorInfoDto> clusterIdT2MasterReplicators = getMasterReplicatorInfoDtos(dc2ReplicatorIps);

        for (Entry<String, List<ReplicatorWrapper>> entry : allReplicators.entrySet()) {
            String clusterId = entry.getKey();
            List<ReplicatorWrapper> rWrappers = entry.getValue();
            ReplicatorWrapper rWrapper = rWrappers.get(0);
            logger.info("filterMasterReplicator cluster: {}", clusterId);
            ReplicatorInfoDto activeReplicator = clusterIdT2MasterReplicators.get(clusterId);
            if (null != activeReplicator) {
                String ip = activeReplicator.getIp();
                int applierPort = activeReplicator.getApplierPort();
                logger.info("filterMasterReplicator cluster: {}, replicator: {}", clusterId, ip + ":" + applierPort);
                rWrappers.removeIf(current -> current.getIp().equalsIgnoreCase(activeReplicator.getIp()) && current.getPort() == activeReplicator.getApplierPort());
                updateMasterReplicatorIfChange(rWrapper.mhaName, activeReplicator.getIp());
            } else {
                logger.warn("filterMasterReplicator no master replicator, cluster: {}", clusterId);
                rWrappers.removeIf(current -> current.getReplicator().isMaster());
            }
        }
    }

    @VisibleForTesting
    protected void updateListenReplicators() throws SQLException {
        List<String> mhasToBeMonitored = monitorServiceV2.getMhaNamesToBeMonitored();
        Map<String, ReplicatorWrapper> theNewestReplicatorWrappers = cacheMetaService.getMasterReplicatorsToBeMonitored(mhasToBeMonitored);
        checkReplicatorWrapperChange(replicatorWrappers, theNewestReplicatorWrappers, replicatorWrappers, delayMonitorServerMap);
    }

    private void checkReplicatorWrapperChange(Map<String, ReplicatorWrapper> current,
                                              Map<String, ReplicatorWrapper> future,
                                              Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                              Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {
        ListeningReplicatorComparator comparator = new ListeningReplicatorComparator(current, future);
        comparator.compare();
        handleReplicatorWrapperChange(comparator, future, replicatorWrappersHolder, delayMonitorServersHolder);
    }

    private void handleReplicatorWrapperChange(ListeningReplicatorComparator comparator,
                                               Map<String, ReplicatorWrapper> future,
                                               Map<String, ReplicatorWrapper> replicatorWrappersHolder,
                                               Map<String, StaticDelayMonitorServer> delayMonitorServersHolder) {

        logger.info("handle change for added size: {}, removed size: {}, modified size: {}",
                comparator.getAdded().size(), comparator.getRemoved().size(), comparator.getMofified().size());
        for (String added : comparator.getAdded()) {
            ReplicatorWrapper replicatorWrapperToAdd = future.get(added);
            addListenServer(added, replicatorWrapperToAdd, replicatorWrappersHolder, delayMonitorServersHolder);
        }

        for (String removed : comparator.getRemoved()) {
            removeListenServer(removed, replicatorWrappersHolder, delayMonitorServersHolder);
        }

        for (@SuppressWarnings("rawtypes") MetaComparator modifiedComparator : comparator.getMofified()) {
            ReplicatorWrapperComparator replicatorWrapperComparator = (ReplicatorWrapperComparator) modifiedComparator;
            String modified = replicatorWrapperComparator.getDbClusterId();
            ReplicatorWrapper newReplicatorWrapper = future.get(modified);
            modifyListenServer(modified, newReplicatorWrapper, replicatorWrappersHolder, delayMonitorServersHolder);
        }
    }

    @VisibleForTesting
    protected void updateListenReplicatorSlaves() throws SQLException {
        Map<String, ReplicatorWrapper> theNewestSlaveReplicatorWrappers = Maps.newConcurrentMap();

        Map<String, List<ReplicatorWrapper>> allReplicatorsInLocalRegion = cacheMetaService.getAllReplicatorsInLocalRegion();
        filterMasterReplicatorV2(allReplicatorsInLocalRegion);
        for (List<ReplicatorWrapper> replicatorWrappers : allReplicatorsInLocalRegion.values()) {
            for (ReplicatorWrapper rWrapper : replicatorWrappers) {
                theNewestSlaveReplicatorWrappers.put(rWrapper.getIp() + ":" + rWrapper.getPort(), rWrapper);
            }
        }

        logger.info("[[tag=replicatorSlaveMonitor]] get NewestReplicatorSlaves count:{},current:{}", theNewestSlaveReplicatorWrappers.size(), slaveReplicatorWrappers.size());
        checkReplicatorWrapperChange(slaveReplicatorWrappers, theNewestSlaveReplicatorWrappers, slaveReplicatorWrappers, slaveReplicatorDelayMonitorServerMap);
    }

    @VisibleForTesting
    protected void pollDetectReplicatorsV2() {
        List<String> clusterIds = Lists.newArrayList(delayMonitorServerMap.keySet());
        Map<String, Set<String>> dc2ReplicatorIps = cacheMetaService.getDc2ReplicatorIps(clusterIds);
        Map<String, ReplicatorInfoDto> clusterIdT2MasterReplicators = getMasterReplicatorInfoDtos(dc2ReplicatorIps);

        for (String id : delayMonitorServerMap.keySet()) {
            logger.info("pollDetectReplicators cluster: {}", id);
            ReplicatorInfoDto activeReplicator = clusterIdT2MasterReplicators.get(id);
            if (null != activeReplicator) {
                String ip = activeReplicator.getIp();
                int applierPort = activeReplicator.getApplierPort();
                logger.info("pollDetectReplicators cluster: {}, replicator: {}", id, ip + ":" + applierPort);
                switchListenReplicator(id, ip, applierPort);
            } else {
                logger.warn("pollDetectReplicators no master replicator, cluster: {}", id);
            }
        }
    }

    private Map<String, ReplicatorInfoDto> getMasterReplicatorInfoDtos(Map<String, Set<String>> dc2ReplicatorIps) {
        Map<String, Set<String>> region2dcsMapping = consoleConfig.getRegion2dcsMapping();
        Map<String, Set<String>> region2ReplicatorIps = Maps.newHashMap();
        region2dcsMapping.forEach((region, dcs) -> {
            Set<String> replicatorIps = Sets.newHashSet();
            dcs.forEach(dc -> {
                Set<String> ips = dc2ReplicatorIps.get(dc);
                if (!CollectionUtils.isEmpty(ips)) {
                    replicatorIps.addAll(ips);
                }
            });
            if (!CollectionUtils.isEmpty(replicatorIps)) {
                region2ReplicatorIps.put(region, replicatorIps);
            }
        });

        List<ReplicatorInfoDto> masterReplicators = Lists.newArrayList();
        region2ReplicatorIps.forEach((region, replicatorIps) -> {
            List<ReplicatorInfoDto> masterReplicatorsInRegion = resourceService.getMasterReplicatorsInRegion(region, Lists.newArrayList(replicatorIps));
            if (!CollectionUtils.isEmpty(masterReplicatorsInRegion)) {
                masterReplicators.addAll(masterReplicatorsInRegion);
            }
        });

        return masterReplicators.stream().collect(Collectors.toMap(ReplicatorInfoDto::getRegistryKey, Function.identity(), (k1, k2) -> k1));
    }

    protected void updateMasterReplicatorIfChange(String mhaName, String newReplicatorIp) {
        monitorMasterRExecutorService.submit(() -> {
            try {
                centralService.updateMasterReplicatorIfChange(new MhaReplicatorEntity(mhaName, newReplicatorIp));
            } catch (Throwable t) {
                logger.error("updateMasterReplicatorIfChange error mha:{},newRIp:{}", mhaName, newReplicatorIp, t);
            }
        });
    }

}
