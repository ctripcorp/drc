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
import com.ctrip.framework.drc.fetcher.activity.replicator.FetcherSlaveServer;
import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private Map<String, ReplicatorWrapper> replicatorWrappers = Maps.newHashMap();

    // key: id(aka registryKey)
    private Map<String, StaticDelayMonitorHolder> delayMonitorHolderMap = Maps.newConcurrentMap();

    private static final String MYSQL_DELAY_MESUREMENT = "fx.drc.delay.mysql";

    private static final String DRC_DELAY_MESUREMENT = "fx.drc.delay";

    private static final int INITIAL_DELAY = 5;

    private static final int PERIOD = 30;

    private static final String WARN = "warn";

    private static final String ERROR = "error";

    private static final String DEBUG = "debug";

    private static final String INFO= "info";

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
//    private ScheduledExecutorService checkReplicatorScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("chkMR");

    private ScheduledExecutorService updateListenReplicatorScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("update-listen-replicator-scheduledExecutorService");

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
                    if(SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getListenReplicatorSwitch())) {
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
        if(!SWITCH_STATUS_ON.equalsIgnoreCase(monitorTableSourceProvider.getListenReplicatorMonitorSwitch())) {
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

    @VisibleForTesting
    protected void startListenServer(String clusterId, ReplicatorWrapper replicatorWrapper) {
        if(delayMonitorHolderMap.get(clusterId) != null) {
            logger.error("[[monitor=delaylisten]] current listen already exists, cluster id is: {}", clusterId);
            return;
        }

        DelayMonitorSlaveConfig config = generateConfig(replicatorWrapper, DRC_DELAY_MESUREMENT);
        StaticDelayMonitorHolder delayMonitorHolder = new StaticDelayMonitorHolder(config, periodicalUpdateDbTask, consoleConfig.getDelayExceptionTime());
        try {
            delayMonitorHolder.initialize();
            delayMonitorHolder.start();
            delayMonitorHolderMap.put(clusterId, delayMonitorHolder);
            replicatorWrappers.put(clusterId, replicatorWrapper);
        } catch (Exception e) {
            log(config, "initialize and start error", ERROR, e);
        }
    }

    @VisibleForTesting
    protected void stopListenServer(String clusterId) {
        StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(clusterId);
        if(delayMonitorHolder == null) {
            logger.error("[[monitor=delaylisten]] current listen does not exist, cluster id is: {}", clusterId);
            return;
        }
        delayMonitorHolder.stopOldServer();
        delayMonitorHolderMap.remove(clusterId);
        replicatorWrappers.remove(clusterId);
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

        for(String added : comparator.getAdded()) {
            logger.info("[[monitor=delaylisten]] add replicator listen, cluster id is: {}", added);
            ReplicatorWrapper replicatorWrapperToAdd = future.get(added);
            startListenServer(added, replicatorWrapperToAdd);
        }

        for(String removed : comparator.getRemoved()) {
            logger.info("[[monitor=delaylisten]] remove replicator listen, cluster id is: {}", removed);
            stopListenServer(removed);
        }

        for(@SuppressWarnings("rawtypes") MetaComparator modifiedComparator : comparator.getMofified()) {
            ReplicatorWrapperComparator replicatorWrapperComparator = (ReplicatorWrapperComparator) modifiedComparator;
            String modified = replicatorWrapperComparator.getDbClusterId();
            ReplicatorWrapper newReplicatorWrapper = future.get(modified);

            logger.info("[[monitor=delaylisten]] modify replicator listen, cluster id is: {}", modified);
            stopListenServer(modified);
            startListenServer(modified, newReplicatorWrapper);
        }
    }

    private void pollDetectReplicators() {
        for(String id : delayMonitorHolderMap.keySet()) {
            StaticDelayMonitorHolder delayMonitorHolder = delayMonitorHolderMap.get(id);
            DelayMonitorSlaveConfig config = delayMonitorHolder.getConfig();
            logger.debug("request CM for real master R for {} in {}", id, config.getDestDc());
            Replicator activeReplicator = moduleCommunicationService.getActiveReplicator(config.getDestDc(), id);
            if(null != activeReplicator) {
                String ip = activeReplicator.getIp();
                Integer applierPort = activeReplicator.getApplierPort();
                String endpointStr = ip + ':' + applierPort;
                log(config, "realR ip:" + ip + ", port: " + applierPort + ", generate str: " + endpointStr, INFO, null);
                updateMasterReplicator(config, endpointStr);
                delayMonitorHolder.checkMaster(endpointStr);
            } else {
                log(config, "Fail to get realR(oldR is in tags), check next round", INFO, null);
            }
        }
    }

    protected void updateMasterReplicator(DelayMonitorSlaveConfig config, String endpointStr) {
        String[] split = endpointStr.split(":");
        if(split.length == 2) {
            String mhaName = config.getDestMha();
            String oldIp = config.getIp();
            String newIp = split[0];
            monitorMasterRExecutorService.submit(() -> {
                if(drcMaintenanceService.updateMasterReplicator(mhaName, newIp)) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.master", String.format("%s-%s", oldIp, newIp));
                    String measurement = "fx.drc.replicator.master";
                    BaseEntity baseEntity = new BaseEntity(0L, "unset", config.getDestDc(), config.getCluster(), mhaName, RegistryKey.from(config.getCluster(), config.getDestMha()));
                    DefaultReporterHolder.getInstance().reportResetCounter(baseEntity.getTags(), 1L, measurement);
                }
            });
        }
    }

    private static void log(DelayMonitorSlaveConfig config, String msg, String types, Exception e) {
        String prefix = new StringBuilder().append(CLOG_TAGS).append(msg).toString();
        switch (types) {
            case WARN:
                logger.warn(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(),config.getRouteInfo());
                break;
            case ERROR:
                logger.error(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(),config.getRouteInfo(), e);
                break;
            case DEBUG:
                logger.debug(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(),config.getRouteInfo());
                break;
            case INFO:
                logger.info(prefix, config.getMha(), config.getDc(), config.getDestMha(), config.getDestDc(), config.getCluster(), config.getEndpoint().getHost(), config.getEndpoint().getPort(), config.getMeasurement(),config.getRouteInfo());
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
        protected void doStart() throws Exception{
            super.doStart();
            startServer(config, periodicalUpdateDbTask, delayExceptionTime);
        }

        @Override
        protected void doStop() throws Exception{
            super.doStop();
            stopOldServer();
        }

        private void setServer(StaticDelayMonitorServer server) {
            this.server = server;
        }

        public DelayMonitorSlaveConfig getConfig() {
            return config;
        }

        /**
         * @param endpointStr the activeReplicator's endpoint as String, i.e. "ip:port"
         */
        public synchronized void checkMaster(String endpointStr) {
            String[] split = endpointStr.split(":");
            if(split.length == 2) {
                try {
                    String ip = split[0];
                    Integer port = Integer.parseInt(split[1]);
                    if(!(server.getConfig().getIp().equalsIgnoreCase(ip) && server.getConfig().getPort().equals(port))) {
                        log(config, "oldR(" + config.getIp() + ':'  + config.getPort() + ") unequals to realR(" + endpointStr + "), going to stop & dispose old DelayMonitor server", INFO, null);
                        stopOldServer();
                        Endpoint endpoint = new DefaultEndPoint(ip, port);
                        config.setEndpoint(endpoint);
                        startServer(config, periodicalUpdateDbTask, delayExceptionTime);
                    } else {
                        log(config, "oldR(" + config.getIp() + ':'  + config.getPort() + ") equals to realR(" + endpointStr + ')', INFO, null);
                    }
                } catch (NumberFormatException e) {
                    log(config, "incorrect endpoint(" + endpointStr + ')', ERROR, e);
                }
            } else {
                log(config, "incorrect endpoint(" + endpointStr + ')', INFO, null);
            }
        }

        private void stopOldServer() {
            DelayMonitorSlaveConfig config = server.getConfig();
            log(config, "stopping server", INFO,null);
            try {
                if (StringUtils.isNotBlank(config.getRouteInfo())) {
                    ProxyRegistry.unregisterProxy(config.getIp(), config.getPort());
                }
                server.stop();
                server.dispose();
                log(config, "sleep 5s to prove last DelayMonitor server is completely disposed.", INFO, null);
                Thread.sleep(5000);
                server = null;
            } catch (Exception e) {
                log(config, "stop server failure", ERROR, e);
            }
        }

        private void startServer(DelayMonitorSlaveConfig config, PeriodicalUpdateDbTask periodicalUpdateDbTask, long delayExceptionTime) {
            StaticDelayMonitorServer server = new StaticDelayMonitorServer(config, new DelayMonitorPooledConnector(config.getEndpoint()), periodicalUpdateDbTask, delayExceptionTime);
            try {
                log(config, "starting server", INFO, null);
                String routeInfo = config.getRouteInfo();
                if (StringUtils.isNotBlank(routeInfo)) {
                    ProxyRegistry.registerProxy(config.getIp(), config.getPort(), routeInfo);
                }
                server.initialize();
                server.start();
                this.setServer(server);
                log(config, "started server", INFO, null);
            } catch (Exception e) {
                log(config, "start server error", ERROR, e);
            }
        }
    }

    public Map<String, StaticDelayMonitorHolder> getDelayMonitorHolderMap() {
        return Collections.unmodifiableMap(delayMonitorHolderMap);
    }
    
    public void getMap(String ... s) {
        
    }
}
