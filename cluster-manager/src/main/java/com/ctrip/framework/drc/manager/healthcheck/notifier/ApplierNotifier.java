package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.meta.InstanceInfo;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.ha.meta.CurrentMetaManager;
import com.ctrip.framework.drc.manager.ha.meta.impl.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.manager.healthcheck.service.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.manager.utils.SpringUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.NOTIFY_LOGGER;

/**
 * Created by mingdongli
 * 2019/11/23 下午10:57.
 */
public class ApplierNotifier extends AbstractNotifier implements Notifier {

    private static final String URL_PATH = "appliers";

    private CurrentMetaManager currentMetaManager;

    private ListeningExecutorService gtidQueryExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("ApplierNotifier-ExecutedGtidQuery"));

    private ApplierNotifier() {
        super();
        initMetaManager();
    }

    private void initMetaManager() {
        try {
            ApplicationContext applicationContext = SpringUtils.getApplicationContext();
            if (applicationContext != null) {
                currentMetaManager = applicationContext.getBean(DefaultCurrentMetaManager.class);
            }
        } catch (Exception e) {
            logger.error("initMetaManager error", e);
        }
    }

    private static class ApplierNotifierHolder {
        public static final ApplierNotifier INSTANCE = new ApplierNotifier();
    }

    public static ApplierNotifier getInstance() {
        return ApplierNotifierHolder.INSTANCE;
    }

    @Override
    public void notify(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        for (Db db : dbs.getDbs()) {
            if (db.isMaster()) {
                NOTIFY_LOGGER.info("[Gtid] query for restart {} begin", db.getIp());
                Endpoint newMaster = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                ListenableFuture<String> listenableFuture =  gtidQueryExecutorService.submit(new ExecutedGtidQueryTask(newMaster));
                Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                    @Override
                    public void onSuccess(String gtid) {
                        List<Applier> appliers = dbCluster.getAppliers();
                        appliers.forEach(applier -> applier.setGtidExecuted(gtid));
                        ApplierNotifier.super.notify(dbCluster);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        NOTIFY_LOGGER.error("[Query] new master executed gtid error", t);
                    }
                });
            }
        }
    }

    @Override
    public void notifyAdd(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        for (Db db : dbs.getDbs()) {
            if (db.isMaster()) {
                NOTIFY_LOGGER.info("[Gtid] query for add {} begin", db.getIp());
                Endpoint newMaster = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                ListenableFuture<String> listenableFuture =  gtidQueryExecutorService.submit(new ExecutedGtidQueryTask(newMaster));
                Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                    @Override
                    public void onSuccess(String gtid) {
                        List<Applier> appliers = dbCluster.getAppliers();
                        appliers.forEach(applier -> applier.setGtidExecuted(gtid));
                        NOTIFY_LOGGER.info("[ExecutedGtid] is set to {}", gtid);
                        ApplierNotifier.super.notifyAdd(dbCluster);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        NOTIFY_LOGGER.error("[Query] new master executed gtid error", t);
                    }
                });
            }
        }
    }

    @Override
    protected String getUrlPath() {
        return URL_PATH;
    }

    @Override
    protected Object getBody(String ipAndPort, DbCluster dbCluster, boolean register) {
        ApplierConfigDto config = new ApplierConfigDto();
        config.target = new DBInfo();
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        for (Db db : dbList) {
            if (db.isMaster()) {
                config.target.ip = db.getIp();
                config.target.port = db.getPort();
                config.target.uuid = db.getUuid();
                break;
            }
        }
        config.target.username = dbs.getWriteUser();
        config.target.password = dbs.getWritePassword();
        config.target.cluster = dbCluster.getName();
        config.target.mhaName = dbCluster.getMhaName();
        config.target.idc = System.getProperty(DefaultFoundationService.DATA_CENTER_KEY, "unknown");

        //incorrect naming, should use 'sourceIdc'.
        String targetIdc = null;
        String targetName = null;
        String replicatorMhaName = null;
        for (Applier applier : dbCluster.getAppliers()) {
            if (ipAndPort.equals(applier.getIp() + ":" + applier.getPort())) {
                config.ip = applier.getIp();
                config.port = applier.getPort();
                config.setGtidExecuted(applier.getGtidExecuted());
                config.setIncludedDbs(applier.getIncludedDbs());
                config.setApplyMode(applier.getApplyMode());
                config.setNameFilter(applier.getNameFilter());
                Route route = getRoute(dbCluster.getId(), applier.getTargetIdc());
                config.setRouteInfo(route == null ? "" : (route.routeProtocol() + " " + ProxyEndpoint.PROXY_SCHEME.TCP.name()));
                targetIdc = applier.getTargetIdc();
                targetName = applier.getTargetName();
                replicatorMhaName = applier.getTargetMhaName();
            }
        }
        if (targetIdc == null) {
            throw new RuntimeException("source idc not found");
        } else {
            NOTIFY_LOGGER.info("notify - source idc: " + targetIdc);
        }
        config.replicator = new InstanceInfo();

        Replicator replicator = null;
        List<Replicator> replicators = dbCluster.getReplicators();
        if (!replicators.isEmpty()) {
            replicator = replicators.get(0);
        }

        if (!register && replicator == null) {
            throw new RuntimeException("replicator not found.");
        } else {
            NOTIFY_LOGGER.info("notify - replicator: " + replicator);
        }

        if (!register) {
            config.replicator.ip = replicator.getIp();
            config.replicator.port = replicator.getApplierPort();
            config.replicator.name = targetName;
            config.replicator.cluster = dbCluster.getName();
            config.replicator.idc = "unknown";
        }
        config.replicator.mhaName = replicatorMhaName;

        config.idc = "unknown";  //TODO get right idc
        config.cluster = dbCluster.getName();
        config.name = config.cluster + "-applier";

        return config;
    }

    @Override
    protected List<String> getDomains(DbCluster dbCluster) {
        if (dbCluster.getAppliers() == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(dbCluster.getAppliers().stream().map(
                applier -> applier.getIp() + ":" + applier.getPort()
        ).collect(Collectors.toList()));
    }

    @VisibleForTesting
    protected Route getRoute(String clusterId, String dstDc) {
        try {
            return currentMetaManager.randomRoute(clusterId, dstDc);
        } catch (Throwable t) {
            logger.warn("[ApplierNotifier]Fail get route for {}->{}", clusterId, dstDc, t);
            return null;
        }
    }

    @VisibleForTesting
    public void setCurrentMetaManager(CurrentMetaManager currentMetaManager) {
        this.currentMetaManager = currentMetaManager;
    }
}
