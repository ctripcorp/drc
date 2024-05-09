package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.utils.IpUtils;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.healthcheck.service.task.DbClusterHeartbeatTask;
import com.ctrip.framework.drc.manager.healthcheck.service.task.MasterHeartbeatTask;
import com.ctrip.framework.drc.manager.healthcheck.tracker.HeartBeatTracker;
import com.ctrip.framework.drc.manager.healthcheck.tracker.HeartBeatTrackerImpl;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by mingdongli
 * 2019/11/21 下午3:12.
 */
@Component
public class MySQLHeartBeatImpl extends AbstractLifecycleObservable implements HeartBeat, HeartBeatTracker.HeartbeatExpirer, TopElement {

    private Set<Endpoint> masterMySQLSet = Sets.newConcurrentHashSet();

    private Map<Endpoint, DbCluster> dbsMap = Maps.newConcurrentMap();  // master down, should ping dbs

    private ScheduledExecutorService masterCheckScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("MySQLHeartBeatImpl-Scheduled-Check");

    private ListeningExecutorService masterExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-Master"));

    private ScheduledExecutorService dbClusterCheckScheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("MySQLHeartBeatImpl-Scheduled-Check");

    private ListeningExecutorService dbClusterExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-DbCluster"));

    private ExecutorService notifyExecutorService = ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-Notify");

    private HeartBeatTrackerImpl heartbeatTracker;

    private HeartBeatContext heartBeatContext;

    @Autowired
    private MySQLMasterManager mySQLMasterManager;

    public MySQLHeartBeatImpl() {
        heartBeatContext = new MySQLHeartBeatContext();
        heartbeatTracker = new HeartBeatTrackerImpl(heartBeatContext);
    }

    public MySQLHeartBeatImpl(HeartBeatContext heartBeatContext) {
        this.heartBeatContext = heartBeatContext;
        heartbeatTracker = new HeartBeatTrackerImpl(heartBeatContext);
    }

    @Override
    protected void doInitialize() throws Exception{
        heartbeatTracker.setExpirer(this);
        heartbeatTracker.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        startMasterScheduleTask();
        startZoneScheduleTask();
        heartbeatTracker.start();
    }

    @Override
    protected void doStop() throws Exception {
        masterCheckScheduledExecutorService.shutdown();
        masterExecutorService.shutdown();
        dbClusterCheckScheduledExecutorService.shutdown();
        dbClusterExecutorService.shutdown();
        heartbeatTracker.stop();
    }

    @Override
    public void pingMaster() {
        Set<Endpoint> copy = Sets.newHashSet(masterMySQLSet);
        for (Endpoint endpoint : copy) {
            doPingMaster(endpoint);
        }
    }

    @Override
    public void pingZone() {
        Map<Endpoint, DbCluster> copy = new HashMap<>(dbsMap);
        for (Map.Entry<Endpoint, DbCluster> entry : copy.entrySet()) {
            doPingDbCluster(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void addServer(Endpoint endpoint) {
        masterMySQLSet.add(endpoint);
        heartbeatTracker.addHeartbeat(endpoint, heartBeatContext.getLease());
        logger.info("[Add] {} to heartbeatTracker", endpoint);
    }

    @Override
    public void removeServer(Endpoint endpoint) {
        heartbeatTracker.removeHeartbeat(endpoint);
        masterMySQLSet.remove(endpoint);
    }

    @Override
    public void expire(List<Endpoint> downs) {
        for (Endpoint master : downs) {
            try {
                logger.info("[Expire] {} and put into dbsMap with size {}", master, dbsMap.size());
                DbCluster dbCluster = mySQLMasterManager.getDbs(master);
                masterMySQLSet.remove(master);
                DataSourceManager.getInstance().clearDataSource(master);
                dbsMap.put(master, dbCluster);  //transfer to dbsMap
            } catch (Exception e) {
                logger.error("[Expire] {} and put into dbsMap error", master, e);
            }
        }
    }

    @Override
    public String getServerId() {
        return IpUtils.getFistNonLocalIpv4ServerAddress();
    }

    private void startMasterScheduleTask() {
        masterCheckScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Cluster-Manager", "Master-Check", new Task() {
                        @Override
                        public void go() throws Exception {
                            pingMaster();
                        }
                    });
                } catch (Throwable t) {
                    logger.info("cluster manager check error", t);
                }
            }
        }, 0, heartBeatContext.getInterval(), TimeUnit.MILLISECONDS);
    }

    private void startZoneScheduleTask() {
        dbClusterCheckScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultTransactionMonitorHolder.getInstance().logTransaction("Cluster-Manager", "Zone-Check", new Task() {
                        @Override
                        public void go() throws Exception {
                            pingZone();
                        }
                    });
                } catch (Throwable t) {
                    logger.info("cluster manager check error", t);
                }
            }
        }, 0, heartBeatContext.getInterval(), TimeUnit.MILLISECONDS);
    }

    private void doPingMaster(Endpoint endpoint) {
        ListenableFuture<Boolean> listenableFuture =  masterExecutorService.submit(new MasterHeartbeatTask(endpoint));
        Futures.addCallback(listenableFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                if (Boolean.TRUE.equals(result)) {
                    if(!heartbeatTracker.touchHeartbeat(endpoint, heartBeatContext.getLease())) {
                        heartbeatTracker.addHeartbeat(endpoint, heartBeatContext.getLease());
                    }
                }
                logger.info("[Ping] Master {} successfully with result {}", endpoint, result);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("[Ping] Master {} error", endpoint);
            }
        }, MoreExecutors.directExecutor());
    }

    private void doPingDbCluster(Endpoint endpoint, DbCluster dbCluster) {
        ListenableFuture<DbCluster> listenableFuture =  dbClusterExecutorService.submit(new DbClusterHeartbeatTask(endpoint, dbCluster));
        Futures.addCallback(listenableFuture, new FutureCallback<DbCluster>() {
            @Override
            public void onSuccess(DbCluster result) {
                Dbs dbs = result.getDbs();
                List<Db> dbList = dbs.getDbs();
                Endpoint newMaster = null;
                boolean multiMaster = false;
                for (Db db : dbList) {
                    if (!db.isMaster()) {
                        if (db.getIp().equalsIgnoreCase(endpoint.getHost())) {
                            dbs.setPreviousMaster(db.getUuid());
                        }
                        continue;
                    }
                    if (newMaster != null) {
                        multiMaster = true;
                        DefaultEventMonitorHolder.getInstance().logEvent("Cluster-Manager", "Multi-Master");  //alarm
                        logger.error("[Multi-Master] found for cluster {}", dbs);
                    } else {
                        newMaster = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                    }
                }

                if (!multiMaster && newMaster != null) {
                    if (!endpoint.equals(newMaster)) {
                        Endpoint finalNewMaster = newMaster;
                        notifyExecutorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                mySQLMasterManager.updateDbs(finalNewMaster, dbCluster);  // update and notify
                            }
                        });
                        logger.info("[Ping] NEW Master {} successfully and remove it", dbs);
                    } else {
                        logger.info("[Ping] OLD same Master {} successfully and remove it", endpoint);
                    }
                    transferFromZoneToMaster(endpoint, newMaster);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("[Ping] DbCluster {} error", endpoint, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private void transferFromZoneToMaster(Endpoint oldMaster, Endpoint newMaster) {
        dbsMap.remove(oldMaster);
        addServer(newMaster);
    }

    public Set<Endpoint> getMasterMySQLSet() {
        return Collections.unmodifiableSet(masterMySQLSet);
    }
}
