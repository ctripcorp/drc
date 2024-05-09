package com.ctrip.framework.drc.manager.healthcheck.service;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.healthcheck.HeartBeat;
import com.ctrip.framework.drc.manager.healthcheck.service.task.DbClusterUuidQueryTask;
import com.ctrip.framework.drc.manager.healthcheck.service.task.SlaveFetcherQueryTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mingdongli
 * 2019/11/21 下午9:45.
 */
public class DefaultClusterService extends AbstractLifecycle implements ClusterService<List<String>> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ListeningExecutorService dbClusterExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-DbCluster"));

    private ScheduledExecutorService slaveUpdateExecutorService = ThreadUtils.newSingleThreadScheduledExecutor("MySQLHeartBeatImpl-SlaveUpdate");

    private Map<Endpoint, DbCluster> dbClusterMap = Maps.newConcurrentMap();

    private AtomicReference<Drc> drcAtomicReference = new AtomicReference<>();

    private HeartBeat heartBeat;

    private ScheduledFuture scheduledFuture;

    private CallBack defaultCallBack = new CallBack() {
        @Override
        public ListenableFuture<List<String>> onAddDbCluster(DbCluster dbCluster) {
            Dbs dbs = dbCluster.getDbs();
            List<Db> dbList = dbs.getDbs();
            ListenableFuture<List<String>> listenableFuture =  dbClusterExecutorService.submit(new DbClusterUuidQueryTask(null, dbCluster.getDbs()));
            Futures.addCallback(listenableFuture, new FutureCallback<List<String>>() {
                @Override
                public void onSuccess(List<String> result) {
                    Endpoint master = null;
                    for (int i = 0; i < dbList.size(); ++i) {
                        Db db = dbList.get(i);
                        db.setUuid(result.get(i));
                        if (db.isMaster()) {
                            master = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                        }
                    }
                    if (master == null) {
                        throw new IllegalStateException("[No] master find for cluster " + dbCluster.getName());
                    }
                    dbClusterMap.put(master, dbCluster);
                    heartBeat.addServer(master);
                    logger.info("[Add] {} to dbClusterMap and heartBeat", master);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("[Ping] DbCluster uuid {} error", dbs, t);
                }
            }, MoreExecutors.directExecutor());

            return listenableFuture;
        }
    };

    private DefaultClusterService() {

    }


    private static class DefaultClusterServiceHolder {
        public static final DefaultClusterService INSTANCE = new DefaultClusterService();
    }

    public static DefaultClusterService getInstance() {
        return DefaultClusterServiceHolder.INSTANCE;
    }

    @Override
    public DbCluster getDbCluster(Endpoint endpoint) {
        return dbClusterMap.get(endpoint);
    }

    @Override
    public DbCluster getDbCluster(String registryPath) {
        RegistryKey registryKey = RegistryKey.from(registryPath);
        logger.info("[Fetch] RegistryKey for {} to {}", registryPath, registryKey);
        DbCluster dbCluster = dbClusterMap.values().stream().filter(u -> u.getName() != null && u.getName().equalsIgnoreCase(registryKey.getClusterName()) && u.getMhaName() != null && u.getMhaName().equalsIgnoreCase(registryKey.getMhaName())).findFirst().orElse(null);
        return dbCluster;
    }

    @Override
    public ListenableFuture<List<String>> addDbCluster(DbCluster dbCluster) {
        return defaultCallBack.onAddDbCluster(dbCluster);
    }

    @Override
    public void updateDbCluster(Endpoint master, DbCluster dbCluster) {
        dbClusterMap.put(master, dbCluster);
    }

    @Override
    public DbCluster removeDbCluster(Endpoint endpoint) {
        return dbClusterMap.remove(endpoint);
    }

    @Override
    public void updateDrc(Drc drc) {
        this.drcAtomicReference.set(drc);
    }

    @Override
    public Drc getDrc() {
        return drcAtomicReference.get();
    }

    @Override
    protected void doStart() throws Exception {
        scheduledFuture = slaveUpdateExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    doUpdateSlaves();
                } catch (Throwable t) {
                    logger.error("doUpdateSlaves error", t);
                }
            }
        }, 10, 60, TimeUnit.MINUTES);
    }

    private void doUpdateSlaves() {
        Map<Endpoint, DbCluster> copy = Maps.newHashMap(dbClusterMap);
        for (Map.Entry<Endpoint, DbCluster> entry : copy.entrySet()) {
            SlaveFetcherQueryTask slaveFetcherQueryTask = new SlaveFetcherQueryTask(entry.getKey());
            List<String> slaves = slaveFetcherQueryTask.call();

        }
    }

    protected void doStop() throws Exception{
        scheduledFuture.cancel(true);
    }

    public void setHeartBeat(HeartBeat heartBeat) {
        this.heartBeat = heartBeat;
    }
}
