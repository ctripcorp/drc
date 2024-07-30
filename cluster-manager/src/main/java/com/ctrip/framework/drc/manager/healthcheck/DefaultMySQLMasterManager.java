package com.ctrip.framework.drc.manager.healthcheck;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.config.RegistryKey;
import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.ha.cluster.impl.AbstractCurrentMetaObserver;
import com.ctrip.framework.drc.manager.ha.meta.comparator.ClusterComparator;
import com.ctrip.framework.drc.manager.ha.meta.comparator.DbComparator;
import com.ctrip.framework.drc.manager.healthcheck.service.task.DbClusterUuidQueryTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * add to heartbeat of mysql
 * @Author limingdong
 * @create 2020/5/19
 */
@Component
public class DefaultMySQLMasterManager extends AbstractCurrentMetaObserver implements MySQLMasterManager<List<String>>, TopElement {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ListeningExecutorService dbClusterExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newCachedThreadPool("MySQLHeartBeatImpl-DbCluster"));

    private Map<Endpoint, DbCluster> dbsMap = Maps.newConcurrentMap();

    private Map<String, Endpoint> clusterId2Endpoint = Maps.newConcurrentMap();

    @Autowired
    private HeartBeat heartBeat;

    private CallBack defaultCallBack = new CallBack() {
        @Override
        public ListenableFuture<List<String>> onAddDbs(String clusterId, DbCluster dbCluster) {
            Dbs dbs = dbCluster.getDbs();
            List<Db> dbList = dbs.getDbs();
            ListenableFuture<List<String>> listenableFuture = dbClusterExecutorService.submit(new DbClusterUuidQueryTask(null, dbs));
            Futures.addCallback(listenableFuture, new FutureCallback<List<String>>() {
                @Override
                public void onSuccess(List<String> result) {
                    Endpoint master = null;
                    for (int i = 0; i < dbList.size(); ++i) {
                        Db db = dbList.get(i);
                        if (StringUtils.isBlank(db.getUuid()) && result.size() > i) {
                            db.setUuid(result.get(i));
                        }
                        if (db.isMaster()) {
                            master = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                        }
                    }
                    if (master == null) {
                        throw new IllegalStateException("[No] master find for cluster " + dbs);
                    }
                    dbsMap.put(master, dbCluster);
                    clusterId2Endpoint.put(clusterId, master);
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

    @Override
    public DbCluster getDbs(Endpoint endpoint) {
        DbCluster dbCluster = dbsMap.get(endpoint);
        if (dbCluster == null) {
            return null;
        }

        return MetaClone.clone(dbCluster);
    }

    @Override
    public void updateDbs(Endpoint endpoint, DbCluster dbCluster) {
        if (currentMetaManager.hasCluster(dbCluster.getId())) {
            clusterId2Endpoint.put(dbCluster.getId(), endpoint);
            DbCluster old = dbsMap.put(endpoint, dbCluster);
            logger.info("[Update] {} DefaultMySQLMasterManager to {}", old, dbCluster);
            DefaultTransactionMonitorHolder.getInstance().logTransactionSwallowException("DRC.cm.switch.mysql.master", dbCluster.getMhaName(), () -> {
                currentMetaManager.switchMySQLMaster(dbCluster.getId(), endpoint);
            });
        } else {
            logger.info("[MySQL] master change, not interest. {}:{}", endpoint, dbCluster);
        }
    }

    @Override
    public DbCluster getDbs(String clusterId) {
        RegistryKey registryKey = RegistryKey.from(clusterId);
        logger.info("[Fetch] RegistryKey for {} to {}", clusterId, registryKey);
        Endpoint endpoint = currentMetaManager.getMySQLMaster(clusterId);
        if (endpoint != null) {
            return dbsMap.get(endpoint);
        }
        return null;
    }

    @Override
    public Map<Endpoint, DbCluster> getAllDbs() {
        return Maps.newHashMap(dbsMap);
    }

    @Override
    public void removeDbs(String clusterId, Endpoint endpoint) {
        logger.info("[Master] of {} is removed, endpoint {}", clusterId, endpoint);
        dbsMap.remove(endpoint);
        clusterId2Endpoint.remove(clusterId);
        heartBeat.removeServer(endpoint);
    }

    private ListenableFuture<List<String>> addDbs(String clusterId, DbCluster dbCluster) {
        return defaultCallBack.onAddDbs(clusterId, dbCluster);
    }

    @Override
    protected void handleClusterModified(ClusterComparator comparator) {
        DbComparator dbComparator = comparator.getDbComparator();
        int changeCount = dbComparator.totalChangedCount();
        if (changeCount > 0) {
            logger.info("[DbComparator] changeCount {} for {}", changeCount, dbComparator.idDesc());
            DbCluster dbCluster = dbComparator.getFuture();
            handleClusterDeleted(dbCluster);
            handleClusterAdd(dbCluster);
            Pair<Boolean, Db> res = masterSwitch(dbComparator);
            if (res.getKey()) {
                Db db = res.getValue();
                Dbs dbs = db.parent();
                DefaultEndPoint newMaster = new DefaultEndPoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword());
                logger.info("[switchMySQLMaster] update to {}", newMaster);
                currentMetaManager.switchMySQLMaster(dbCluster.getId(), newMaster);
            }
        }
    }

    @Override
    protected void handleClusterDeleted(DbCluster clusterMeta) {
        Endpoint endpoint = clusterId2Endpoint.get(clusterMeta.getId());
        if (endpoint != null) {
            removeDbs(clusterMeta.getId(), endpoint);
        }
    }

    @Override
    protected void handleClusterAdd(DbCluster clusterMeta) {
        addDbs(clusterMeta.getId(), clusterMeta);
    }

    private Pair<Boolean, Db> masterSwitch(DbComparator dbComparator) {
        DbCluster current = dbComparator.getCurrent();
        DbCluster future = dbComparator.getFuture();

        Db currentDb = getMaster(current);
        Db futureDb = getMaster(future);
        if (futureDb != null && !futureDb.equals(currentDb)) {
            return Pair.from(true, futureDb);
        }
        return Pair.from(false, null);
    }

    private Db getMaster(DbCluster dbCluster) {
        if (dbCluster != null) {
            Dbs dbs = dbCluster.getDbs();
            if (dbs != null) {
                List<Db> dbList = dbs.getDbs();
                for (Db db : dbList) {
                    if (db.isMaster()) {
                        return db;
                    }
                }
            }
        }
        return null;
    }
}
