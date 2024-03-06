package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.MachineService;
import com.ctrip.framework.drc.console.service.v2.MonitorServiceV2;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.Db;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dbs;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName CacheMetaServiceImpl
 * @Author haodongPan
 * @Date 2023/7/28 17:52
 * @Version: $
 * @Description: aggregate all kinds of meta info by meta drc cache
 */
@Service
public class CacheMetaServiceImpl implements CacheMetaService {
    
    @Autowired private MetaProviderV2 metaProviderV2;

    @Autowired private DefaultConsoleConfig consoleConfig;

    @Autowired private MonitorServiceV2 monitorServiceV2;
    @Autowired private MachineService machineService;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Map<String, List<ReplicatorWrapper>> getAllReplicatorsInLocalRegion() {
        Map<String, List<ReplicatorWrapper>> replicators = Maps.newHashMap();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            replicators.putAll(getAllReplicatorInDc(dcInLocalRegion));
        }
        return replicators;
    }

    @Override
    public Map<String, ReplicatorWrapper> getMasterReplicatorsToBeMonitored(List<String> mhaNamesToBeMonitored) {
        Map<String, ReplicatorWrapper> replicators = Maps.newHashMap();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            replicators.putAll(getReplicatorsSrcDcRelated(mhaNamesToBeMonitored,dcInLocalRegion));
        }
        return replicators;
    }

    /**
     * dcNames: dcs in local region
     * key: local Mha
     * value: master db's uuid set which are not in local dc, i.e. all potential uuids which will be copied into local mha
     */
    @Override
    public Map<String, Set<String>> getMha2UuidsMap(Set<String> dcNames) {
        Map<String, Set<String>> uuidMap = Maps.newHashMap();
        Drc drc = metaProviderV2.getDrc();

        for (String localDcName : dcNames) {
            List<DbCluster> localDbClusters = Lists.newArrayList(drc.findDc(localDcName).getDbClusters().values());

            for(DbCluster dbCluster :  localDbClusters) {
                String localMhaName = dbCluster.getMhaName();
                for (Applier applier : dbCluster.getAppliers()) {
                    DbCluster remoteDbCluster = getRemoteDbCluster(drc, applier);
                    List<Db> dbList = remoteDbCluster.getDbs().getDbs();
                    for (Db db : dbList) {
                        // ali dc uuid is not only
                        String uuidString = db.getUuid();
                        String[] uuids = uuidString.split(",");
                        Set<String> uuidSet = uuidMap.getOrDefault(localMhaName, Sets.newHashSet());
                        for (String uuid : uuids) {
                            uuidSet.add(uuid);
                            logger.info("[getUuidMap] localMhaName {},opposite db(isMaster:{}) uuid contain {}",
                                    localMhaName, db.isMaster(), uuid);
                        }
                        uuidMap.put(localMhaName, uuidSet);
                    }
                }
            }
        }
        return uuidMap;
    }


    /**
     * dcNames: dcs in local region
     * key: local Mha
     * value: master db's uuid set which are not in local dc, i.e. all potential uuids which will be copied into local mha
     */
    @Override
    public Map<String, Map<String, Set<String>>> getMhaDbUuidsMap(Set<String> dcNames, Drc drc) {
        Map<String, Map<String, Set<String>>> MhaDbUUidMap = Maps.newHashMap();
        for (String localDcName : dcNames) {
            List<DbCluster> localDbClusters = Lists.newArrayList(drc.findDc(localDcName).getDbClusters().values());

            for (DbCluster dbCluster : localDbClusters) {
                String localMhaName = dbCluster.getMhaName();
                List<Applier> appliers = dbCluster.getAppliers().stream()
                        .filter(e -> e.getApplyMode() != null && e.getApplyMode() == ApplyMode.db_transaction_table.getType())
                        .collect(Collectors.toList());
                for (Applier applier : appliers) {
                    DbCluster remoteDbCluster = getRemoteDbCluster(drc, applier);
                    List<Db> remoteDbs = remoteDbCluster.getDbs().getDbs();
                    String dbName = applier.getIncludedDbs();
                    Map<String, Set<String>> dbUuidMap = MhaDbUUidMap.computeIfAbsent(localMhaName, k -> Maps.newHashMap());
                    for (Db db : remoteDbs) {
                        // ali dc uuid is not only
                        String[] uuids = db.getUuid().split(",");
                        for (String uuid : uuids) {
                            dbUuidMap.computeIfAbsent(dbName, k -> Sets.newHashSet()).add(uuid);
                        }
                    }
                }
            }
        }
        return MhaDbUUidMap;
    }

    private static DbCluster getRemoteDbCluster(Drc drc, Applier applier) {
        String remoteDc = applier.getTargetIdc();
        String remoteCluster = applier.getTargetName();
        String remoteMha = applier.getTargetMhaName();
        String remoteDbClusterId = remoteCluster + "." + remoteMha;
        return drc.findDc(remoteDc).findDbCluster(remoteDbClusterId);
    }

    @Override
    public MonitorMetaInfo getMonitorMetaInfo() throws SQLException {
        List<String> mhaNamesToBeMonitored = monitorServiceV2.getMhaNamesToBeMonitored();
        return getMonitorMetaInfo(Sets.newHashSet(mhaNamesToBeMonitored));
    }

    @Override
    public MonitorMetaInfo getDstMonitorMetaInfo() throws SQLException {
        List<String> mhaNamesToBeMonitored = monitorServiceV2.getDestMhaNamesToBeMonitored();
        return getMonitorMetaInfo(Sets.newHashSet(mhaNamesToBeMonitored));
    }

    private MonitorMetaInfo getMonitorMetaInfo(Set<String> mhaNamesToBeMonitored) {
        MonitorMetaInfo monitorMetaInfo = new MonitorMetaInfo();
        Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint = Maps.newConcurrentMap();
        Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint= Maps.newConcurrentMap();
        Map<MetaKey, Endpoint> masterReplicatorEndpoint = Maps.newConcurrentMap();
        monitorMetaInfo.setMasterMySQLEndpoint(masterMySQLEndpoint);
        monitorMetaInfo.setSlaveMySQLEndpoint(slaveMySQLEndpoint);
        monitorMetaInfo.setMasterReplicatorEndpoint(masterReplicatorEndpoint);

        try {
            Drc drc = metaProviderV2.getDrc();
            if(drc == null) {
                logger.info("[getMonitorMetaInfo] return drc null");
                throw new RuntimeException("get drc fail");
            }
            for(Dc dc : drc.getDcs().values()) {
                for(DbCluster dbCluster : dc.getDbClusters().values()) {
                    String mhaName = dbCluster.getMhaName();
                    if(!mhaNamesToBeMonitored.contains(mhaName)) {
                        continue;
                    }
                    MetaKey metaKey = new MetaKey.Builder()
                            .dc(dc.getId())
                            .clusterId(dbCluster.getId())
                            .clusterName(dbCluster.getName())
                            .mhaName(dbCluster.getMhaName())
                            .build();

                    Replicator masterReplicator = dbCluster.getReplicators().stream()
                            .filter(Replicator::isMaster).findFirst()
                            .orElse(dbCluster.getReplicators().stream().findFirst().orElse(null));
                    Dbs dbs = dbCluster.getDbs();
                    String monitorUser = dbs.getMonitorUser();
                    String monitorPassword = dbs.getMonitorPassword();
                    List<Db> dbList = dbs.getDbs();
                    Db masterDb = dbList.stream()
                            .filter(Db::isMaster).findFirst().orElse(null);
                    Db slaveDb = dbList.stream()
                            .filter(db -> !db.isMaster()).findFirst().orElse(null);

                    if(masterReplicator != null) {
                        masterReplicatorEndpoint.put(metaKey, new DefaultEndPoint(masterReplicator.getIp(), masterReplicator.getApplierPort()));
                        logger.info("[META] one masterReplicatorEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),masterReplicator.getIp(), masterReplicator.getApplierPort());
                    } else {
                        logger.warn("[NO META] no master replicator for: {}", metaKey);
                    }
                    if(masterDb != null) {
                        masterMySQLEndpoint.put(metaKey, new MySqlEndpoint(masterDb.getIp(), masterDb.getPort(), monitorUser, monitorPassword, true));
                        logger.info("[META] one masterMySQLEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),masterDb.getIp(), masterDb.getPort());
                    } else {
                        logger.warn("[NO META] no master mysql for: {}", metaKey);
                    }
                    if(slaveDb != null) {
                        slaveMySQLEndpoint.put(metaKey, new MySqlEndpoint(slaveDb.getIp(), slaveDb.getPort(), monitorUser, monitorPassword, false));
                        logger.info("[META] one slaveMySQLEndpoint mhaName is {},ipPort is {}:{}",metaKey.getMhaName(),slaveDb.getIp(), slaveDb.getPort());
                    } else {
                        logger.warn("[NO META] no slave mysql for: {}", metaKey);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("getMonitorMetaInfo fail:{} ", e.getMessage());
            throw e;
        }
        return monitorMetaInfo;
    }

    @Override
    public Endpoint getMasterEndpoint(String mha) {
        Map<String, Dc> dcs = getDcs();
        for(Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                return getMaster(dbCluster);
            }
        }
        return machineService.getMasterEndpointCached(mha);
    }

    @Override
    public Endpoint getMasterEndpointForWrite(String mha) {
        Map<String, Dc> dcs = getDcs();
        for(Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                return getMasterForWrite(dbCluster);
            }
        }
        return null;
    }

    @Override
    public List<Endpoint> getMasterEndpointsInAllAccounts(String mha) {
        Map<String, Dc> dcs = getDcs();
        for(Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                return getAllAccountsMaster(dbCluster);
            }
        }
        return null;
    }
    
    @Override
    public Set<String> getSrcMhasShouldMonitor(String dbClusterId, String srcRegion) {
        Set<String> res = Sets.newHashSet();
        DbCluster dbCluster = metaProviderV2.getDcBy(dbClusterId).findDbCluster(dbClusterId);
        dbCluster.getAppliers().forEach(applier -> {
            if(applier.getTargetRegion().equalsIgnoreCase(srcRegion)) {
                res.add(applier.getTargetMhaName());
            }
        });
        return res;
    }

    @Override
    public boolean refreshMetaCache() {
        metaProviderV2.scheduledTask();
        return true;
    }

    public List<Endpoint> getAllAccountsMaster(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        List<Endpoint> endpoints = Lists.newArrayList();
        for(Db db : dbList) {
            if(db.isMaster()) {
                endpoints.add(new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getReadUser(), dbs.getReadPassword(), BooleanEnum.TRUE.isValue()));
                endpoints.add(new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getWriteUser(), dbs.getWritePassword(), BooleanEnum.TRUE.isValue()));
                return endpoints;
            }
        }
        return null;
    }

    public Map<String, Dc> getDcs() {
        return metaProviderV2.getDrc() != null ? metaProviderV2.getDrc().getDcs() : Maps.newLinkedHashMap();
    }

    public Endpoint getMaster(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        for(Db db : dbList) {
            if(db.isMaster()) {
                return new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }

    public Endpoint getMasterForWrite(DbCluster dbCluster) {
        Dbs dbs = dbCluster.getDbs();
        List<Db> dbList = dbs.getDbs();
        for(Db db : dbList) {
            if(db.isMaster()) {
                return new MySqlEndpoint(db.getIp(), db.getPort(), dbs.getWriteUser(), dbs.getWritePassword(), BooleanEnum.TRUE.isValue());
            }
        }
        return null;
    }

    private Map<String, ReplicatorWrapper> getReplicatorsSrcDcRelated(List<String> mhaNamesToBeMonitored, String srcDc) {
        Map<String, ReplicatorWrapper> replicators = Maps.newHashMap();
        Map<String, Dc> dcs = metaProviderV2.getDrc().getDcs();
        HashSet<String> mhasRelated = Sets.newHashSet(mhaNamesToBeMonitored);
        for (Dc dc : dcs.values()) {
            String dcName = dc.getId();
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            for (DbCluster dbCluster : dbClusters.values()) {
                List<Applier> appliers = dbCluster.getAppliers();
                for (Applier applier : appliers) {
                    if (srcDc.equals(applier.getTargetIdc()) && mhasRelated.contains(applier.getTargetMhaName())) {

                        if (dbCluster.getReplicators().isEmpty()) {
                            break;
                        }
                        // get Routes
                        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
                        List<Route> routes = Lists.newArrayList();
                        for (String dcInLocalRegion : dcsInLocalRegion) {
                            routes.addAll(RouteUtils.filterRoutes(
                                    dcInLocalRegion, Route.TAG_CONSOLE, dbCluster.getOrgId(), dcName, dcs.get(dcInLocalRegion)
                            ));
                        }
                        replicators.put(
                                dbCluster.getId(),
                                new ReplicatorWrapper(
                                        dbCluster.getReplicators().
                                                stream().filter(Replicator::isMaster).
                                                findFirst().orElse(dbCluster.getReplicators().get(0)),
                                        srcDc,
                                        dcName,
                                        dbCluster.getName(),
                                        applier.getTargetMhaName(),
                                        dbCluster.getMhaName(),
                                        routes
                                )
                        );
                        break;
                    }
                }
            }
        }
        return replicators;
    }

    private Map<String,List<ReplicatorWrapper>> getAllReplicatorInDc(String dcInRegion) {
        Map<String, List<ReplicatorWrapper>> replicators = Maps.newHashMap();
        Dc dc = metaProviderV2.getDrc().findDc(dcInRegion);
        String dcName = dc.getId();
        for (DbCluster dbCluster : dc.getDbClusters().values()) {
            String mhaName = dbCluster.getMhaName();
            List<ReplicatorWrapper> rWrappers = Lists.newArrayList();
            for (Replicator replicator: dbCluster.getReplicators()) {
                ReplicatorWrapper rWrapper = new ReplicatorWrapper(
                        replicator,
                        dcName,
                        dcName,
                        dbCluster.getName(),
                        mhaName,
                        mhaName,
                        Lists.newArrayList());
                rWrappers.add(rWrapper);
            }
            if (!CollectionUtils.isEmpty(rWrappers)) {
                replicators.put(dbCluster.getId(), rWrappers);
            }
        }
        return replicators;
    }
}
