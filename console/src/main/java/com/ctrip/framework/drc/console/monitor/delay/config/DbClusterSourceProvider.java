package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.AbstractMonitor;
import com.ctrip.framework.drc.console.pojo.ReplicatorMonitorWrapper;
import com.ctrip.framework.drc.console.pojo.ReplicatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.server.utils.RouteUtils;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.PriorityOrdered;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-12
 * analysis of drc.xml
 */
@Component("dbClusterSourceProvider")
public class DbClusterSourceProvider extends AbstractMonitor implements PriorityOrdered {

    @Autowired
    private DataCenterService dataCenterService;

    @Autowired
    private CompositeConfig compositeConfig;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    private static final String DRC_DB_CLUSTER = "drc.dbclusters";

    protected volatile String drcString;

    protected volatile Drc drc;

    // deprecated
    protected Drc drcFromQConfig;

    protected String localDc;

    private List<Set<Mha>> mhaGroups;

    private Set<Mha> allMhas;

    private List<List<Mha>> allMhaCombinationList;

    @Override
    public void initialize() {
        super.initialize();
        setInitialDelay(0);
    }

    @Override // refresh when new config submit
    public synchronized void scheduledTask() {
        compositeConfig.updateConfig();
        String newDrcString = compositeConfig.getConfig();
        if (StringUtils.isNotBlank(newDrcString) && !newDrcString.equalsIgnoreCase(drcString)) {
            drcString = newDrcString;
            try {
                drc = DefaultSaxParser.parse(drcString);
            } catch (Throwable t) {
                logger.error("[Parser] config {} error", drcString);
            }
        }
    }

    public Dc getDcBy(String dbClusterId) {
        Drc drc = getDrc();
        Map<String, Dc> dcs = drc.getDcs();
        for (Entry<String, Dc> dcEntry : dcs.entrySet()) {
            Dc dc = dcEntry.getValue();
            DbCluster dbCluster = dc.findDbCluster(dbClusterId);
            if (dbCluster != null) {
                return dc;
            }
        }
        throw new IllegalArgumentException("dbCluster not find!");
    }

    public String getLocalDcName() {
        return localDc == null ? (localDc = dataCenterService.getDc()) : localDc;
    }

    public synchronized Drc getDrc() {
        if(drc == null) {
            scheduledTask();
        }
        return drc;
    }

    public Drc getDrc(String dcName) {
        Dc dc = getDc(dcName);
        Drc drc = new Drc();
        drc.addDc(dc);
        return drc;
    }

    public Drc getLocalDrc() {
        return getDrc(getLocalDcName());
    }

    public synchronized String getDrcString() {
        if(drcString == null) {
            scheduledTask();
        }
        return drcString;
    }

    public Dc getDc(String dcName) {
        return getDrc().findDc(dcName);
    }

    public Dc getLocalDc() {
        return getDc(getLocalDcName());
    }

    /**
     * Deprecated
     */
    public synchronized Drc getDrcFromQConfig() {
        String dbClusterString = getProperty(DRC_DB_CLUSTER);
        try {
            if (drcFromQConfig == null) {
                drcFromQConfig = DefaultSaxParser.parse(dbClusterString);
            }
            return drcFromQConfig;
        } catch (Throwable t) {
            logger.error("[Parser] config {} error", dbClusterString);
        }
        return new Drc();
    }

    public Map<String, Dc> getDcs() {
        return getDrc() != null ? getDrc().getDcs() : Maps.newLinkedHashMap();
    }


    public synchronized Map<String,List<Mha>> getMhaGroupPairs() {
        Map<String,List<Mha>> mhaGroupPairsMap = Maps.newHashMap();
        Map<String, Dc> dcs = getDcs();
        for (String dcName : dcs.keySet()) {
            Dc dc = dcs.get(dcName);
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            for (String registrykey : dbClusters.keySet()) {
                DbCluster dbCluster = dbClusters.get(registrykey);
                String mhaName = dbCluster.getMhaName();
                String clusterName = dbCluster.getName();
                List<Applier> appliers = dbCluster.getAppliers();
                for (Applier applier : appliers) {
                    String targetClusterName = StringUtils.isNotBlank(applier.getTargetName()) ? applier.getTargetName() : clusterName;
                    String targetDcName = applier.getTargetIdc();
                    String targetMhaName = applier.getTargetMhaName();
                    String targetRegistrykey = targetClusterName + "." + targetMhaName;
                    Dc targetDc = dcs.get(targetDcName);
                    if (null != targetDc) {
                        DbCluster targetDbCluster = targetDc.getDbClusters().get(targetRegistrykey);
                        if (null != targetDbCluster) {
                            Mha localMha = new Mha(dcName, dbCluster);
                            Mha targetMha = new Mha(targetDcName, targetDbCluster);
                            if (!mhaGroupPairsMap.containsKey(mhaName+"."+targetMhaName) &&
                                    !mhaGroupPairsMap.containsKey(targetMhaName+"."+mhaName)) {
                                mhaGroupPairsMap.put(mhaName+"."+targetMhaName,Lists.newArrayList(localMha,targetMha));
                            }
                        }
                    }
                }
            }
        }
        return mhaGroupPairsMap;
    }
    public synchronized List<Set<Mha>> getMhaGroups() {
        if(null == mhaGroups) {
            mhaGroups = Lists.newArrayList();
            Map<String, Dc> dcs = getDcs();
            for(String dcName : dcs.keySet()) {
                Dc dc = dcs.get(dcName);
                Map<String, DbCluster> dbClusters = dc.getDbClusters();
                for(String registrykey : dbClusters.keySet()) {
                    DbCluster dbCluster = dbClusters.get(registrykey);
                    String clusterName = dbCluster.getName();
                    List<Applier> appliers = dbCluster.getAppliers();
                    for(Applier applier : appliers) {
                        String targetClusterName = StringUtils.isNotBlank(applier.getTargetName()) ? applier.getTargetName() : clusterName;
                        String targetDcName = applier.getTargetIdc();
                        String targetMhaName = applier.getTargetMhaName();
                        String targetRegistrykey = targetClusterName + "." + targetMhaName;
                        Dc targetDc = dcs.get(targetDcName);
                        if(null != targetDc) {
                            DbCluster targetDbCluster = targetDc.getDbClusters().get(targetRegistrykey);
                            if(null != targetDbCluster) {
                                Mha localMha = new Mha(dcName, dbCluster);
                                Mha targetMha = new Mha(targetDcName, targetDbCluster);
                                Set localGroup = getMhaGroup(localMha);
                                Set targetGroup = getMhaGroup(targetMha);
                                if(null == localGroup && null == targetGroup) {
                                    mhaGroups.add(new HashSet<>() {{
                                        add(localMha);
                                        add(targetMha);
                                    }});
                                } else {
                                    if(null != localGroup) {
                                        localGroup.add(targetMha);
                                    } else {
                                        targetGroup.add(localMha);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return mhaGroups;
    }

    private Set<Mha> getMhaGroup(Mha mha) {
        if(null == mhaGroups) {
            return null;
        }
        for(Set<Mha> mhaGroup : mhaGroups) {
            if(mhaGroup.contains(mha)) {
                return mhaGroup;
            }
        }
        return null;
    }

    public synchronized Set<Mha> getAllMhas() {
        if(null == allMhas) {
            allMhas = Sets.newHashSet();
            Map<String, Dc> dcs = getDcs();
            for(Map.Entry<String, Dc> entry : dcs.entrySet()) {
                String id = entry.getKey();
                Dc dc = entry.getValue();
                dc.getDbClusters().values().forEach(dbCluster -> {
                    allMhas.add(new Mha(id, dbCluster));
                });
            }
        }
        return allMhas;
    }

    public Set<String> getAllMhaWithMessengerInLocalRegion() {
        Set<String> res = Sets.newHashSet();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            Dc dc = getDc(dcInLocalRegion);
            for (DbCluster dbCluster : dc.getDbClusters().values()) {
                List<Messenger> messengers = dbCluster.getMessengers();
                if (!messengers.isEmpty()) {
                    res.add(dbCluster.getMhaName());
                }
            }
        }
        return res;
    }
    
    
    public Map<String, List<ReplicatorWrapper>> getAllReplicatorsInLocalRegion() {
        Map<String, List<ReplicatorWrapper>> replicators = Maps.newHashMap();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            replicators.putAll(getAllReplicatorInDc(dcInLocalRegion));
        }
        return replicators;
    }

    private Map<String,List<ReplicatorWrapper>> getAllReplicatorInDc(String dcInRegion) {
        Map<String, List<ReplicatorWrapper>> replicators = Maps.newHashMap();
        Dc dc = getDc(dcInRegion);
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


    /**
     * @return replicators delay monitor need
     *  key: clusterId where the replicator bounds to, aka registryKey
     *  value: ReplicatorWrapper
     */
    public Map<String, ReplicatorWrapper> getReplicatorsNeeded(List<String> mhaNamesToBeMonitored) {
        Map<String, ReplicatorWrapper> replicators = Maps.newHashMap();
        Set<String> dcsInLocalRegion = consoleConfig.getDcsInLocalRegion();
        for (String dcInLocalRegion : dcsInLocalRegion) {
            replicators.putAll(getReplicatorsSrcDcRelated(mhaNamesToBeMonitored,dcInLocalRegion));
        }
        return replicators;
    }

    
    public Map<String, ReplicatorWrapper> getReplicatorsSrcDcRelated(List<String> mhaNamesToBeMonitored, String srcDc) {
        Map<String, ReplicatorWrapper> replicators = Maps.newHashMap();
        Map<String, Dc> dcs = getDcs();
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
    
    /**
     * get all the replicatorMonitor which are in local dc
     */
    public List<ReplicatorMonitorWrapper> getReplicatorMonitorsInLocalDc() {
        List<ReplicatorMonitorWrapper> replicatorMonitors = Lists.newArrayList();
        String LOCAL_DC = getLocalDcName();
        Drc drc = getDrcFromQConfig();
        Map<String, Dc> dcs = drc.getDcs();
        for (Dc dc : dcs.values()) {
            String dcName = dc.getId();
            if (LOCAL_DC.equalsIgnoreCase(dcName)) {
                Map<String, DbCluster> dbClusters = dc.getDbClusters();
                for (DbCluster dbCluster : dbClusters.values()) {
                    if (null == dbCluster.getReplicatorMonitor()) {
                        continue;
                    }
                    replicatorMonitors.add(new ReplicatorMonitorWrapper(dbCluster.getReplicatorMonitor(), LOCAL_DC, LOCAL_DC, dbCluster.getName(), dbCluster.getMhaName(), dbCluster.getMhaName()));
                }
                break;
            }
        }
        return replicatorMonitors;
    }

    /**
     * key: local mhaName, value: corresponding master DB Endpoint
     * @return
     */
    public Map<String, Endpoint> getMasterDbEndpointInLocalDc() {
        Map<String, Endpoint> clusterEndpointMapper = Maps.newHashMap();
        String localDcName = getLocalDcName();
        Map<String, Dc> dcs = getDcs();
        for (Dc dc : dcs.values()) {
            String dcName = dc.getId();
            /**
             * only check the local dc's clusters
             */
            if (localDcName.equalsIgnoreCase(dcName)) {
                Map<String, DbCluster> dbClusters = dc.getDbClusters();
                /**
                 * check all the mhas
                 */
                for (DbCluster dbCluster : dbClusters.values()) {
                    String clusterName = dbCluster.getName();
                    String mhaName = dbCluster.getMhaName();
                    Dbs dbs = dbCluster.getDbs();
                    String monitorUser = dbs.getMonitorUser();
                    String monitorPassword = dbs.getMonitorPassword();
                    List<Db> dbList = dbs.getDbs();
                    for (Db db : dbList) {
                        if (db.isMaster()) {
                            Endpoint mySqlMasterEndpoint = new MySqlEndpoint(db.getIp(), db.getPort(), monitorUser, monitorPassword, BooleanEnum.TRUE.isValue());
                            clusterEndpointMapper.put(mhaName, mySqlMasterEndpoint);
                            break;
                        }
                    }
                }
                break;
            }
        }
        return clusterEndpointMapper;
    }

    public List<String> getTargetDcMha(String mha) {
        Map<String, Dc> dcs = getDcs();
        for (Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                List<Applier> appliers = dbCluster.getAppliers();
                if(!appliers.isEmpty()) {
                    List<String> targetDcMha = Lists.newArrayList();
                    targetDcMha.add(appliers.get(0).getTargetIdc());
                    targetDcMha.add(appliers.get(0).getTargetMhaName());
                    return targetDcMha;
                }
            }
        }
        return null;
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

    public Endpoint getMasterEndpoint(String mha) {
        Map<String, Dc> dcs = getDcs();
        for(Dc dc : dcs.values()) {
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            DbCluster dbCluster = dbClusters.values().stream().filter(p -> mha.equalsIgnoreCase(p.getMhaName())).findFirst().orElse(null);
            if(null != dbCluster) {
                return getMaster(dbCluster);
            }
        }
        return null;
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


    public List<List<Mha>> getCombinationListFromSet(Set<Mha> mhaGroup) {
        List<List<Mha>> mhaCombinationList = Lists.newArrayList();
        Set<Mha> traversed = new HashSet<>();
        for(Mha mha : mhaGroup) {
            traversed.add(mha);
            for(Mha mha1 : mhaGroup) {
                if(!traversed.contains(mha1)) {
                    List<Mha> clusterDbWrapperCombination = new ArrayList<>() {{
                        add(mha);
                        add(mha1);
                    }};
                    mhaCombinationList.add(clusterDbWrapperCombination);
                }
            }
        }
        return mhaCombinationList;
    }

    public List<List<Mha>> getAllMhaCombinationList() {
        List<Set<Mha>> mhaGroups = getMhaGroups();
        if(null == allMhaCombinationList) {
            allMhaCombinationList = Lists.newArrayList();
            for(Set<Mha> mhaGroup : mhaGroups) {
                allMhaCombinationList.addAll(getCombinationListFromSet(mhaGroup));
            }
        }
        return allMhaCombinationList;
    }


    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
    

    public static final class Mha {

        private String dc;

        private DbCluster dbCluster;

        public Mha(String dc, DbCluster dbCluster) {
            this.dc = dc;
            this.dbCluster = dbCluster;
        }

        public String getDc() {
            return dc;
        }

        public void setDc(String dc) {
            this.dc = dc;
        }

        public DbCluster getDbCluster() {
            return dbCluster;
        }

        public void setDbCluster(DbCluster dbCluster) {
            this.dbCluster = dbCluster;
        }

        public Endpoint getMasterDb() {
            Dbs dbs = dbCluster.getDbs();
            List<Db> dbList = dbs.getDbs();
            Db master = dbList.stream().filter(db -> db.isMaster()).findFirst().orElse(dbList.get(0));
            return new MySqlEndpoint(master.getIp(), master.getPort(), dbs.getMonitorUser(), dbs.getMonitorPassword(), BooleanEnum.TRUE.isValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Mha)) {
                return false;
            }
            Mha mha = (Mha) o;
            return Objects.equals(getDc(), mha.getDc()) &&
                    Objects.equals(getDbCluster(), mha.getDbCluster());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getDc(), getDbCluster());
        }
    }

    @Override
    public String toString() {
        return "dbClusterSourceProvider";
    }
}
