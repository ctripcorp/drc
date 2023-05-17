package com.ctrip.framework.drc.console.service.impl;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.service.TransferService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-11
 */
@Service
public class TransferServiceImpl implements TransferService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    private DalUtils dalUtils = DalUtils.getInstance();

    @Override
    public void loadMetaData(String meta) throws Exception {
        logger.info("start loading meta data.");
        Drc drc = DefaultSaxParser.parse(meta);
        logger.info("start logically removing meta data.");
        removeMetaData(drc);
        logger.info("logically remove meta data finished.");
        Map<String, Dc> dcs = drc.getDcs();
        for(Dc dc : dcs.values()) {
            String dcName = dc.getId();
            Long dcId = dalUtils.updateOrCreateDc(dcName);
            ZkServer zkServer = dc.getZkServer();
            loadZk(dcId, zkServer);
            List<ClusterManager> clusterManagers = dc.getClusterManagers();
            loadCM(dcId, clusterManagers);
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            loadDbClusters(dcId, dbClusters);
        }
        loadAppliers(dcs);
        logger.info("load meta data finished.");
    }

    @Override
    public void loadOneMetaData(String oneMetaData) throws Exception {
        logger.info("start loading one meta data.");
        Drc drc = DefaultSaxParser.parse(oneMetaData);
        Map<String, Dc> dcs = drc.getDcs();
        for(Dc dc : dcs.values()) {
            String dcName = dc.getId();
            Long dcId = dalUtils.updateOrCreateDc(dcName);
            Map<String, DbCluster> dbClusters = dc.getDbClusters();
            loadDbClusters(dcId, dbClusters);
        }
        loadAppliers(dcs);
        logger.info("load one meta data finished.");
    }

    private void removeMetaData(Drc drc) throws SQLException {
        if(null == drc) {
            // Fuse strategy
            return;
        }
        Set<String> mhaNames = Sets.newHashSet();
        drc.getDcs().forEach((dcId, dc) -> dc.getDbClusters().forEach((id, dbCluster) -> mhaNames.add(dbCluster.getMhaName())));
        List<MhaGroupTbl> mhaGroupTbls = dalUtils.getMhaGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        for(MhaGroupTbl mhaGroupTbl : mhaGroupTbls) {
            Long mhaGroupTblId = mhaGroupTbl.getId();
            List<MhaTbl> mhaTbls = metaInfoService.getMhaTbls(mhaGroupTblId);
            int deletedCount = 0;
            for(MhaTbl mhaTbl : mhaTbls) {
                if(!mhaNames.contains(mhaTbl.getMhaName())) {
                    deletedCount++;
                }
            }
            if(deletedCount == mhaTbls.size()) {
                doRemove(mhaGroupTbl, mhaTbls);
            }
        }
    }

    protected void loadZk(long dcId, ZkServer zkServer) throws SQLException {
        String addressesStr = zkServer.getAddress();
        String[] addresses = addressesStr.split(",");
        for(String address : addresses) {
            String[] split = address.split(":");
            if (split.length != 2) {
                continue;
            }
            String ip = split[0];
            int port = Integer.parseInt(split[1]);

            Long resourceId = dalUtils.updateOrCreateResource(ip, dcId, ModuleEnum.ZOOKEEPER);
            dalUtils.updateOrCreateZk(port, resourceId);
        }
    }

    protected void loadCM(long dcId, List<ClusterManager> clusterManagers) throws SQLException {
        for(ClusterManager cm : clusterManagers) {
            String ip = cm.getIp();
            int port = cm.getPort();
            boolean master = cm.isMaster();

            Long resourceId = dalUtils.updateOrCreateResource(ip, dcId, ModuleEnum.CLUSTER_MANAGER);
            dalUtils.updateOrCreateCM(port, resourceId, master);
        }
    }

    protected void loadDbClusters(long dcId, Map<String, DbCluster> dbClusters) throws SQLException {
        for(DbCluster dbCluster : dbClusters.values()) {
            String buName = dbCluster.getBuName();

            Long buId = dalUtils.updateOrCreateBu(buName);

            String clusterName = dbCluster.getName();
            Long appId = dbCluster.getAppId();
            Long clusterId = dalUtils.updateOrCreateCluster(clusterName, appId, buId);

            String mhaName = dbCluster.getMhaName();
            Dbs dbs = dbCluster.getDbs();
            List<Replicator> replicators = dbCluster.getReplicators();
            List<Applier> appliers = dbCluster.getAppliers();
            Map<String, String> targetMhaNames = appliers.stream().collect(Collectors.toMap(Applier::getTargetMhaName, Applier::getTargetIdc, (o, n) -> n));
            long mhaId = loadMha(dcId, clusterId, mhaName, dbs, targetMhaNames);

            loadReplicators(dcId, mhaId, replicators);
        }
    }

    protected Long loadMha(long dcId, long clusterId, String mhaName, Dbs dbs, Map<String, String> targetMhaNames) throws SQLException {

        Long mhaId = dalUtils.updateOrCreateMha(mhaName, dcId);
        dalUtils.updateOrCreateClusterMhaMap(clusterId, mhaId);

        for (Map.Entry<String, String> entry : targetMhaNames.entrySet()) {
            String targetMhaName = entry.getKey();
            Long targetDcId = dalUtils.updateOrCreateDc(entry.getValue());
            Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName, targetMhaName);
            if(null == mhaGroupId) {
                logger.info("load mha group for {}-{}", mhaName, targetMhaName);
                mhaGroupId = dalUtils.insertMhaGroup(BooleanEnum.TRUE, EstablishStatusEnum.ESTABLISHED, dbs.getReadUser(), dbs.getReadPassword(), dbs.getWriteUser(), dbs.getWritePassword(), dbs.getMonitorUser(), dbs.getMonitorPassword());
            } else {
                logger.info("already loaded mha group for {}-{}", mhaName, targetMhaName);
            }
            Long dstMhaId = dalUtils.updateOrCreateMha(targetMhaName, targetDcId);
            dalUtils.updateOrCreateClusterMhaMap(clusterId, dstMhaId);
            dalUtils.updateOrCreateGroupMapping(mhaGroupId, mhaId);
            dalUtils.updateOrCreateGroupMapping(mhaGroupId, dstMhaId);
        }

        loadMachine(mhaId, dbs.getDbs());
        return mhaId;
    }

    protected void loadMachine(long mhaId, List<Db> dbList) throws SQLException {
        for(Db db : dbList) {
            dalUtils.updateOrCreateMachine(db.getIp(), db.getPort(), db.getUuid(), db.isMaster() ? BooleanEnum.TRUE : BooleanEnum.FALSE, mhaId);
        }
    }

    protected void loadReplicators(long dcId, long mhaId, List<Replicator> replicators) throws SQLException {

        Long replicatorGroupId = dalUtils.updateOrCreateRGroup(mhaId);

        for(Replicator replicator : replicators) {
            String ip = replicator.getIp();
            Long resourceId = dalUtils.updateOrCreateResource(ip , dcId, ModuleEnum.REPLICATOR);
            dalUtils.updateOrCreateReplicator(replicator.getPort(), replicator.getApplierPort(), replicator.getGtidSkip(), resourceId, replicatorGroupId, replicator.isMaster() ? BooleanEnum.TRUE : BooleanEnum.FALSE);
        }
    }

    protected void loadAppliers(Map<String, Dc> dcs) throws SQLException {
        for(Dc dc : dcs.values()) {
            for(DbCluster dbCluster : dc.getDbClusters().values()) {
                String mhaName = dbCluster.getMhaName();
                Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mhaName);

                Map<String, List<Applier>> applierGroups = getApplierGroups(dbCluster);
                for (Map.Entry<String, List<Applier>> applierGroup : applierGroups.entrySet()) {
                    String targetMhaName = applierGroup.getKey();
                    List<Applier> appliers = applierGroup.getValue();

                    Long replicatorGroupId = metaInfoService.getReplicatorGroupId(targetMhaName);
                    String includedDbs = appliers.size() == 0 ? "" : appliers.get(0).getIncludedDbs();
                    String nameFilter = appliers.size() == 0 ? "" : appliers.get(0).getNameFilter();
                    String nameMapping = appliers.size() == 0 ? "" : appliers.get(0).getNameMapping();
                    String targetName = appliers.size() == 0 ? "" : appliers.get(0).getTargetName();
                    int applyMode = appliers.size() == 0 ? 0 : appliers.get(0).getApplyMode();
                    String gtid = appliers.size() == 0 ? "" : appliers.get(0).getGtidExecuted();
                    Long applierGroupId = dalUtils.updateOrCreateAGroup(replicatorGroupId, mhaId, includedDbs, applyMode, nameFilter, nameMapping, targetName,gtid);

                    // rough implementation, only for duo repl
                    for(Applier applier : appliers) {
                        String ip = applier.getIp();
                        long dcId = dalUtils.getId(TableEnum.DC_TABLE, dc.getId());
                        Long resourceId = dalUtils.updateOrCreateResource(ip, dcId, ModuleEnum.APPLIER);
                        dalUtils.updateOrCreateApplier(applier.getPort(), applier.getGtidExecuted(), resourceId, applierGroupId, applier.isMaster() ? BooleanEnum.TRUE : BooleanEnum.FALSE);
                    }
                }
            }
        }
    }

    private Map<String, List<Applier>> getApplierGroups(DbCluster dbCluster) {
        Map<String, List<Applier>> applierGroups = Maps.newHashMap();
        for (Applier applier : dbCluster.getAppliers()) {
            String targetMhaName = applier.getTargetMhaName();
            List<Applier> appliers = applierGroups.getOrDefault(targetMhaName, Lists.newArrayList());
            appliers.add(applier);
            applierGroups.put(targetMhaName, appliers);
        }
        return applierGroups;
    }

    public void removeConfig(String mhaName, String dstMhaName) throws Exception {
        Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName, dstMhaName);
        if(null != mhaGroupId) {
//            MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId);
            MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryAll().stream()
                    .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && (p.getId().equals(mhaGroupId))).findFirst().get();
            List<MhaTbl> srcMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && (p.getMhaName().equalsIgnoreCase(mhaName) ))
                    .collect(Collectors.toList());
            // actually only one
            List<MhaTbl> destMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && (p.getMhaName().equalsIgnoreCase(dstMhaName) ))
                    .collect(Collectors.toList());
            srcMha.addAll(destMha);
            doRemove(mhaGroupTbl, srcMha);
        } else {
            throw new Exception("no such mha: " + mhaName);
        }
    }

    public void doRemove(MhaGroupTbl mhaGroupTbl, List<MhaTbl> mhaTbls) throws SQLException {
        List<MachineTbl> machineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = dalUtils.getApplierGroupTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorTbl> replicatorTbls = dalUtils.getReplicatorTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTbl> applierTbls = dalUtils.getApplierTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        List<MachineTbl> machineTblsToBeDeleted = Lists.newArrayList();
        List<MhaTbl> mhaTblsToBeDeleted = Lists.newArrayList();
        List<ReplicatorGroupTbl> replicatorGroupTblsToBeDeleted = Lists.newArrayList();
        List<ApplierGroupTbl> applierGroupTblsToBeDeleted = Lists.newArrayList();
        List<ReplicatorTbl> replicatorTblsToBeDeleted = Lists.newArrayList();
        List<ApplierTbl> applierTblsToBeDeleted = Lists.newArrayList();

        logger.info("do mark mha group as deleted{}", mhaGroupTbl.getId());
        mhaGroupTbl.setDrcEstablishStatus(EstablishStatusEnum.UNSTART.getCode());
        mhaGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        mhaGroupTbl.setMonitorSwitch(BooleanEnum.FALSE.getCode());

        // should be mark as deleted in any case
        List<GroupMappingTbl> groupMappingTblsToBeDeleted = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && p.getMhaGroupId().equals(mhaGroupTbl.getId())).collect(Collectors.toList());
        for(GroupMappingTbl groupMappingTbl : groupMappingTblsToBeDeleted){
            logger.info("do mark groupMapping as deleted {}", groupMappingTbl.getId());
            groupMappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        }
        for (MhaTbl mhaTbl : mhaTbls) {
            long anOtherMhaId = mhaTbls.stream().filter(p -> !p.getMhaName().equals(mhaTbl.getMhaName())).findFirst().get().getId();
            long anOtherMhaReplicatorGroupId = replicatorGroupTbls.stream().filter(p -> p.getMhaId().equals(anOtherMhaId)).findFirst().get().getId();
            ApplierGroupTbl applierGroupTbl = applierGroupTbls.stream()
                    .filter(p -> p.getReplicatorGroupId().equals(anOtherMhaReplicatorGroupId) && p.getMhaId().equals(mhaTbl.getId()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode())).findFirst().get();
            logger.info("do mark applier group {} as deleted", applierGroupTbl.getId());
            applierGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
            applierGroupTblsToBeDeleted.add(applierGroupTbl);
            applierTbls.stream().filter(p -> p.getApplierGroupId().equals(applierGroupTbl.getId()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode())).forEach(applierTbl -> {
                logger.info("do mark applier as deleted,id is {}", applierTbl.getId());
                applierTbl.setDeleted(BooleanEnum.TRUE.getCode());
                applierTblsToBeDeleted.add(applierTbl);
            });
        }
        
        //judge by mapping_number
        MhaTbl srcMhaTbl = mhaTbls.get(0);
        MhaTbl destMhaTbl = mhaTbls.get(1);
        List<GroupMappingTbl> srcMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getMhaId().equals(srcMhaTbl.getId())).collect(Collectors.toList());
        List<GroupMappingTbl> destMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getMhaId().equals(destMhaTbl.getId())).collect(Collectors.toList());
        
        if (srcMappingTbls.size() > 1 && destMappingTbls.size() > 1) {
            // many2many case
            logger.info("retain two mha in many2many case,mhaGroupId to be deleted is {}",mhaGroupTbl.getId());
        } else if (srcMappingTbls.size() > 1) {
            // one2many case 
            mhaTblsToBeDeleted.add(destMhaTbl);
        } else if (destMappingTbls.size() > 1) {
            // one2many case 
            mhaTblsToBeDeleted.add(srcMhaTbl);
        } else {
            // one2one case
            mhaTblsToBeDeleted.addAll(mhaTbls);
        }

        for(MhaTbl mhaTbl : mhaTblsToBeDeleted) {
            Long mhaId = mhaTbl.getId();
            logger.info("do mark mha {} as deleted", mhaTbl.getMhaName());
            mhaTbl.setDeleted(BooleanEnum.TRUE.getCode());
            machineTbls.stream().filter(p -> p.getMhaId().equals(mhaId) && p.getDeleted().equals(BooleanEnum.FALSE.getCode())).forEach(machineTbl -> {
                logger.info("do mark db machine {}:{} as deleted", machineTbl.getIp(), machineTbl.getPort());
                machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
                machineTblsToBeDeleted.add(machineTbl);
            });
            replicatorGroupTbls.stream().filter(p -> p.getMhaId().equals(mhaId) && p.getDeleted().equals(BooleanEnum.FALSE.getCode())).forEach(replicatorGroupTbl -> {
                logger.info("do mark replicator group {} as deleted", replicatorGroupTbl.getId());
                replicatorGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
                replicatorGroupTblsToBeDeleted.add(replicatorGroupTbl);
                replicatorTbls.stream().filter(p -> p.getRelicatorGroupId().equals(replicatorGroupTbl.getId()) && p.getDeleted().equals(BooleanEnum.FALSE.getCode())).forEach(replicatorTbl -> {
                    logger.info("do mark replicator {} as deleted", replicatorTbl.getId());
                    replicatorTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    replicatorTblsToBeDeleted.add(replicatorTbl);
                });
            });
        }

        logger.info("do delete mhaGroup, GroupMhaMapping, mha, machine, replicator group, applier group, replicator, applier");
        dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
        dalUtils.getGroupMappingTblDao().update(groupMappingTblsToBeDeleted);
        dalUtils.getMhaTblDao().update(mhaTblsToBeDeleted);
        dalUtils.getMachineTblDao().update(machineTblsToBeDeleted);
        dalUtils.getReplicatorGroupTblDao().update(replicatorGroupTblsToBeDeleted);
        dalUtils.getApplierGroupTblDao().update(applierGroupTblsToBeDeleted);
        dalUtils.getReplicatorTblDao().update(replicatorTblsToBeDeleted);
        dalUtils.getApplierTblDao().update(applierTblsToBeDeleted);
    }


    public void recoverDeletedDrc(String mhaName, String dstMhaName) throws SQLException {
        Long mhaGroupId = metaInfoService.getMhaGroupId(mhaName,dstMhaName,BooleanEnum.TRUE);
        if (metaInfoService.getMhaGroupId(mhaName,dstMhaName,BooleanEnum.FALSE) != null) {
            logger.warn("group-{}-{} already exist",mhaName,dstMhaName);
            return;
        }
        if (null != mhaGroupId ) {
            MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId);
            List<MhaTbl> srcMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> (p.getMhaName().equalsIgnoreCase(mhaName))).collect(Collectors.toList());
            // actually only one
            List<MhaTbl> destMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> (p.getMhaName().equalsIgnoreCase(dstMhaName))).collect(Collectors.toList());
            srcMha.addAll(destMha);
            doReCover(mhaGroupTbl, srcMha);
        }
    }

    public void doReCover(MhaGroupTbl mhaGroupTbl, List<MhaTbl> mhaTbls) throws SQLException {
        // gruop,groupMapping,mha,applicator_group,replicator_group,machine
        // replicators,appliers insert new data no need to recovery

        List<GroupMappingTbl> groupMappingTblsToBeUpdated = Lists.newArrayList();
        List<MhaTbl> mhaTblsToBeUpdated = Lists.newArrayList();
        List<MachineTbl> machineTblsToBeUpdated = Lists.newArrayList();
        List<ReplicatorGroupTbl> replicatorGroupTblsToBeUpdated = Lists.newArrayList();
        List<ApplierGroupTbl> applierGroupTblsToBeUpdated = Lists.newArrayList();

        logger.info("do recover mha group {}", mhaGroupTbl.getId());
        mhaGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
        groupMappingTblsToBeUpdated = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.TRUE.getCode())
                && p.getMhaGroupId().equals(mhaGroupTbl.getId())).collect(Collectors.toList());
        for (GroupMappingTbl groupMappingTbl : groupMappingTblsToBeUpdated) {
            logger.info("do recover mha group mapping {}", groupMappingTbl.getId());
            groupMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
        }
        for (MhaTbl mhaTbl : mhaTbls) {
            long anOtherMhaId = mhaTbls.stream().filter(p -> !p.getMhaName().equals(mhaTbl.getMhaName())).findFirst().get().getId();
            long anOtherMhaReplicatorGroupId = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(p -> p.getMhaId().equals(anOtherMhaId)).findFirst().get().getId();
            ApplierGroupTbl applierGroupTbl = dalUtils.getApplierGroupTblDao().queryAll().stream()
                    .filter(p -> p.getReplicatorGroupId().equals(anOtherMhaReplicatorGroupId) && p.getMhaId().equals(mhaTbl.getId()) && p.getDeleted().equals(BooleanEnum.TRUE.getCode())).findFirst().get();
            logger.info("do recover applier group {} as unDeleted", applierGroupTbl.getId());
            applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            applierGroupTblsToBeUpdated.add(applierGroupTbl);
        }

        mhaTblsToBeUpdated.addAll(mhaTbls.stream().filter(p -> p.getDeleted().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList()));
        for (MhaTbl mhaTbl : mhaTblsToBeUpdated) {
            Long mhaId = mhaTbl.getId();
            logger.info("mark mha {} as unDeleted", mhaTbl.getMhaName());
            mhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            dalUtils.getMachineTblDao().queryAll().stream().filter(predicate -> predicate.getMhaId().equals(mhaId) && predicate.getDeleted().equals(BooleanEnum.TRUE.getCode())).forEach(machineTbl -> {
                logger.info("mark machine {}:{} as unDeleted", machineTbl.getIp(), machineTbl.getPort());
                machineTbl.setDeleted(BooleanEnum.FALSE.getCode());
                machineTblsToBeUpdated.add(machineTbl);
            });
            dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(predicate -> predicate.getMhaId().equals(mhaId) && predicate.getDeleted().equals(BooleanEnum.TRUE.getCode())).forEach(replicatorGroupTbl -> {
                logger.info("mark replicator group {} as unDeleted", replicatorGroupTbl.getId());
                replicatorGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
                replicatorGroupTblsToBeUpdated.add(replicatorGroupTbl);
            });
        }

        logger.info("do recover mhaGroup, GroupMhaMapping, mha, machine, replicator group, applier group, replicator, applier s");
        dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
        dalUtils.getGroupMappingTblDao().update(groupMappingTblsToBeUpdated);
        dalUtils.getMhaTblDao().update(mhaTblsToBeUpdated);
        dalUtils.getMachineTblDao().update(machineTblsToBeUpdated);
        dalUtils.getReplicatorGroupTblDao().update(replicatorGroupTblsToBeUpdated);
        dalUtils.getApplierGroupTblDao().update(applierGroupTblsToBeUpdated);

    }
}
