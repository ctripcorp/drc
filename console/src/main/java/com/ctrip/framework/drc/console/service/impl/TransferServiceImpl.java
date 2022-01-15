package com.ctrip.framework.drc.console.service.impl;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.pojo.Mha;
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
                    int applyMode = appliers.size() == 0 ? 0 : appliers.get(0).getApplyMode();
                    Long applierGroupId = dalUtils.updateOrCreateAGroup(replicatorGroupId, mhaId, includedDbs, applyMode, nameFilter, nameMapping);

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

        List<GroupMappingTbl> groupMappingTblsToBeDeleted = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) && p.getMhaGroupId().equals(mhaGroupTbl.getId())).collect(Collectors.toList());
        for(GroupMappingTbl groupMappingTbl : groupMappingTblsToBeDeleted){
            groupMappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        }
        logger.info("do delete mha group {}", mhaGroupTbl.getId());
        mhaGroupTbl.setDrcEstablishStatus(EstablishStatusEnum.UNSTART.getCode());
        mhaGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        mhaGroupTbl.setMonitorSwitch(BooleanEnum.FALSE.getCode());

        List<MachineTbl> machineTblsToBeDeleted = Lists.newArrayList();
        List<MhaTbl> mhaTblsToBeDeleted = Lists.newArrayList();
        List<ReplicatorGroupTbl> replicatorGroupTblsToBeDeleted = Lists.newArrayList();
        List<ApplierGroupTbl> applierGroupTblsToBeDeleted = Lists.newArrayList();
        List<ReplicatorTbl> replicatorTblsToBeDeleted = Lists.newArrayList();
        List<ApplierTbl> applierTblsToBeDeleted = Lists.newArrayList();

        //judge by mapping_number
        MhaTbl srcMhaTbl = mhaTbls.get(0);
        MhaTbl destMhaTbl = mhaTbls.get(1);

        MhaTbl finalSrcMhaTbl = srcMhaTbl;
        List<GroupMappingTbl> srcMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getMhaId().equals(finalSrcMhaTbl.getId())).collect(Collectors.toList());
        MhaTbl finalDestMhaTbl = destMhaTbl;
        List<GroupMappingTbl> destMappingTbls = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(predicate -> predicate.getDeleted().equals(BooleanEnum.FALSE.getCode()) && predicate.getMhaId().equals(finalDestMhaTbl.getId())).collect(Collectors.toList());

        // put srcMha at first as mainMha
        if(destMappingTbls.size() > 1) {
            MhaTbl tmp = srcMhaTbl;
            srcMhaTbl = destMhaTbl;
            destMhaTbl = tmp;
        }

        if(srcMappingTbls.size() > 1 || destMappingTbls.size() > 1) {
            // one_to_many_effect_toBeDeleted  :  group_mapping,mha_group,mha,destMha：{replicator_group,replicator,applier_group,applier,machine}
            // + srcMha's applier_group and its applier ( replicator_group_id = dest.replicator_group_id)
            // replicator_group.mha_id means its.srcMhaId  applier_group.mha_id means its.destMhaId
            mhaTblsToBeDeleted.add(destMhaTbl);

            MhaTbl finalDestMhaTbl1 = destMhaTbl;
            final long destMha_replicator_group_id = replicatorGroupTbls.stream().filter(predicate -> predicate.getMhaId().equals(finalDestMhaTbl1.getId())).findFirst().get().getId();
            MhaTbl finalSrcMhaTbl1 = srcMhaTbl;
            applierGroupTbls.stream().filter(p -> p.getReplicatorGroupId().equals(destMha_replicator_group_id) && p.getMhaId().equals(finalSrcMhaTbl1.getId())).forEach(applierGroupTbl -> {
                logger.info("do mark srcMha's applier group {} as deleted", applierGroupTbl.getId());
                applierGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
                applierGroupTblsToBeDeleted.add(applierGroupTbl);
                applierTbls.stream().filter(predicate -> predicate.getApplierGroupId().equals(applierGroupTbl.getId())).forEach(applierTbl -> {
                    logger.info("do mark srcMha's applier {} as deleted", applierTbl.getId());
                    applierTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    applierTblsToBeDeleted.add(applierTbl);
                });
            });
        }else{
            // one_to_one
            mhaTblsToBeDeleted.addAll(mhaTbls);
        }
        for(MhaTbl mhaTbl : mhaTblsToBeDeleted) {
            Long mhaId = mhaTbl.getId();
            logger.info("do mark mha {} as deleted", mhaTbl.getMhaName());
            mhaTbl.setDeleted(BooleanEnum.TRUE.getCode());
            machineTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaId)).forEach(machineTbl -> {
                logger.info("do mark machine {}:{} as deleted", machineTbl.getIp(), machineTbl.getPort());
                machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
                machineTblsToBeDeleted.add(machineTbl);
            });
            replicatorGroupTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaId)).forEach(replicatorGroupTbl -> {
                logger.info("do mark replicator group {} as deleted", replicatorGroupTbl.getId());
                replicatorGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
                replicatorGroupTblsToBeDeleted.add(replicatorGroupTbl);
                replicatorTbls.stream().filter(predicate -> predicate.getRelicatorGroupId().equals(replicatorGroupTbl.getId())).forEach(replicatorTbl -> {
                    logger.info("do mark replicator {} as deleted", replicatorTbl.getId());
                    replicatorTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    replicatorTblsToBeDeleted.add(replicatorTbl);
                });
            });
            // one2many destMha's applierGroup_mhaId could make sure unique one,no need replicator_group_id
            applierGroupTbls.stream().filter(predicate -> predicate.getMhaId().equals(mhaId)).forEach(applierGroupTbl -> {
                logger.info("do mark applier group {} as deleted", applierGroupTbl.getId());
                applierGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
                applierGroupTblsToBeDeleted.add(applierGroupTbl);
                applierTbls.stream().filter(predicate -> predicate.getApplierGroupId().equals(applierGroupTbl.getId())).forEach(applierTbl -> {
                    logger.info("do mark applier {} as deleted", applierTbl.getId());
                    applierTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    applierTblsToBeDeleted.add(applierTbl);
                });
            });
        }


        logger.info("do delete mhaGroup, GroupMhaMapping, mha, machine, replicator group, applier group, replicator, applier s");
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
        if(null != mhaGroupId) {
            MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId);
            List<MhaTbl> srcMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> (p.getMhaName().equalsIgnoreCase(mhaName))).collect(Collectors.toList());
            // actually only one
            List<MhaTbl> destMha = dalUtils.getMhaTblDao().queryAll().stream()
                    .filter(p -> (p.getMhaName().equalsIgnoreCase(dstMhaName))).collect(Collectors.toList());
            srcMha.addAll(destMha);
            doUpdate(mhaGroupTbl, srcMha,BooleanEnum.FALSE);
        }
    }

    public void doUpdate(MhaGroupTbl mhaGroupTbl, List<MhaTbl> mhaTbls, BooleanEnum finalDeletedStatus) throws SQLException{
        if (finalDeletedStatus.equals(BooleanEnum.TRUE)) {
            // doRemove
            return;
        } else {
            // doRecover
            // gruop,groupMapping,mha,replicator_group,applicator_group,machine
            // replicators,appliers insert new data no need recovery
            List<GroupMappingTbl> groupMappingTblsToBeUpdated = dalUtils.getGroupMappingTblDao().queryAll().stream()
                    .filter(p -> p.getDeleted().equals(BooleanEnum.TRUE.getCode())
                            && p.getMhaGroupId().equals(mhaGroupTbl.getId())).collect(Collectors.toList());
            List<MhaTbl> mhaTblsToBeUpdated = Lists.newArrayList();
            List<MachineTbl> machineTblsToBeUpdated = Lists.newArrayList();
            List<ReplicatorGroupTbl> replicatorGroupTblsToBeUpdated = Lists.newArrayList();
            List<ApplierGroupTbl> applierGroupTblsToBeUpdated = Lists.newArrayList();
            List<ReplicatorTbl> replicatorTblsToBeUpdated = Lists.newArrayList();
            List<ApplierTbl> applierTblsToBeUpdated = Lists.newArrayList();
            for (MhaTbl mhaTbl : mhaTbls) {
                if (mhaTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
                    mhaTblsToBeUpdated.add(mhaTbl);
                }
            }
            logger.info("do recover mha group {}", mhaGroupTbl.getId());
            mhaGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            for (GroupMappingTbl groupMappingTbl : groupMappingTblsToBeUpdated) {
                logger.info("do recover mha group mapping {}", groupMappingTbl.getId());
                groupMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            }
            if (mhaTblsToBeUpdated.size() == 1) {
                // recover one2many(one is not deleted) gruop*1,groupMapping*2,mha*1,replicator_group*1,applicator_group*2,machine*(n/2n)
                // special applicator_group and applicators is essential recover first
                long destMhaId = mhaTblsToBeUpdated.get(0).getId();
                long srcMhaId = mhaTbls.stream().filter(p -> !p.getId().equals(destMhaId)).findFirst().get().getId();
                final long destMha_replicator_group_id = dalUtils.getReplicatorGroupTblDao().queryAll().stream().filter(predicate -> predicate.getMhaId().equals(destMhaId)).findFirst().get().getId();
                dalUtils.getApplierGroupTblDao().queryAll().stream().filter(p -> p.getReplicatorGroupId().equals(destMha_replicator_group_id) && p.getMhaId().equals(srcMhaId)).forEach(applierGroupTbl -> {
                    logger.info("do mark srcMha's applier group {} as unDeleted", applierGroupTbl.getId());
                    applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
                    applierGroupTblsToBeUpdated.add(applierGroupTbl);
                });
            }
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
                dalUtils.getApplierGroupTblDao().queryAll().stream().filter(predicate -> predicate.getMhaId().equals(mhaId) && predicate.getDeleted().equals(BooleanEnum.TRUE.getCode())).forEach(applierGroupTbl -> {
                    logger.info("mark applier group {} as unDeleted", applierGroupTbl.getId());
                    applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
                    applierGroupTblsToBeUpdated.add(applierGroupTbl);
                });
            }

            logger.info("do recover mhaGroup, GroupMhaMapping, mha, machine, replicator group, applier group, replicator, applier s");
            dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
            dalUtils.getGroupMappingTblDao().update(groupMappingTblsToBeUpdated);
            dalUtils.getMhaTblDao().update(mhaTblsToBeUpdated);
            dalUtils.getMachineTblDao().update(machineTblsToBeUpdated);

            dalUtils.getReplicatorGroupTblDao().update(replicatorGroupTblsToBeUpdated);
            dalUtils.getApplierGroupTblDao().update(applierGroupTblsToBeUpdated);
            dalUtils.getReplicatorTblDao().update(replicatorTblsToBeUpdated);
            dalUtils.getApplierTblDao().update(applierTblsToBeUpdated);

        }

    }
}
