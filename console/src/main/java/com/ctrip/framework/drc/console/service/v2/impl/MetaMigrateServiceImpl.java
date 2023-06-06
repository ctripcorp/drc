package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierGroupTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.ApplierTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.MetaMigrateService;
import com.ctrip.platform.dal.dao.DalHints;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/6/5 16:53
 */
@Service
public class MetaMigrateServiceImpl implements MetaMigrateService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private MhaTblDao mhaTblDao;
    @Autowired
    private MhaGroupTblDao mhaGroupTblDao;
    @Autowired
    private GroupMappingTblDao groupMappingTblDao;
    @Autowired
    private ClusterTblDao clusterTblDao;
    @Autowired
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierGroupTblDao applierGroupTblDao;
    @Autowired
    private ApplierTblDao applierTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ClusterManagerTblDao clusterManagerTblDao;
    @Autowired
    private ZookeeperTblDao zookeeperTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;

    private static final int MHA_GROUP_SIZE = 2;

    @Override
    public int migrateMhaTbl() throws Exception {
        List<MhaTbl> oldMhaTbls = mhaTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaGroupTbl> mhaGroupTbls = mhaGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<GroupMappingTbl> groupMappingTbls = groupMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterTbl> clusterTbls = clusterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ClusterMhaMapTbl> clusterMhaMapTbls = clusterMhaMapTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, MhaGroupTbl> mhaGroupMap = mhaGroupTbls.stream().collect(Collectors.toMap(MhaGroupTbl::getId, Function.identity()));
        Map<Long, Long> groupMap = groupMappingTbls.stream().collect(Collectors.toMap(GroupMappingTbl::getMhaId, GroupMappingTbl::getMhaGroupId, (k1, k2) -> k1));
        Map<Long, Long> clusterMhaMap = clusterMhaMapTbls.stream().collect(Collectors.toMap(ClusterMhaMapTbl::getMhaId, ClusterMhaMapTbl::getClusterId, (k1, k2) -> k1));
        Map<Long, ClusterTbl> clusterMap = clusterTbls.stream().collect(Collectors.toMap(ClusterTbl::getId, Function.identity()));


        List<String> errorMhaNames = new ArrayList<>();
        List<MhaTblV2> newMhaTbls = new ArrayList<>();
        for (MhaTbl oldMhaTbl :oldMhaTbls) {
            if (!groupMap.containsKey(oldMhaTbl.getId()) || clusterMhaMap.containsKey(oldMhaTbl.getId())) {
                errorMhaNames.add(oldMhaTbl.getMhaName());
                continue;
            }
            long mhaGroupId = groupMap.get(oldMhaTbl.getId());
            long clusterId = clusterMhaMap.get(oldMhaTbl.getId());
            MhaGroupTbl mhaGroupTbl = mhaGroupMap.get(mhaGroupId);
            ClusterTbl clusterTbl = clusterMap.get(clusterId);

            MhaTblV2 newMhaTbl = buildMhaTblV2(oldMhaTbl, mhaGroupTbl, clusterTbl);
            newMhaTbls.add(newMhaTbl);
        }

        if (CollectionUtils.isEmpty(errorMhaNames)) {
            mhaTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), newMhaTbls);
        } else {
            throw new IllegalArgumentException(String.format("batchInsert mhaTblV2 fail, error mhaNames: %s", errorMhaNames));
        }

        return newMhaTbls.size();
    }

    @Override
    public int migrateMhaReplication() throws Exception {
        List<MhaGroupTbl> mhaGroupTbls = mhaGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<GroupMappingTbl> groupMappingTbls = groupMappingTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> applierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTbl> applierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getMhaId, ReplicatorGroupTbl::getId));
        Map<Long, List<ApplierGroupTbl>> applierGroupMap = applierGroupTbls.stream().collect(Collectors.groupingBy(ApplierGroupTbl::getReplicatorGroupId));
        Map<Long, List<ApplierTbl>> applierMap = applierTbls.stream().collect(Collectors.groupingBy(ApplierTbl::getApplierGroupId));
        Map<Long, List<Long>> groupMappingMap = groupMappingTbls.stream().collect(Collectors.groupingBy(GroupMappingTbl::getMhaGroupId, Collectors.mapping(GroupMappingTbl::getMhaId, Collectors.toList())));

        List<MhaReplicationTbl> mhaReplicationTbls = new ArrayList<>();
        for (MhaGroupTbl mhaGroupTbl : mhaGroupTbls) {
            List<Long> mhaIds = groupMappingMap.get(mhaGroupTbl.getId());
            if (CollectionUtils.isEmpty(mhaIds) || mhaIds.size() != MHA_GROUP_SIZE) {
                logger.warn("mhaGroupId: {} not match mha replication", mhaGroupTbl.getId());
                continue;
            }
            if (existReplication(mhaIds.get(0), mhaIds.get(1), replicatorGroupMap, applierGroupMap, applierMap)) {
                MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
                mhaReplicationTbl.setSrcMhaId(mhaIds.get(0));
                mhaReplicationTbl.setDstMhaId(mhaIds.get(1));
                mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaReplicationTbls.add(mhaReplicationTbl);
            }
            if (existReplication(mhaIds.get(1), mhaIds.get(0), replicatorGroupMap, applierGroupMap, applierMap)) {
                MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
                mhaReplicationTbl.setSrcMhaId(mhaIds.get(1));
                mhaReplicationTbl.setDstMhaId(mhaIds.get(0));
                mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
                mhaReplicationTbls.add(mhaReplicationTbl);
            }
        }

        mhaReplicationTblDao.batchInsert(mhaReplicationTbls);
        return mhaReplicationTbls.size();
    }

    @Override
    public int migrateApplierGroup() throws Exception {
        List<ReplicatorGroupTbl> replicatorGroupTbls = replicatorGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierGroupTbl> oldApplierGroupTbls = applierGroupTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Map<Long, Long> replicatorGroupMap = replicatorGroupTbls.stream().collect(Collectors.toMap(ReplicatorGroupTbl::getId, ReplicatorGroupTbl::getMhaId));

        List<Long> errorApplierGroupIds = new ArrayList<>();
        List<ApplierGroupTblV2> newApplierGroupTbls = new ArrayList<>();
        for (ApplierGroupTbl oldApplierGroupTbl : oldApplierGroupTbls) {
            Long srcMhaId = replicatorGroupMap.get(oldApplierGroupTbl.getReplicatorGroupId());
            if (srcMhaId == null) {
                errorApplierGroupIds.add(oldApplierGroupTbl.getId());
                continue;
            }
            Long dstMhaId = oldApplierGroupTbl.getMhaId();
            Long mhaReplicationId = mhaReplicationTbls.stream().filter(e -> e.getSrcMhaId().equals(srcMhaId) && e.getDstMhaId().equals(dstMhaId)).findFirst().map(MhaReplicationTbl::getId).orElse(-1L);
            ApplierGroupTblV2 newApplierGroupTbl = new ApplierGroupTblV2();
            newApplierGroupTbl.setId(oldApplierGroupTbl.getId());
            newApplierGroupTbl.setMhaReplicationId(mhaReplicationId);
            newApplierGroupTbl.setGtidInit(oldApplierGroupTbl.getGtidExecuted());
            newApplierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());

            newApplierGroupTbls.add(newApplierGroupTbl);
        }

        if (CollectionUtils.isEmpty(errorApplierGroupIds)) {
            applierGroupTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), newApplierGroupTbls);
        } else {
            throw new IllegalArgumentException(String.format("batchInsert applierGroup fail, errorApplierGroupIds: %s", errorApplierGroupIds));
        }

        return newApplierGroupTbls.size();
    }

    @Override
    public int migrateApplier() throws Exception {
        List<ApplierTbl> oldApplierTbls = applierTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        List<ApplierTblV2> newApplierTbls = oldApplierTbls.stream().map(source -> {
            ApplierTblV2 target = new ApplierTblV2();
            target.setId(source.getId());
            target.setApplierGroupId(source.getApplierGroupId());
            target.setPort(source.getPort());
            target.setResourceId(source.getResourceId());
            target.setMaster(source.getMaster());
            target.setDeleted(source.getDeleted());

            return target;
        }).collect(Collectors.toList());

        applierTblV2Dao.batchInsert(new DalHints().enableIdentityInsert(), newApplierTbls);
        return newApplierTbls.size();
    }

    private boolean existReplication(Long srcMhaId, Long dstMhaId, Map<Long, Long> replicatorGroupMap, Map<Long, List<ApplierGroupTbl>> applierGroupMap, Map<Long, List<ApplierTbl>> applierMap) {
        Long replicatorGroupId = replicatorGroupMap.get(srcMhaId);
        if (replicatorGroupId == null) {
            logger.warn("mhaId: {} not match replicatorGroup");
            return false;
        }
        List<ApplierGroupTbl> applierGroupTbls = applierGroupMap.get(replicatorGroupId);
        if (CollectionUtils.isEmpty(applierGroupTbls)) {
            return false;
        }
        Long applierGroupId = applierGroupTbls.stream().filter(e -> e.getMhaId().equals(dstMhaId)).findFirst().map(ApplierGroupTbl::getId).orElse(null);
        if (applierGroupId == null) {
            return false;
        }
        return applierMap.containsKey(applierGroupId);
    }

    private MhaTblV2 buildMhaTblV2(MhaTbl mhaTbl, MhaGroupTbl mhaGroupTbl, ClusterTbl clusterTbl) {
        MhaTblV2 newMhaTbl = new MhaTblV2();
        newMhaTbl.setId(mhaTbl.getId());
        newMhaTbl.setMhaName(mhaTbl.getMhaName());
        newMhaTbl.setDcId(mhaTbl.getDcId());
        newMhaTbl.setApplyMode(mhaTbl.getApplyMode());
        newMhaTbl.setMonitorSwitch(mhaTbl.getMonitorSwitch());
        newMhaTbl.setBuId(clusterTbl.getBuId());
        newMhaTbl.setClusterName(clusterTbl.getClusterName());
        newMhaTbl.setAppId(clusterTbl.getClusterAppId());
        newMhaTbl.setReadUser(mhaGroupTbl.getReadUser());
        newMhaTbl.setReadPassword(mhaGroupTbl.getReadPassword());
        newMhaTbl.setWriteUser(mhaGroupTbl.getWriteUser());
        newMhaTbl.setWritePassword(mhaGroupTbl.getWritePassword());
        newMhaTbl.setMonitorUser(mhaGroupTbl.getMonitorUser());
        newMhaTbl.setMonitorPassword(mhaGroupTbl.getMonitorPassword());
        newMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return newMhaTbl;
    }
}
