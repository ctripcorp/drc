package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_DB;
import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_MQ;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.MigrationTaskTbl;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.MigrationTaskTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.sql.SQLException;
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
 * @ClassName DbMigrationServiceImpl
 * @Author haodongPan
 * @Date 2023/8/14 14:58
 * @Version: $
 */
@Service
public class DbMigrationServiceImpl implements DbMigrationService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired private MachineTblDao machineTblDao;
    @Autowired private MhaTblV2Dao mhaTblV2Dao;
    @Autowired private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired private DbReplicationTblDao dbReplicationTblDao;
    @Autowired private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired private DbTblDao dbTblDao;
    @Autowired private MigrationTaskTblDao migrationTaskTblDao;

    @Override
    public Long dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException {
        // meta info check and init
        checkDbMigrationParam(dbMigrationRequest);
        MhaTblV2 oldMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getOldMha());
        MhaTblV2 newMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getNewMha());
        
        List<String> migrateDbs = dbMigrationRequest.getDbs();
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        Set<MhaTblV2> otherMhaTbls = Sets.newHashSet();
        // find migrateDbs drcRelated and record otherMhaTblsInDrcReplication
        for (DbTbl migrateDbTbl : migrateDbTbls) {
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryByDbIdAndMhaId(migrateDbTbl.getId(), oldMhaTblV2.getId());
            if (mhaDbMappingTbl != null) {
                // DB_TO_DB In Src
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrc = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                // get another mhaTbl in dbReplicationTbl
                List<MhaDbMappingTbl> anotherMhaDbMappingTblsInDest = mhaDbMappingTblDao.queryByIds(
                        dbReplicationTblsOldMhaInSrc.stream().map(DbReplicationTbl::getDstMhaDbMappingId)
                                .collect(Collectors.toList()));
                List<MhaTblV2> anotherMhaTblsInDest = mhaTblV2Dao.queryByIds(
                        anotherMhaDbMappingTblsInDest.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList()));
                
                otherMhaTbls.addAll(anotherMhaTblsInDest);
                
                // DB_TO_DB In Dest
                List<DbReplicationTbl> dbReplicationTblsOldMhaInDest = dbReplicationTblDao.queryByDestMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                // get another mhaTbl in dbReplicationTbl
                List<MhaDbMappingTbl> anotherMhaDbMappingTblsInSrc = mhaDbMappingTblDao
                        .queryByIds(dbReplicationTblsOldMhaInDest.stream().map(DbReplicationTbl::getSrcMhaDbMappingId)
                                .collect(Collectors.toList()));
                List<MhaTblV2> anotherMhaTblsInSrc = mhaTblV2Dao.queryByIds(
                        anotherMhaDbMappingTblsInSrc.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList()));
                otherMhaTbls.addAll(anotherMhaTblsInSrc);
                
                // DB_TO_MQ
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrcMQ = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_MQ.getType());
                
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest) 
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)) {
                    migrateDbTblsDrcRelated.add(migrateDbTbl);
                }
            }

        }
        //  no dbRelated in drcReplication
        if (CollectionUtils.isEmpty(migrateDbTblsDrcRelated)) {
            return null; 
        }
        
        // check case1:migrate dbs effect multi mha-Replication in same region is not allowed;
        Map<String, List<MhaTblV2>> mhaTblsByRegion = groupByRegion(Lists.newArrayList(otherMhaTbls));
        mhaTblsByRegion.forEach((region, mhaTbls) -> {
            if (mhaTbls.size() > 1) {
                String mhasInSameRegion = mhaTbls.stream().map(MhaTblV2::getMhaName).collect(Collectors.joining(",")); 
                throw new ConsoleException(region + " effect multi mhaTbs in drcReplication, please check! mha: " + mhasInSameRegion);
            }
        });
        
        // check case2:newMha and oldMha have common mha in Replication is not allowed;
        List<MhaReplicationTbl> oldMhaReplications = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(oldMhaTblV2.getId()));
        List<MhaReplicationTbl> newMhaReplications = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(newMhaTblV2.getId()));
        if (!CollectionUtils.isEmpty(oldMhaReplications) && !CollectionUtils.isEmpty(newMhaReplications)) {
            Set<Long> anotherMhaIdsInOld = getAnotherMhaIds(oldMhaReplications, oldMhaTblV2.getId());
            Set<Long> anotherMhaIdsInNew = getAnotherMhaIds(newMhaReplications, newMhaTblV2.getId());
            
            anotherMhaIdsInNew.retainAll(anotherMhaIdsInOld);
            if (!CollectionUtils.isEmpty(anotherMhaIdsInNew)) {
                List<MhaTblV2> commonMhas = mhaTblV2Dao.queryByPk(Lists.newArrayList(anotherMhaIdsInNew));
                String commonMhaNames = commonMhas.stream().map(MhaTblV2::getMhaName).collect(Collectors.joining(","));
                throw new ConsoleException("newMha and oldMha have common mha in Replication, please check! commomMhas: " + commonMhaNames);
            }
        }
        
        // init and insert task
        MigrationTaskTbl migrationTaskTbl = new MigrationTaskTbl();
        migrationTaskTbl.setDbs(JsonUtils.toJson(dbMigrationRequest.getDbs()));
        migrationTaskTbl.setOldMha(oldMhaTblV2.getMhaName());
        migrationTaskTbl.setNewMha(newMhaTblV2.getMhaName());
        migrationTaskTbl.setOldMhaDba(dbMigrationRequest.getOldMha().getName());
        migrationTaskTbl.setNewMhaDba(dbMigrationRequest.getNewMha().getName());
        migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
        migrationTaskTbl.setOperator(dbMigrationRequest.getOperator());
        return migrationTaskTblDao.insertWithReturnId(migrationTaskTbl);
    }

    @Override
    public boolean startDbMigrationTask(Long taskId) throws SQLException {
        // 1. check task status
        // 2. mha config check
        // 3. start task thread
            // 3.1 find related replication
            // 3.2 copy config form old mha to new mha
            // 3.3 push new meta config to CM
            // 3.4 update task status

//        MigrateTaskTbl migrateTaskTbl = migrateTaskTblDao.queryByPk(taskId);
//        if (migrateTaskTbl == null) {
//            throw new ConsoleException("migrateTask not exist! taskId: " + taskId);
//        }
//        if (migrateTaskTbl.getStatus() != MigrateStatusEnum.INIT.getStatus()) {
//            throw new ConsoleException("migrateTask status is not INIT, can not start! taskId: " + taskId);
//        }
//        migrateTaskTbl.setStatus(MigrateStatusEnum.RUNNING.getStatus());
//        migrateTaskTblDao.update(migrateTaskTbl);
//
//        // start task
//        MigrateTask migrateTask = new MigrateTask(migrateTaskTbl);
//        migrateTask.start();

        return true;
    }

    private Set<Long> getAnotherMhaIds(List<MhaReplicationTbl> mhaReplications, Long mhaId) {
        Set<Long> anotherMhaIds = Sets.newHashSet();
        for (MhaReplicationTbl mhaReplication : mhaReplications) {
            if (mhaReplication.getSrcMhaId().equals(mhaId)) {
                anotherMhaIds.add(mhaReplication.getDstMhaId());
            } else {
                anotherMhaIds.add(mhaReplication.getSrcMhaId());
            }
        }
        return anotherMhaIds;
    }
    
    private Map<String, List<MhaTblV2>> groupByRegion(List<MhaTblV2> mhaTblV2s) {
        Map<String, List<MhaTblV2>> mhaTblV2sByRegion = Maps.newHashMap();
        Map<Long,String> dcId2RegionNameMap = metaInfoServiceV2.queryAllDcWithCache().stream().collect(Collectors.toMap(
                DcDo::getDcId, DcDo::getRegionName));
        for (MhaTblV2 mhaTblV2 : mhaTblV2s) {
            String regionName = dcId2RegionNameMap.get(mhaTblV2.getDcId());
            List<MhaTblV2> mhaTblV2sInRegion = mhaTblV2sByRegion.computeIfAbsent(regionName, k -> Lists.newArrayList());
            mhaTblV2sInRegion.add(mhaTblV2);
        }
        return mhaTblV2sByRegion;
    }
    
    private MhaTblV2 checkAndInitMhaInfo(MigrateMhaInfo mhaInfo) throws SQLException {
        MhaTblV2 mhaTblV2;
        MachineTbl mhaMaterNode = machineTblDao.queryByIpPort(mhaInfo.getMasterIp(), mhaInfo.getMasterPort());
        if (mhaMaterNode == null) {
            mhaTblV2 = syncMhaInfoFormDBAAPI(mhaInfo);
        } else if (mhaMaterNode.getMaster().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message(mhaInfo.getName() + ",masterNode metaInfo not match,Please contact DRC team!");
        } else {
            Long mhaId = mhaMaterNode.getMhaId();
            mhaTblV2 = mhaTblV2Dao.queryByPk(mhaId);
            String newMhaNameInDrc = mhaTblV2.getMhaName();
            if (!newMhaNameInDrc.equals(mhaInfo.getName())) {
                logger.warn("drcMha:{},dbRequestMha:{},mhaName not match....", newMhaNameInDrc, mhaInfo.getName());
            }
        }
        return mhaTblV2;
    }

    private MhaTblV2 syncMhaInfoFormDBAAPI(MigrateMhaInfo newMha) {
        // todo api: syncMhaInfoFormDBAAPI
        return null;
    }

    private void checkDbMigrationParam(DbMigrationParam dbMigrationRequest) {
        PreconditionUtils.checkNotNull(dbMigrationRequest, "dbMigrationRequest is null");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getOldMha(), "oldMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getName() , "oldMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getMasterIp(), "oldMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getOldMha().getMasterPort() != 0, "oldMha masterPort is 0");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getNewMha(), "newMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getName() , "newMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getMasterIp(), "newMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getNewMha().getMasterPort() != 0, "newMha masterPort is 0");

        PreconditionUtils.checkCollection(dbMigrationRequest.getDbs(), "dbs is empty");
    }



}
