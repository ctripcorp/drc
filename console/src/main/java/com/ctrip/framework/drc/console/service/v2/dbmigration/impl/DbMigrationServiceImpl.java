package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_DB;
import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_MQ;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
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
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
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
    @Autowired private MhaDbMappingService mhaDbMappingService;
    
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

        // find migrateDbs drcRelated and record otherMhaTblsInDrcReplication
        List<String> migrateDbs = dbMigrationRequest.getDbs();
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTbls =  Lists.newArrayList();
        for (DbTbl migrateDbTbl : migrateDbTbls) {
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryByDbIdAndMhaId(migrateDbTbl.getId(), oldMhaTblV2.getId());
            if (mhaDbMappingTbl != null) {
                // DB_TO_DB In Src
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrc = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInDest = getAnotherMhaTblsInDest(dbReplicationTblsOldMhaInSrc);
                otherMhaTbls.addAll(anotherMhaTblsInDest);
                
                // DB_TO_DB In Dest
                List<DbReplicationTbl> dbReplicationTblsOldMhaInDest = dbReplicationTblDao.queryByDestMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInSrc = getAnotherMhaTblsInSrc(dbReplicationTblsOldMhaInDest);
                otherMhaTbls.addAll(anotherMhaTblsInSrc);
                
                // DB_TO_MQ
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrcMQ = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_MQ.getType());
                
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc) 
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
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean exStartDbMigrationTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        String oldMha = migrationTaskTbl.getOldMha();
        String newMha = migrationTaskTbl.getNewMha();
        String status = migrationTaskTbl.getStatus();
        String dbs = migrationTaskTbl.getDbs();
        
        // 1. check status can exStart? todo
        // 2. check mha config newMhaConfig should equal newMhaTbl todo
        // 3. for each db
            // 3.1 find dbReplications and mqReplications in oldMha
            // 3.2 init dbMhaMappingTbls, copy dbReplications and mqReplications to newMha
            // 3.3 init mhaReplicationTbls,replicatorGroups,applierGroupTbls,messengerGroupTbls
            // 3.4 auto chose replicators
        // 4. update task status todo
        
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMha);
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMha);
        List<String> migrateDbs = JsonUtils.fromJsonToList(dbs, String.class);
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);


        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();
        
        for (DbTbl migrateDbTbl : migrateDbTbls) {
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryByDbIdAndMhaId(migrateDbTbl.getId(), oldMhaTbl.getId());
            if (mhaDbMappingTbl != null) {
                // DB_TO_DB OldMha In Src // todo npe?
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrc = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInDest = getAnotherMhaTblsInDest(dbReplicationTblsOldMhaInSrc);
                otherMhaTblsInDest.addAll(anotherMhaTblsInDest);
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)) {
                    dbReplicationTblsInOldMhaInSrcPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInSrc));
                }
                
                // DB_TO_DB OldMha In Dest
                List<DbReplicationTbl> dbReplicationTblsOldMhaInDest = dbReplicationTblDao.queryByDestMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInSrc = getAnotherMhaTblsInSrc(dbReplicationTblsOldMhaInDest);
                otherMhaTblsInSrc.addAll(anotherMhaTblsInSrc);
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)) {
                    dbReplicationTblsInOldMhaInDestPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInDest));
                }
                
                // DB_TO_MQ in OldMha
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrcMQ = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_MQ.getType());
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)) {
                    db2MqReplicationTblsInOldMhaPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInSrcMQ));
                }
                
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest) 
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)) {
                    migrateDbTblsDrcRelated.add(migrateDbTbl);
                }
            }
        }
        
        // init mhaDBMappingTbls about newMha and migrateDbTbls
        Map<Long, MhaDbMappingTbl> dbIdAndmhaDbMappingMapInNewMha = initMhaDbMappingTblsInNewMha(newMhaTbl, migrateDbTblsDrcRelated);
        
        // copy dbReplications and mqReplications to newMha
        initDbReplicationTblsInNewMha(
                newMhaTbl, 
                dbIdAndmhaDbMappingMapInNewMha,
                dbReplicationTblsInOldMhaInSrcPairs,
                dbReplicationTblsInOldMhaInDestPairs, 
                db2MqReplicationTblsInOldMhaPairs
        );
        
        //init mhaReplicationTbls,replicatorGroups,applierGroupTbls,messengerGroupTbls
//        initMhaReplicationTblsInNewMha(newMhaTbl);
//        initReplicatorGroupTblsInNewMha(newMhaTbl);
//        initApplierGroupTblsInNewMha(newMhaTbl);
//        initMessengerGroupTblsInNewMha(newMhaTbl);
        
        //auto chose replicators
        
        // generate newMha dbcluster and push to clusterManager
        return false;
    }

    // return map of dbId and mhaDbMappingTbl
    private Map<Long,MhaDbMappingTbl> initMhaDbMappingTblsInNewMha(MhaTblV2 newMhaTbl,List<DbTbl> migrateDbTblsDrcRelated) throws SQLException {
        mhaDbMappingService.buildMhaDbMappings(newMhaTbl.getMhaName(),migrateDbTblsDrcRelated.stream().map(DbTbl::getDbName).collect(Collectors.toList()));
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(newMhaTbl.getId());
        return  mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, Function.identity()));
    }


    // before dbMigration, dbMigrationdbs' dbReplicationTbls & mqReplicationTbls should not exist in newMha
    private void initDbReplicationTblsInNewMha(MhaTblV2 newMhaTbl,
            Map<Long,MhaDbMappingTbl> dbIdAndmhaDbMappingMapInNewMha,
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs, 
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs, 
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs) throws SQLException {

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryMappingIds(
                dbIdAndmhaDbMappingMapInNewMha.values().stream().map(MhaDbMappingTbl::getId)
                        .collect(Collectors.toList()));
        if (!CollectionUtils.isEmpty(dbReplicationTbls)) {
            throw ConsoleExceptionUtils.message("dbReplicationTbls already exist in newMha:" + newMhaTbl.getMhaName() + ",please contact drcTeam!");
        }

        
        List<DbReplicationTbl> dbReplicationTblsToBeInsert = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(dbReplicationTblsInOldMhaInSrcPairs)) {
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> dbReplicationTblsInOldMhaInSrcPair : dbReplicationTblsInOldMhaInSrcPairs) {
                MhaDbMappingTbl dbMappingTblInOldMha = dbReplicationTblsInOldMhaInSrcPair.getLeft();
                List<DbReplicationTbl> dbReplicationTblsInOldMhaInSrc = dbReplicationTblsInOldMhaInSrcPair.getRight();
                Long dbId = dbMappingTblInOldMha.getDbId();
                MhaDbMappingTbl mhaDbMappingTblInNewMha = dbIdAndmhaDbMappingMapInNewMha.get(dbId);
                if (mhaDbMappingTblInNewMha == null) {
                    throw ConsoleExceptionUtils.message("dbId:" + dbId + " not in newMha drcMetaDB,please contact drcTeam!");
                }
                // dbReplicationTblsInOldMhaInSrc -> dbReplicationTblsInNewMhaInSrc
                dbReplicationTblsInOldMhaInSrc.stream().forEach(dbReplicationTblInOldMhaInSrc -> {
                    DbReplicationTbl dbReplicationTbl = copyDbReplicationTbl(dbReplicationTblInOldMhaInSrc);
                    dbReplicationTbl.setSrcMhaDbMappingId(mhaDbMappingTblInNewMha.getId());
                    dbReplicationTblsToBeInsert.add(dbReplicationTbl);
                });
            }
        }
        if (!CollectionUtils.isEmpty(dbReplicationTblsInOldMhaInDestPairs)) {
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> dbReplicationTblsInOldMhaInDestPair : dbReplicationTblsInOldMhaInDestPairs) {
                MhaDbMappingTbl dbMappingTblInOldMha = dbReplicationTblsInOldMhaInDestPair.getLeft();
                List<DbReplicationTbl> dbReplicationTblsInOldMhaInDest = dbReplicationTblsInOldMhaInDestPair.getRight();
                Long dbId = dbMappingTblInOldMha.getDbId();
                MhaDbMappingTbl mhaDbMappingTblInNewMha = dbIdAndmhaDbMappingMapInNewMha.get(dbId);
                if (mhaDbMappingTblInNewMha == null) {
                    throw ConsoleExceptionUtils.message("dbId:" + dbId + " not in newMha drcMetaDB,please contact drcTeam!");
                }
                // dbReplicationTblsInOldMhaInDest -> dbReplicationTblsInNewMhaInDest
                dbReplicationTblsInOldMhaInDest.stream().forEach(dbReplicationTblInOldMhaInDest -> {
                    DbReplicationTbl dbReplicationTbl = copyDbReplicationTbl(dbReplicationTblInOldMhaInDest);
                    dbReplicationTbl.setDstMhaDbMappingId(mhaDbMappingTblInNewMha.getId());
                    dbReplicationTblsToBeInsert.add(dbReplicationTbl);
                });
            }
        }
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> db2MqReplicationTblsOldMhaPair : db2MqReplicationTblsInOldMhaPairs) {
                MhaDbMappingTbl dbMappingTblInOldMha = db2MqReplicationTblsOldMhaPair.getLeft();
                List<DbReplicationTbl> db2MqReplicationTblsOldMha = db2MqReplicationTblsOldMhaPair.getRight();
                Long dbId = dbMappingTblInOldMha.getDbId();
                MhaDbMappingTbl mhaDbMappingTblInNewMha = dbIdAndmhaDbMappingMapInNewMha.get(dbId);
                if (mhaDbMappingTblInNewMha == null) {
                    throw ConsoleExceptionUtils.message("dbId:" + dbId + " not in newMha drcMetaDB,please contact drcTeam!");
                }
                // db2MqReplicationTblsOldMha -> db2MqReplicationTblsInNewMha
                db2MqReplicationTblsOldMha.stream().forEach(dbReplicationTblInOldMhaInDest -> {
                    DbReplicationTbl dbReplicationTbl = copyDbReplicationTbl(dbReplicationTblInOldMhaInDest);
                    dbReplicationTbl.setSrcMhaDbMappingId(mhaDbMappingTblInNewMha.getId());
                    dbReplicationTblsToBeInsert.add(dbReplicationTbl);
                });
            }
        }
        
//        if (!CollectionUtils.isEmpty(dbReplicationTblsToBeInsert)) {
//            int[] ids = dbReplicationTblDao.insertWithKeyHolder(new KeyHolder(),dbReplicationTblsToBeInsert);
//            logger.info("[[migration=copyReplication,newMha={}]] copy size:{} ,effectSize:{}", newMhaTbl.getMhaName(),
//                    dbReplicationTblsToBeInsert.size(), Arrays.stream(effects).sum());
//        }
        // copy db_replication_filter_mapping_tbl todo 
        
        
    }
    
    
    private void initMhaReplicationTblsInNewMha(MhaTblV2 newMhaTbl,List<MhaTblV2> otherMhaTblsInSrc,List<MhaTblV2> otherMhaTblsInDest) throws SQLException {
        List<MhaReplicationTbl> mhaReplicationTblsToBeInsert = Lists.newArrayList();
        List<MhaReplicationTbl> mhaReplicationTblsInNewMha = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(newMhaTbl.getId()));
        
        // init mhaReplicationTblsInNewMha
        for (MhaTblV2 otherMhaTblInSrc : otherMhaTblsInSrc) {
            MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
            mhaReplicationTbl.setSrcMhaId(otherMhaTblInSrc.getId());
            mhaReplicationTbl.setDstMhaId(newMhaTbl.getId());
            mhaReplicationTblsToBeInsert.add(mhaReplicationTbl);
        }
        for (MhaTblV2 otherMhaTblInDest : otherMhaTblsInDest) {
            MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
            mhaReplicationTbl.setSrcMhaId(newMhaTbl.getId());
            mhaReplicationTbl.setDstMhaId(otherMhaTblInDest.getId());
            mhaReplicationTblsToBeInsert.add(mhaReplicationTbl);
        }
        if (!CollectionUtils.isEmpty(mhaReplicationTblsToBeInsert)) {
            int[] effects = mhaReplicationTblDao.batchInsert(mhaReplicationTblsToBeInsert);
            logger.info("[[migration=copyReplication,newMha={}]] copy size:{} ,effectSize:{}", newMhaTbl.getMhaName(),
                    mhaReplicationTblsToBeInsert.size(), Arrays.stream(effects).sum());
        }
        
    }
    
    private void initReplicatorGroupTblsInNewMha(MhaTblV2 newMhaTbl) throws SQLException {
        
    }

    private void initApplierGroupTblsInNewMha(MhaTblV2 newMhaTbl,List<MhaTblV2> otherMhaTblsInSrc,List<MhaTblV2> otherMhaTblsInDest) throws SQLException {
    }

    private void initMessengerGroupTblsInNewMha(MhaTblV2 newMhaTbl) throws SQLException {
        
    }
    
    private DbReplicationTbl copyDbReplicationTbl(DbReplicationTbl dbReplicationTbl) {
        DbReplicationTbl dbReplicationTblCopy = new DbReplicationTbl();
        dbReplicationTblCopy.setSrcMhaDbMappingId(dbReplicationTbl.getSrcMhaDbMappingId());
        dbReplicationTblCopy.setSrcLogicTableName(dbReplicationTbl.getSrcLogicTableName());
        dbReplicationTblCopy.setDstMhaDbMappingId(dbReplicationTbl.getDstMhaDbMappingId());
        dbReplicationTblCopy.setDstLogicTableName(dbReplicationTbl.getDstLogicTableName());
        dbReplicationTblCopy.setReplicationType(dbReplicationTbl.getReplicationType());
        dbReplicationTblCopy.setDeleted(dbReplicationTbl.getDeleted());
        return dbReplicationTblCopy;
    }
    
    
    @Override
    public boolean startDbMigrationTask(Long taskId) throws SQLException {
        // 1. check task status todo
        // 2. get applierGroup,messengerGroup should start todo 
        // 3. auto chose applier,messenger todo
        // 4. generate dbClusters to CM todo
        // 5. update task status todo 
        

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

    private List<MhaTblV2> getAnotherMhaTblsInDest(List<DbReplicationTbl> dbReplicationTblsMhaInSrc) throws SQLException{
        List<MhaDbMappingTbl> anotherMhaDbMappingTblsInDest = mhaDbMappingTblDao.queryByIds(
                dbReplicationTblsMhaInSrc.stream().map(DbReplicationTbl::getDstMhaDbMappingId).collect(Collectors.toList()));
        return mhaTblV2Dao.queryByIds(
                anotherMhaDbMappingTblsInDest.stream().map(MhaDbMappingTbl::getMhaId).collect(Collectors.toList()));
    }

    private List<MhaTblV2> getAnotherMhaTblsInSrc(List<DbReplicationTbl> dbReplicationTblsMhaInDest) throws SQLException{
        List<MhaDbMappingTbl> anotherMhaDbMappingTblsInSrc = mhaDbMappingTblDao
                .queryByIds(dbReplicationTblsMhaInDest.stream().map(DbReplicationTbl::getSrcMhaDbMappingId)
                        .collect(Collectors.toList()));
        return mhaTblV2Dao.queryByIds(anotherMhaDbMappingTblsInSrc.stream().map(MhaDbMappingTbl::getMhaId)
                        .collect(Collectors.toList()));
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
        mhaTblV2s = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, mhaTblV2 -> mhaTblV2, (m1, m2) -> m1))
                .values().stream().collect(Collectors.toList());
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
        PreconditionUtils.checkCollection(dbMigrationRequest.getDbs(), "dbs is empty");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getOldMha(), "oldMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getName() , "oldMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getMasterIp(), "oldMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getOldMha().getMasterPort() != 0, "oldMha masterPort is 0");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getNewMha(), "newMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getName() , "newMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getMasterIp(), "newMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getNewMha().getMasterPort() != 0, "newMha masterPort is 0");
    }

    @Override
    public PageResult<MigrationTaskTbl> queryByPage(MigrationTaskQuery query) {
        try {
            List<MigrationTaskTbl> data = migrationTaskTblDao.queryByPage(query);
            int count = migrationTaskTblDao.count(query);
            return PageResult.newInstance(data, query.getPageIndex(), query.getPageSize(), count);
        } catch (SQLException e) {
            logger.error("queryByPage error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

}
