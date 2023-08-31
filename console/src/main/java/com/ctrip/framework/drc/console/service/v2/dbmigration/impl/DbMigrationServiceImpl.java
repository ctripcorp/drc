package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_DB;
import static com.ctrip.framework.drc.console.enums.ReplicationTypeEnum.DB_TO_MQ;

/**
 * @ClassName DbMigrationServiceImpl
 * @Author haodongPan
 * @Date 2023/8/14 14:58
 * @Version: $
 */
@Service
public class DbMigrationServiceImpl implements DbMigrationService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private MachineTblDao machineTblDao;
    @Autowired
    private MigrationTaskTblDao migrationTaskTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private MetaGeneratorV3 metaGeneratorV3;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    
    
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;

    private RegionConfig regionConfig = RegionConfig.getInstance();

    private static final String BASE_API_URL = "/api/meta/clusterchange/%s/?operator={operator}";

    @Override
    public Long dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException {
        // meta info check and init
        checkDbMigrationParam(dbMigrationRequest);
        MhaTblV2 oldMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getOldMha());
        MhaTblV2 newMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getNewMha());
        
        List<String> migrateDbs = dbMigrationRequest.getDbs();
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        if (migrateDbs.size() != migrateDbTbls.size()) {
            throw new ConsoleException("unknown db in drc, request size: " + migrateDbs.size() + " match size: " + migrateDbTbls.size());
        }
        
        // find migrateDbs drcRelated and record otherMhaTblsInDrcReplication
        ReplicationInfo replicationInfoInOldMha = getReplicationInfoInOldMha(migrateDbTbls, oldMhaTblV2);
        List<DbTbl> migrateDbTblsDrcRelated = replicationInfoInOldMha.migrateDbTblsDrcRelated;
        List<MhaTblV2> otherMhaTbls = Lists.newArrayList();
        otherMhaTbls.addAll(replicationInfoInOldMha.otherMhaTblsInSrc);
        otherMhaTbls.addAll(replicationInfoInOldMha.otherMhaTblsInDest);
        
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
        oldMhaReplications = oldMhaReplications.stream().filter(mhaReplicationTbl -> mhaReplicationTbl.getDrcStatus() == 1).collect(Collectors.toList());
        newMhaReplications = newMhaReplications.stream().filter(mhaReplicationTbl -> mhaReplicationTbl.getDrcStatus() == 1).collect(Collectors.toList());
        
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
    public boolean exStartDbMigrationTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        String oldMha = migrationTaskTbl.getOldMha();
        String newMha = migrationTaskTbl.getNewMha();
        String status = migrationTaskTbl.getStatus();
        String dbs = migrationTaskTbl.getDbs();
        // migrationTask status check by lifeCycle 
        if (!MigrationStatusEnum.INIT.getStatus().equals(status)) { 
            throw new ConsoleException("task status is not INIT, can not exStart! taskId: " + taskId);
        }
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMha);
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMha);
        List<String> migrateDbs = JsonUtils.fromJsonToList(dbs, String.class);
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        
        // check mha config newMhaConfig should equal newMhaTbl
        Map<String, Object> configInOldMha = mysqlServiceV2.preCheckMySqlConfig(oldMha);
        Map<String, Object> configInNewMha = mysqlServiceV2.preCheckMySqlConfig(newMha);

        MapDifference<String, Object> configsDiff = Maps.difference(configInOldMha,configInNewMha);
        if (!configsDiff.areEqual()) { // todo  添加一个开关？
            Map<String, ValueDifference<Object>> valueDiff = configsDiff.entriesDiffering();
            String diff = valueDiff.entrySet().stream().map(
                    entry -> "config:" + entry.getKey() + 
                    ",oldMha:" + entry.getValue().leftValue() + 
                    ",newMha:" + entry.getValue().rightValue()).collect(Collectors.joining(";"));
            throw new ConsoleException("MhaConfigs not equals!" + diff);
        } 
        
        ReplicationInfo replicationInfoInOldMha = getReplicationInfoInOldMha(migrateDbTbls, oldMhaTbl);
        List<DbTbl> migrateDbTblsDrcRelated  = replicationInfoInOldMha.migrateDbTblsDrcRelated;
        List<MhaTblV2> otherMhaTblsInSrc = replicationInfoInOldMha.otherMhaTblsInSrc;
        List<MhaTblV2> otherMhaTblsInDest = replicationInfoInOldMha.otherMhaTblsInDest;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = 
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInSrcPairs;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = 
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInDestPairs;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = 
                replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs;
        
        // init mhaDBMappingTbls about newMha and migrateDbTbls
        Map<Long, MhaDbMappingTbl> dbId2MhaDbMappingMapInNewMha = initMhaDbMappingTblsInNewMha(newMhaTbl, migrateDbTblsDrcRelated);
        
        // copy dbReplications and their configTbls  to newMha
        initDbReplicationTblsInNewMha(
                newMhaTbl,
                dbId2MhaDbMappingMapInNewMha,
                dbReplicationTblsInOldMhaInSrcPairs,
                dbReplicationTblsInOldMhaInDestPairs, 
                db2MqReplicationTblsInOldMhaPairs
        );
        
        // init mhaReplicationTbls,replicatorGroups,applierGroupTbls,messengerGroupTbls
        initMhaReplicationsAndApplierGroups(newMhaTbl,otherMhaTblsInSrc,otherMhaTblsInDest);
        initReplicatorGroupAndMessengerGroup(newMhaTbl,db2MqReplicationTblsInOldMhaPairs);
        
        //auto chose replicators
        drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(newMhaTbl);
        
        // generate newMha dbcluster and push to clusterManager
        try {
            pushConfigToCM(Lists.newArrayList(newMhaTbl.getId()),migrationTaskTbl.getOperator(),HttpRequestEnum.POST);
        } catch (Exception e) {
            logger.warn("[[migration=exStarting,newMha={}]] task:{} pushConfigToCM fail!", newMhaTbl.getMhaName(),taskId,e);
        }
        // todo optimize: migrationTaskManager schedule check replicator slave delay to update status
        migrationTaskTbl.setStatus(MigrationStatusEnum.EX_STARTED.getStatus());
        migrationTaskTblDao.update(migrationTaskTbl);
        logger.info("[[migration=exStarting,newMha={}]] task:{} exStarting!", newMhaTbl.getMhaName(),taskId);
        return true;
    }
    
    @Override
    public boolean startDbMigrationTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        if (!MigrationStatusEnum.EX_STARTED.getStatus().equals(migrationTaskTbl.getStatus())) { // migrationTaskManager
            throw new ConsoleException("task status is not exStarted, can not start! taskId: " + taskId);
        }
        
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(migrationTaskTbl.getOldMha());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(migrationTaskTbl.getNewMha());
        List<String> migrateDbs = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);

        ReplicationInfo replicationInfoInOldMha = getReplicationInfoInOldMha(migrateDbTbls, oldMhaTbl);
        List<MhaTblV2> otherMhaTblsInSrc = replicationInfoInOldMha.otherMhaTblsInSrc;
        List<MhaTblV2> otherMhaTblsInDest = replicationInfoInOldMha.otherMhaTblsInDest;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs =
                replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs;
        
        // start appliers 
        if (!CollectionUtils.isEmpty(otherMhaTblsInSrc)) {
            for (MhaTblV2 mhaInSrc : otherMhaTblsInSrc) {
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(mhaInSrc.getId(),
                        newMhaTbl.getId(), BooleanEnum.FALSE.getCode());
                ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(
                        mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
                drcBuildServiceV2.autoConfigAppliersWithRealTimeGtid(mhaReplicationTbl, applierGroupTblV2, mhaInSrc, newMhaTbl);
            }
            
        }
        
        if (!CollectionUtils.isEmpty(otherMhaTblsInDest)) {
            for (MhaTblV2 mhaInDest : otherMhaTblsInDest) {
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(newMhaTbl.getId(),
                        mhaInDest.getId(), BooleanEnum.FALSE.getCode());
                ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(
                        mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
                drcBuildServiceV2.autoConfigAppliersWithRealTimeGtid(mhaReplicationTbl, applierGroupTblV2, newMhaTbl, mhaInDest);
            }
        }
        
        // start messengers
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(newMhaTbl);
        }
        
        try {
            // push to cm
            List<Long> mhaIdsStartRelated = Lists.newArrayList(newMhaTbl.getId());
            mhaIdsStartRelated.addAll(otherMhaTblsInSrc.stream().map(MhaTblV2::getId).collect(Collectors.toList()));
            mhaIdsStartRelated.addAll(otherMhaTblsInDest.stream().map(MhaTblV2::getId).collect(Collectors.toList()));
            pushConfigToCM(mhaIdsStartRelated,migrationTaskTbl.getOperator(),HttpRequestEnum.PUT);
        } catch (Exception e) {
            logger.warn("[[migration=starting,newMha={}]] task:{} pushConfigToCM fail!", newMhaTbl.getMhaName(),taskId);
        }

        // update task status
        migrationTaskTbl.setStatus(MigrationStatusEnum.STARTING.getStatus());
        migrationTaskTblDao.update(migrationTaskTbl);
        logger.info("[[migration=starting,newMha={}]] task:{} starting!", newMhaTbl.getMhaName(),taskId);
        return true;
    }
    
    
    private ReplicationInfo getReplicationInfoInOldMha(List<DbTbl> migrateDbTbls, MhaTblV2 oldMhaTbl) throws SQLException{
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();
        
        for (DbTbl migrateDbTbl : migrateDbTbls) {
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryByDbIdAndMhaId(migrateDbTbl.getId(), oldMhaTbl.getId());
            if (mhaDbMappingTbl != null) {
                // DB_TO_DB Replication OldMha In Src
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrc = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInDest = getAnotherMhaTblsInDest(dbReplicationTblsOldMhaInSrc);
                otherMhaTblsInDest.addAll(anotherMhaTblsInDest);
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)) {
                    dbReplicationTblsInOldMhaInSrcPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInSrc));
                }

                // DB_TO_DB Replication OldMha In Dest
                List<DbReplicationTbl> dbReplicationTblsOldMhaInDest = dbReplicationTblDao.queryByDestMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_DB.getType());
                List<MhaTblV2> anotherMhaTblsInSrc = getAnotherMhaTblsInSrc(dbReplicationTblsOldMhaInDest);
                otherMhaTblsInSrc.addAll(anotherMhaTblsInSrc);
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)) {
                    dbReplicationTblsInOldMhaInDestPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInDest));
                }

                // DB_TO_MQ Replication in OldMha
                List<DbReplicationTbl> dbReplicationTblsOldMhaInSrcMQ = dbReplicationTblDao.queryBySrcMappingIds(
                        Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_MQ.getType());
                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)) {
                    db2MqReplicationTblsInOldMhaPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInSrcMQ));
                }

                if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)
                        || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)) {
                    migrateDbTblsDrcRelated.add(migrateDbTbl);
                }
            }
        }
        otherMhaTblsInSrc = otherMhaTblsInSrc.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, mhaTblV2 -> mhaTblV2, (m1, m2) -> m1))
                .values().stream().collect(Collectors.toList());
        otherMhaTblsInDest = otherMhaTblsInDest.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, mhaTblV2 -> mhaTblV2, (m1, m2) -> m1))
                .values().stream().collect(Collectors.toList());
        
        ReplicationInfo replicationInfo = new ReplicationInfo();
        replicationInfo.otherMhaTblsInSrc = otherMhaTblsInSrc;
        replicationInfo.otherMhaTblsInDest = otherMhaTblsInDest;
        replicationInfo.migrateDbTblsDrcRelated = migrateDbTblsDrcRelated;
        replicationInfo.dbReplicationTblsInOldMhaInSrcPairs = dbReplicationTblsInOldMhaInSrcPairs;
        replicationInfo.dbReplicationTblsInOldMhaInDestPairs = dbReplicationTblsInOldMhaInDestPairs;
        replicationInfo.db2MqReplicationTblsInOldMhaPairs = db2MqReplicationTblsInOldMhaPairs;
        return replicationInfo;
    }

    private void initReplicatorGroupAndMessengerGroup(MhaTblV2 newMhaTbl,
            List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs) throws SQLException {
        Long replicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(newMhaTbl.getId());
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            Long mGroupId = messengerGroupTblDao.upsertIfNotExist(replicatorGroupId, newMhaTbl.getId(), null);
            logger.info("[[migration=exStarting,newMha={}]] initReplicatorGroup:{},initMessengerGroup:{}", newMhaTbl.getMhaName(),
                    replicatorGroupId,mGroupId);
        }
    }

    private void initMhaReplicationsAndApplierGroups(MhaTblV2 newMhaTbl, List<MhaTblV2> otherMhaTblsInSrc, List<MhaTblV2> otherMhaTblsInDest) throws SQLException {
        for (MhaTblV2 otherMhaTblInSrc : otherMhaTblsInSrc) {
            Long mhaReplicationId = mhaReplicationTblDao.insertOrReCover(otherMhaTblInSrc.getId(), newMhaTbl.getId());
            Long applierGroupId = applierGroupTblV2Dao.insertOrReCover(mhaReplicationId, null);
            logger.info("[[migration=exStarting,newMha={}]] initMhaReplicaton:{},initApplierGroup:{}", newMhaTbl.getMhaName(),
                    mhaReplicationId,applierGroupId);
        }
        for (MhaTblV2 otherMhaTblInDest : otherMhaTblsInDest) {
            Long mhaReplicationId = mhaReplicationTblDao.insertOrReCover(newMhaTbl.getId(), otherMhaTblInDest.getId());
            Long applierGroupId = applierGroupTblV2Dao.insertOrReCover(mhaReplicationId, null);
            logger.info("[[migration=exStarting,newMha={}]] initMhaReplicaton:{},initApplierGroup:{}", newMhaTbl.getMhaName(),
                    mhaReplicationId,applierGroupId);
        }
    }

    // return map of dbId and mhaDbMappingTbl
    private Map<Long,MhaDbMappingTbl> initMhaDbMappingTblsInNewMha(MhaTblV2 newMhaTbl,List<DbTbl> migrateDbTblsDrcRelated) throws SQLException {
        mhaDbMappingService.buildMhaDbMappings(newMhaTbl.getMhaName(),migrateDbTblsDrcRelated.stream().map(DbTbl::getDbName).collect(Collectors.toList()));
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(newMhaTbl.getId());
        return  mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, Function.identity()));
    }
    
    
    // before dbMigration, dbMigrationdbs' dbReplicationTbls & mqReplicationTbls should not exist in newMha
    private void initDbReplicationTblsInNewMha(MhaTblV2 newMhaTbl,
            Map<Long,MhaDbMappingTbl> dbId2mhaDbMappingMapInNewMha,
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs, 
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs, 
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs) throws SQLException {

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryMappingIds(
                dbId2mhaDbMappingMapInNewMha.values().stream().map(MhaDbMappingTbl::getId)
                        .collect(Collectors.toList()));
        if (!CollectionUtils.isEmpty(dbReplicationTbls)) {
            throw ConsoleExceptionUtils.message("dbReplicationTbls already exist in newMha:" + newMhaTbl.getMhaName() + ",please contact drcTeam!");
        }
        
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha,dbReplicationTblsInOldMhaInSrcPairs,true);
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha,dbReplicationTblsInOldMhaInDestPairs,false);
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha,db2MqReplicationTblsInOldMhaPairs,true);
        
    }
    
    private void initDbReplicationsAndConfigsTbls(MhaTblV2 newMhaTbl, Map<Long,MhaDbMappingTbl> dbId2mhaDbMappingMapInNewMha, 
            List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaPairs,boolean isInSrc) throws SQLException {
        
        if (!CollectionUtils.isEmpty(dbReplicationTblsInOldMhaPairs)) {
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> dbReplicationTblsInOldMhaPair : dbReplicationTblsInOldMhaPairs) { // every db
                MhaDbMappingTbl dbMappingTblInOldMha = dbReplicationTblsInOldMhaPair.getLeft();
                List<DbReplicationTbl> dbReplicationTblsInOldMha = dbReplicationTblsInOldMhaPair.getRight();
                Long dbId = dbMappingTblInOldMha.getDbId();
                MhaDbMappingTbl mhaDbMappingTblInNewMha = dbId2mhaDbMappingMapInNewMha.get(dbId);
                if (mhaDbMappingTblInNewMha == null) {
                    throw ConsoleExceptionUtils.message("dbId:" + dbId + " not in newMha drcMetaDB,please contact drcTeam!");
                }
                
                // dbReplicationTblsInOldMha -> dbReplicationTblsInNewMha
                List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao
                        .queryByDbReplicationIds(dbReplicationTblsInOldMha.stream().map(DbReplicationTbl::getId).collect(Collectors.toList()));
                Map<Long, DbReplicationFilterMappingTbl> dbReplicaId2FilterMappingMap = dbReplicationFilterMappingTbls.stream()
                        .collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, Function.identity()));
                
                for (DbReplicationTbl dbReplicationTblInOldMha : dbReplicationTblsInOldMha) { // every dbReplication
                    // copy dbReplicationTbl and insert
                    DbReplicationTbl dbReplicationTbl = copyDbReplicationTbl(dbReplicationTblInOldMha,mhaDbMappingTblInNewMha.getId(),isInSrc);
                    Long dbReplicationId = dbReplicationTblDao.insertWithReturnId(dbReplicationTbl);
                    
                    // copy DbReplicationFilterMappingTbl and insert
                    DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = dbReplicaId2FilterMappingMap.get(dbReplicationTblInOldMha.getId());
                    DbReplicationFilterMappingTbl copyFilter = copyDbReplicationFilterMappingTbl(
                            dbReplicationFilterMappingTbl, dbReplicationId);
                    Long filterMappingId = dbReplicationFilterMappingTblDao.insertWithReturnId(copyFilter);
                    logger.info("[[migration=exStarting,newMha={}]] copyDbReplication:{},filterMapping:id:{},{}", newMhaTbl.getMhaName(),
                            dbReplicationTbl,filterMappingId,copyFilter);
                }
            }
        }
    }

    private DbReplicationFilterMappingTbl copyDbReplicationFilterMappingTbl(DbReplicationFilterMappingTbl origin, Long dbReplicationId) {
        DbReplicationFilterMappingTbl copy = new DbReplicationFilterMappingTbl();
        copy.setDbReplicationId(dbReplicationId);
        copy.setColumnsFilterId(origin.getColumnsFilterId());
        copy.setRowsFilterId(origin.getRowsFilterId());
        copy.setMessengerFilterId(origin.getMessengerFilterId());
        copy.setDeleted(0);
        return copy;
    }
    
    private DbReplicationTbl copyDbReplicationTbl(DbReplicationTbl dbReplicationTbl,Long mhaDbMappingId,boolean isInSrc) {
        DbReplicationTbl dbReplicationTblCopy = new DbReplicationTbl();
        if (isInSrc) {
            dbReplicationTblCopy.setSrcMhaDbMappingId(mhaDbMappingId);
            dbReplicationTblCopy.setDstMhaDbMappingId(dbReplicationTbl.getDstMhaDbMappingId());
        } else {
            dbReplicationTblCopy.setSrcMhaDbMappingId(dbReplicationTbl.getSrcMhaDbMappingId());
            dbReplicationTblCopy.setDstMhaDbMappingId(mhaDbMappingId);
        }
        dbReplicationTblCopy.setSrcLogicTableName(dbReplicationTbl.getSrcLogicTableName());
        dbReplicationTblCopy.setDstLogicTableName(dbReplicationTbl.getDstLogicTableName());
        dbReplicationTblCopy.setReplicationType(dbReplicationTbl.getReplicationType());
        dbReplicationTblCopy.setDeleted(dbReplicationTbl.getDeleted());
        return dbReplicationTblCopy;
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

    @Override
    public void offlineOldDrcConfig(long taskId) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("offlineOldDrcConfig", String.valueOf(taskId));
        deleteDrcConfig(taskId, false);
    }

    @Override
    public void rollBackNewDrcConfig(long taskId) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("rollBackNewDrcConfig", String.valueOf(taskId));
        deleteDrcConfig(taskId, true);
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDrcConfig(long taskId, boolean rollBack) throws Exception {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
        if (migrationTaskTbl == null) {
            throw ConsoleExceptionUtils.message("taskId: " + taskId + " not exist!");
        }

        if (!migrationTaskTbl.getStatus().equals(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus())) {
            throw ConsoleExceptionUtils.message("task status is: " + migrationTaskTbl.getStatus() + ", not ready to continue");
        }

        String status = rollBack ? MigrationStatusEnum.FAIL.getStatus() : MigrationStatusEnum.SUCCESS.getStatus();
        migrationTaskTbl.setStatus(status);
        migrationTaskTblDao.update(migrationTaskTbl);

        String oldMhaName = migrationTaskTbl.getOldMha();
        String newMhaName = migrationTaskTbl.getNewMha();
        String operator = migrationTaskTbl.getOperator();
        List<String> dbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);

        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());

        List<MhaDbMappingTbl> relatedMhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(dbIds);
        List<Long> relatedMhaIds = relatedMhaDbMappingTbls.stream()
                .map(MhaDbMappingTbl::getMhaId)
                .filter(mhaId -> !mhaId.equals(oldMhaTbl.getId()) && !mhaId.equals(newMhaTbl.getId()))
                .distinct()
                .collect(Collectors.toList());
        logger.info("offlineDrcConfig relatedMhaIds: {}", relatedMhaIds);

        long mhaId = rollBack ? newMhaTbl.getId() : oldMhaTbl.getId();
        List<MhaDbMappingTbl> mhaDbMappings = mhaDbMappingTblDao.queryByMhaId(mhaId);

        deleteMqReplication(mhaId, mhaDbMappings, relatedMhaDbMappingTbls);

        for (long relateMhaId : relatedMhaIds) {
            deleteDbReplications(mhaId, relateMhaId, relatedMhaDbMappingTbls);
            deleteDbReplications(relateMhaId, mhaId, relatedMhaDbMappingTbls);

            //delete mhaReplication
            List<MhaDbMappingTbl> relatedMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(relateMhaId);
            List<DbReplicationTbl> existDbReplications = getExistDbReplications(mhaDbMappings, relatedMhaDbMappings);
            List<DbReplicationTbl> oppositeExistDbReplications = getExistDbReplications(relatedMhaDbMappings, mhaDbMappings);
            boolean deleted = CollectionUtils.isEmpty(existDbReplications) && CollectionUtils.isEmpty(oppositeExistDbReplications);
            if (CollectionUtils.isEmpty(existDbReplications)) {
                logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", mhaId, relateMhaId);
                deleteMhaReplication(mhaId, relateMhaId, deleted);
            }

            if (CollectionUtils.isEmpty(oppositeExistDbReplications)) {
                logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", relateMhaId, mhaId);
                deleteMhaReplication(relateMhaId, mhaId, deleted);
            }
        }

        List<MhaDbMappingTbl> deleteMhaDbMappingTbls = relatedMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).collect(Collectors.toList());
        mhaDbMappingTblDao.delete(deleteMhaDbMappingTbls);

        try {
            List<Long> mhaIds = Lists.newArrayList(relatedMhaIds);
            mhaIds.add(mhaId);
            pushConfigToCM(mhaIds, operator, HttpRequestEnum.PUT);
        } catch (Exception e) {
            logger.warn("pushConfigToCM failed, mhaIds: {}", relatedMhaIds, e);
        }
    }

    private void pushConfigToCM(List<Long> mhaIds, String operator, HttpRequestEnum httpRequestEnum) throws Exception {
        Map<String, String> cmRegionUrls = regionConfig.getCMRegionUrls();

        for (long mhaId : mhaIds) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
            String dbClusterId = mhaTblV2.getClusterName() + "." + mhaTblV2.getMhaName();
            String url = cmRegionUrls.get(dcTbl.getRegionName()) + String.format(BASE_API_URL, dbClusterId);

            Map<String, String> paramMap = new HashMap<>();
            paramMap.put("operator", operator);
            try {
                if (httpRequestEnum.equals(HttpRequestEnum.POST)) {
                    HttpUtils.post(url, null, ApiResult.class, paramMap);
                } else if (httpRequestEnum.equals(HttpRequestEnum.PUT)) {
                    HttpUtils.put(url, null, ApiResult.class, paramMap);
                }
            } catch (Exception e) {
                logger.error("pushConfigToCM fail: {}", e);
            }
        }
    }

    private DbCluster findDbCluster(Drc drc, MhaTblV2 mhaTblV2, String dcName) {
        Dc dc = drc.findDc(dcName);
        String dbClusterId = mhaTblV2.getClusterName() + "." + mhaTblV2.getMhaName();
        return dc.findDbCluster(dbClusterId);
    }

    private void deleteMqReplication(long mhaId, List<MhaDbMappingTbl> mhaDbMappingTbls, List<MhaDbMappingTbl> relatedMhaDbMappingTbls) throws Exception {
        deleteMqDbReplications(mhaId, relatedMhaDbMappingTbls);
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(mhaDbMappingTbls);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("oldMqDbReplications are empty, delete messenger mhaId: {}", mhaId);
            deleteMessengers(mhaId);
        }
    }

    private void deleteMessengers(long mhaId) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            logger.info("deleteMessengers messengerGroupTbl not exist, mhaId: {}", mhaId);
            return;
        }
        messengerGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("deleteMessengers mhaId: {}, messengerGroupTbl: {}", mhaId, messengerGroupTbl);
        messengerGroupTblDao.update(messengerGroupTbl);

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (!CollectionUtils.isEmpty(messengerTbls)) {
            messengerTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("deleteMessengers, mhaId: {}, messengerTbls: {}", messengerTbls);
            messengerTblDao.update(messengerTbls);
        }
    }

    private void deleteMqDbReplications(long mhaId, List<MhaDbMappingTbl> relatedMhaDbMappingTbls) throws Exception {
        List<MhaDbMappingTbl> srcMhaDbMappings = relatedMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).collect(Collectors.toList());
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(srcMhaDbMappings);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplicationTbl from mhaId: {} not exist", mhaId);
            return;
        }
        List<Long> mqDbReplicationIds = mqDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        mqDbReplications.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete mqDbReplicationTblIds: {}", mqDbReplicationIds);
        dbReplicationTblDao.update(mqDbReplications);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(mqDbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dbReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
    }

    private void deleteDbReplications(long srcMhaId, long dstMhaId, List<MhaDbMappingTbl> relatedMhaDbMappingTbls) throws Exception {
        logger.info("deleteDbReplications srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        List<MhaDbMappingTbl> srcMhaDbMappings = relatedMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(srcMhaId)).collect(Collectors.toList());
        List<MhaDbMappingTbl> dstMhaDbMappings = relatedMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(dstMhaId)).collect(Collectors.toList());
        List<DbReplicationTbl> dbReplicationTbls = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        if (CollectionUtils.isEmpty(dbReplicationTbls)) {
            logger.info("dbReplicationTbl from srcMhaId: {} to dstMhaId: {} not exist", srcMhaId, dstMhaId);
            return;
        }

        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        dbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete dbReplicationTblIds: {}", dbReplicationIds);
        dbReplicationTblDao.update(dbReplicationTbls);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dbReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
    }

    private void deleteMhaReplication(long srcMhaId, long dstMhaId, boolean deleted) throws Exception {
        logger.info("deleteMhaReplication srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId, BooleanEnum.FALSE.getCode());
        if (mhaReplicationTbl == null) {
            logger.info("mhaReplication from srcMhaId: {} to dstMhaId: {} not exist", srcMhaId, dstMhaId);
            return;
        }
        mhaReplicationTbl.setDrcStatus(BooleanEnum.FALSE.getCode());
        if (deleted) {
            mhaReplicationTbl.setDeleted(BooleanEnum.TRUE.getCode());
        }

        logger.info("update mhaReplication: {}", mhaReplicationTbl);
        mhaReplicationTblDao.update(mhaReplicationTbl);
        deleteApplierGroup(mhaReplicationTbl.getId());
    }

    private void deleteApplierGroup(long mhaReplicationId) throws Exception {
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationId, BooleanEnum.FALSE.getCode());
        if (applierGroupTblV2 == null) {
            logger.info("applierGroupTblV2 not exist, mhaReplicationId: {}", mhaReplicationId);
            return;
        }
        applierGroupTblV2.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("delete applierGroupTblV2: {}", applierGroupTblV2);
        applierGroupTblV2Dao.update(applierGroupTblV2);
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryByApplierGroupId(applierGroupTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(applierTblV2s)) {
            applierTblV2s.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("delete applierTblV2s: {}", applierTblV2s);
            applierTblV2Dao.update(applierTblV2s);
        }
    }

    private List<DbReplicationTbl> getExistDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());
        return existDbReplications;
    }

    private List<DbReplicationTbl> getExistMqDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryBySrcMappingIds(srcMhaDbMappingIds, ReplicationTypeEnum.DB_TO_MQ.getType());
        return existDbReplications;
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
        Map<Long, String> dcId2RegionNameMap = metaInfoServiceV2.queryAllDcWithCache().stream().collect(Collectors.toMap(
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
            mhaTblV2 = drcBuildServiceV2.syncMhaInfoFormDbaApi(mhaInfo.getName());
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
    
    private void checkDbMigrationParam(DbMigrationParam dbMigrationRequest) {
        PreconditionUtils.checkNotNull(dbMigrationRequest, "dbMigrationRequest is null");
        PreconditionUtils.checkCollection(dbMigrationRequest.getDbs(), "dbs is empty");
        PreconditionUtils.checkString(dbMigrationRequest.getOperator(),"operator is empty");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getOldMha(), "oldMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getName(), "oldMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getMasterIp(), "oldMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getOldMha().getMasterPort() != 0, "oldMha masterPort is 0");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getNewMha(), "newMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getName(), "newMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getMasterIp(), "newMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getNewMha().getMasterPort() != 0, "newMha masterPort is 0");
    }


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public String getAndUpdateTaskStatus(Long taskId) {
        try {
            MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
            if (migrationTaskTbl == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "task not exist: " + taskId);
            }
            // not STARTING or READY_TO_SWITCH_DAL status, return
            List<String> statusList = Lists.newArrayList(MigrationStatusEnum.STARTING.getStatus(), MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
            if (!statusList.contains(migrationTaskTbl.getStatus())) {
                return migrationTaskTbl.getStatus();
            }

            List<String> dbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);
            String oldMha = migrationTaskTbl.getOldMha();
            String newMha = migrationTaskTbl.getNewMha();

            // all related mha delay < 10s
            boolean allReady = this.isRelatedDelaySmall(dbNames, oldMha, newMha);

            String currStatus = migrationTaskTbl.getStatus();
            String targetStatus = allReady ? MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus() : MigrationStatusEnum.STARTING.getStatus();
            boolean needUpdate = !targetStatus.equals(currStatus);
            if (needUpdate) {
                migrationTaskTbl.setStatus(targetStatus);
                migrationTaskTblDao.update(migrationTaskTbl);
            }
            return targetStatus;
        } catch (SQLException e) {
            logger.error("queryAndPushToReadyIfPossible error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        } catch (Throwable e) {
            logger.error("queryAndPushToReadyIfPossible error", e);
            if (e instanceof ConsoleException) {
                throw e;
            }
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.UNKNOWN_EXCEPTION, e);
        }
    }

    private boolean isRelatedDelaySmall(List<String> dbNames, String oldMha, String newMha) {
        // 1. get mha replication delay info
        try {
            List<MhaReplicationDto> all = mhaReplicationServiceV2.queryRelatedReplications(Lists.newArrayList(oldMha, newMha), dbNames);
            List<MhaDelayInfoDto> mhaReplicationDelays = mhaReplicationServiceV2.getMhaReplicationDelays(all);
            logger.info("oldMha:{}, newMha:{}, db:{}, delay info: {}", oldMha, newMha, dbNames, mhaReplicationDelays);
            if (mhaReplicationDelays.size() != all.size()) {
                throw new ConsoleException("query delay fail[1]");
            }
            if (mhaReplicationDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw new ConsoleException("query delay fail[2]");
            }
            // 2. get mha messenger delay info
            List<MhaMessengerDto> messengerDtoList = messengerServiceV2.getRelatedMhaMessenger(Lists.newArrayList(oldMha, newMha), dbNames);
            List<MhaDelayInfoDto> messengerDelays = messengerServiceV2.getMhaMessengerDelays(messengerDtoList);
            logger.info("messenger oldMha:{}, newMha:{}, db:{}, delay info: {}", oldMha, newMha, dbNames, messengerDelays);
            if (messengerDelays.size() != messengerDtoList.size()) {
                throw new ConsoleException("query delay fail[3]");
            }
            if (messengerDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw new ConsoleException("query delay fail[4]");
            }

            // 3. ready condition: all related mha delay < 10s (given by DBA)
            List<MhaDelayInfoDto> mhaReplicationNotReadyList = mhaReplicationDelays.stream().filter(e -> e.getDelay() > TimeUnit.SECONDS.toMillis(10)).collect(Collectors.toList());
            List<MhaDelayInfoDto> messengerNotReadyList = messengerDelays.stream().filter(e -> e.getDelay() > TimeUnit.SECONDS.toMillis(10)).collect(Collectors.toList());
            return CollectionUtils.isEmpty(mhaReplicationNotReadyList) && CollectionUtils.isEmpty(messengerNotReadyList);
        } catch (ConsoleException e) {
            logger.error("isRelatedDelaySmall exception: " + e.getMessage(), e);
            return false;
        }
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
    private static class ReplicationInfo {
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();
    }
    
}
