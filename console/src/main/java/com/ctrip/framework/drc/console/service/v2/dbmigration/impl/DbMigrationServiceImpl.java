package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
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
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
    private DbReplicationFilterMappingTblDao dBReplicationFilterMappingTblDao;
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

    private RegionConfig regionConfig = RegionConfig.getInstance();

    private static final String BASE_API_URL = "/api/drc/v2/clusterchange/clusterId?operator={operator}";

    @Override
    public Long dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException {
        // meta info check and init
        checkDbMigrationParam(dbMigrationRequest);
        MhaTblV2 oldMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getOldMha());
        MhaTblV2 newMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getNewMha());

        List<String> migrateDbs = dbMigrationRequest.getDbs();
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        List<DbTbl> migrateDbTblsDrcRelated = Lists.newArrayList();
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

    private void deleteDrcConfig(long taskId, boolean rollBack) throws Exception {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
        if (migrationTaskTbl == null) {
            throw ConsoleExceptionUtils.message("taskId: " + taskId + " not exist!");
        }

        if (!migrationTaskTbl.getStatus().equals(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus())) {
            throw ConsoleExceptionUtils.message("task status is: " + migrationTaskTbl.getStatus() + ", not ready to continue");
        }

        String oldMhaName = migrationTaskTbl.getOldMha();
        String newMhaName = migrationTaskTbl.getNewMha();
        String operator = migrationTaskTbl.getOperator();
        List<String> dbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);

        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(dbIds);
        List<Long> relatedMhaIds = mhaDbMappingTbls.stream()
                .map(MhaDbMappingTbl::getMhaId)
                .filter(mhaId -> !mhaId.equals(oldMhaTbl.getId()) && !mhaId.equals(newMhaTbl.getId()))
                .distinct()
                .collect(Collectors.toList());
        logger.info("offlineDrcConfig relatedMhaIds: {}", relatedMhaIds);

        long mhaId = rollBack ? newMhaTbl.getId() : oldMhaTbl.getId();
        List<MhaDbMappingTbl> oldMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(oldMhaTbl.getId());
        if (rollBack) {
            deleteMqbReplicationForRollback(mhaId, mhaDbMappingTbls);
        } else {
            deleteOldMqReplication(mhaId, oldMhaDbMappings, mhaDbMappingTbls);
        }

        for (long relateMhaId : relatedMhaIds) {
            deleteDbReplications(mhaId, relateMhaId, mhaDbMappingTbls);
            deleteDbReplications(relateMhaId, mhaId, mhaDbMappingTbls);
            if (rollBack) {
                deleteMhaReplication(mhaId, relateMhaId);
                deleteMhaReplication(relateMhaId, mhaId);
            } else {
                List<MhaDbMappingTbl> relatedMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(relateMhaId);
                List<DbReplicationTbl> existDbReplications = getExistDbReplications(oldMhaDbMappings, relatedMhaDbMappings);
                if (CollectionUtils.isEmpty(existDbReplications)) {
                    logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", mhaId, relateMhaId);
                    deleteMhaReplication(mhaId, relateMhaId);
                }

                List<DbReplicationTbl> oppositeExistDbReplications = getExistDbReplications(relatedMhaDbMappings, oldMhaDbMappings);
                if (CollectionUtils.isEmpty(oppositeExistDbReplications)) {
                    logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", relateMhaId, mhaId);
                    deleteMhaReplication(relateMhaId, mhaId);
                }
            }
        }

        List<Long> mhaIds = Lists.newArrayList(relatedMhaIds);
        mhaIds.add(mhaId);
//        pushConfigToCM(mhaIds, operator);
    }

    private void pushConfigToCM(List<Long> mhaIds, String operator) throws Exception {
        Drc drc = metaGeneratorV3.getDrc();
        Map<String, String> cmRegionUrls = regionConfig.getCMRegionUrls();
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("operator", operator);

        for (long mhaId : mhaIds) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
            DbCluster dbCluster = findDbCluster(drc, mhaTblV2, dcTbl.getDcName());
            String url = cmRegionUrls.get(dcTbl.getRegionName()) + BASE_API_URL;
            logger.info("pushConfigToCM url: {}", url);
            try {
                HttpUtils.post(url, dbCluster, ApiResult.class, paramMap);
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

    private void deleteMqbReplicationForRollback(long newMhaId, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        logger.info("deleteMqbReplicationForRollback mhaId: {}", newMhaId);
        deleteMqDbReplications(newMhaId, mhaDbMappingTbls);
        deleteMessengers(newMhaId);
    }

    private void deleteOldMqReplication(long oldMhaId, List<MhaDbMappingTbl> oldMhaDbMappingTbls, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        deleteMqDbReplications(oldMhaId, mhaDbMappingTbls);
        List<DbReplicationTbl> oldMqDbReplications = getExistMqDbReplications(oldMhaDbMappingTbls);
        if (CollectionUtils.isEmpty(oldMqDbReplications)) {
            logger.info("oldMqDbReplications are empty, delete messenger mhaId: {}", oldMhaId);
            deleteMessengers(oldMhaId);
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

    private void deleteMqDbReplications(long mhaId, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).collect(Collectors.toList());
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(srcMhaDbMappings);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplicationTbl from mhaId: {} not exist", mhaId);
            return;
        }
        List<Long> mqDbReplicationIds = mqDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        mqDbReplications.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete mqDbReplicationTblIds: {}", mqDbReplicationIds);
        dbReplicationTblDao.update(mqDbReplications);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dBReplicationFilterMappingTblDao.queryByDbReplicationIds(mqDbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dBReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
    }

    private void deleteDbReplications(long srcMhaId, long dstMhaId, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        logger.info("deleteDbReplications srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(srcMhaId)).collect(Collectors.toList());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(dstMhaId)).collect(Collectors.toList());
        List<DbReplicationTbl> dbReplicationTbls = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        if (CollectionUtils.isEmpty(dbReplicationTbls)) {
            logger.info("dbReplicationTbl from srcMhaId: {} to dstMhaId: {} not exist", srcMhaId, dstMhaId);
            return;
        }

        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        dbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete dbReplicationTblIds: {}", dbReplicationIds);
        dbReplicationTblDao.update(dbReplicationTbls);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dBReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dBReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
    }

    private void deleteMhaReplication(long srcMhaId, long dstMhaId) throws Exception {
        logger.info("deleteMhaReplication srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId, BooleanEnum.FALSE.getCode());
        if (mhaReplicationTbl == null) {
            logger.info("mhaReplication from srcMhaId: {} to dstMhaId: {} not exist", srcMhaId, dstMhaId);
            return;
        }
        mhaReplicationTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("delete mhaReplication: {}", mhaReplicationTbl);
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
        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryByApplierGroupId(applierGroupTblV2.getId(), BooleanEnum.TRUE.getCode());
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
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getName(), "oldMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getOldMha().getMasterIp(), "oldMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getOldMha().getMasterPort() != 0, "oldMha masterPort is 0");

        PreconditionUtils.checkNotNull(dbMigrationRequest.getNewMha(), "newMha is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getName(), "newMha name is null");
        PreconditionUtils.checkString(dbMigrationRequest.getNewMha().getMasterIp(), "newMha masterIp is null");
        PreconditionUtils.checkArgument(dbMigrationRequest.getNewMha().getMasterPort() != 0, "newMha masterPort is 0");

        PreconditionUtils.checkCollection(dbMigrationRequest.getDbs(), "dbs is empty");
    }


}
