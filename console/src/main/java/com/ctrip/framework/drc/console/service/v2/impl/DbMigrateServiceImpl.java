package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.v2.DbMigrateService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/8/21 10:52
 */
@Service
public class DbMigrateServiceImpl implements DbMigrateService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
    @Autowired
    private MigrationTaskTblDao migrationTaskTblDao;

    private RegionConfig regionConfig = RegionConfig.getInstance();

    private static final String BASE_API_URL = "/api/drc/v2/clusterchange/clusterId?operator={operator}";

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
        pushConfigToCM(mhaIds, operator);
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

}
