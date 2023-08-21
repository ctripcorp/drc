package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.v2.DbMigrateService;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        String oldMhaName = "";
        String newMhaName = "";

        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());

        List<String> dbNames = new ArrayList<>();

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());

        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(dbIds);
        Set<Long> relatedMhaIds = mhaDbMappingTbls.stream()
                .map(MhaDbMappingTbl::getMhaId)
                .filter(mhaId -> !mhaId.equals(oldMhaTbl.getId()) && !mhaId.equals(newMhaTbl.getId()))
                .collect(Collectors.toSet());
        logger.info("offlineDrcConfig relatedMhaIds: {}", relatedMhaIds);

        long mhaId = rollBack ? newMhaTbl.getId() : oldMhaTbl.getId();
        deleteMqbReplication(mhaId, mhaDbMappingTbls);

        for (long relateMhaId : relatedMhaIds) {
            deleteDrcConfig(mhaId, relateMhaId, mhaDbMappingTbls);
        }
    }

    private void deleteDrcConfig(long mhaId, long relateMhaId, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        logger.info("deleteDrcConfig mhaId: {}, relateMhaId: {}", mhaId, relateMhaId);
        deleteMhaReplication(mhaId, relateMhaId);
        deleteDbReplications(mhaId, relateMhaId, mhaDbMappingTbls);

        deleteMhaReplication(relateMhaId, mhaId);
        deleteDbReplications(relateMhaId, mhaId, mhaDbMappingTbls);
    }

    private void deleteMqbReplication(long mhaId, List<MhaDbMappingTbl> mhaDbMappingTbls) throws Exception {
        logger.info("deleteMqbReplication mhaId: {}", mhaId);
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            logger.info("deleteMqbReplication mhaId: {} not exist", mhaId);
            return;
        }
        messengerGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("delete MessengerGroupTbl: {}", messengerGroupTbl);
        messengerGroupTblDao.update(messengerGroupTbl);

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (!CollectionUtils.isEmpty(messengerTbls)) {
            messengerTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("delete messengerTbls: {}", messengerTbls);
            messengerTblDao.update(messengerTbls);
        }

        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(mhaId)).collect(Collectors.toList());
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(srcMhaDbMappings);
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
