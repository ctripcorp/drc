package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.DbReplicationBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcMhaBuildParam;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.v2.DrcBuildServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbMappingService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplyMode;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/7/27 15:43
 */
@Service
public class DrcBuildServiceV2Impl implements DrcBuildServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierGroupTblV2Dao applierGroupTblDao;
    @Autowired
    private ApplierTblV2Dao applierTblV2Dao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;

    private static final String CLUSTER_NAME_SUFFIX = "_dalcluster";
    private static final String DEFAULT_TABLE_NAME = ".*";

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildMha(DrcMhaBuildParam param) throws Exception {
        checkDrcMhaBuildParam(param);
        String clusterName = param.getDstMhaName() + CLUSTER_NAME_SUFFIX;
        MhaTblV2 srcMha = buildMhaTbl(param.getSrcMhaName(), param.getSrcDcId(), param.getBuId(), clusterName);
        MhaTblV2 dstMha = buildMhaTbl(param.getDstMhaName(), param.getDstDcId(), param.getBuId(), clusterName);

        long srcMhaId = insertMha(srcMha);
        long dstMhaId = insertMha(dstMha);
        insertMhaReplication(srcMhaId, dstMhaId);
        insertMhaReplication(dstMhaId, srcMhaId);
    }

    @Override
    public void buildDrc(DrcBuildParam param) throws Exception {
        checkDrcBuildParam(param);
        DrcBuildBaseParam srcBuildParam = param.getSrcBuildParam();
        DrcBuildBaseParam dstBuildParam = param.getDstBuildParam();
        String srcMhaName = srcBuildParam.getMhaName();
        String dstMhaName = dstBuildParam.getMhaName();

        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        MhaReplicationTbl srcMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId(), BooleanEnum.FALSE.getCode());
        MhaReplicationTbl dstMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMha.getId(), srcMha.getId(), BooleanEnum.FALSE.getCode());
        if (srcMhaReplication == null || dstMhaReplication == null) {
            throw ConsoleExceptionUtils.message(String.format("mhaReplication between %s and %s not exist", srcMhaName, dstMhaName));
        }

        configureReplicatorGroup(srcMha.getId(), srcBuildParam.getReplicatorInitGtid(), srcBuildParam.getReplicatorIps());
        configureReplicatorGroup(dstMha.getId(), dstBuildParam.getReplicatorInitGtid(), dstBuildParam.getReplicatorIps());

        configureApplierGroup(srcMhaReplication.getId(), srcBuildParam.getApplierInitGtid(), srcBuildParam.getApplierIps());
        configureApplierGroup(dstMhaReplication.getId(), dstBuildParam.getApplierInitGtid(), dstBuildParam.getApplierIps());
    }

    @Override
    public List<Long> configureDbReplications(DbReplicationBuildParam param) throws Exception {
        checkDbReplicationBuildParam(param);
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        String nameFilter = param.getDbName() + "\\." + param.getTableName();
        List<String> dbList = mhaDbMappingService.buildMhaDbMappings(srcMha, dstMha, nameFilter);

        List<DbReplicationTbl> dbReplicationTbls = insertDbReplications(srcMha.getId(), dstMha.getId(), dbList, nameFilter);
        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        return dbReplicationIds;
    }

    private List<DbReplicationTbl> insertDbReplications(long srcMhaId, long dstMhaId, List<String> dbList, String nameFilter) throws Exception {
        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMhaId);
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMhaId);
        Map<Long, Long> srcMhaDbMappingMap = srcMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));
        Map<Long, Long> dstMhaDbMappingMap = dstMhaDbMappings.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, MhaDbMappingTbl::getId));

        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbList);
        Map<String, Long> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));

        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        if (StringUtils.isBlank(nameFilter)) {
            dbReplicationTbls.addAll(buildDbReplications(dbList, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, DEFAULT_TABLE_NAME));
        } else {
            String[] dbFullNames = nameFilter.split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
            String srcTableName = dbFullNames[1];
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(dbFullNames[0]);
            List<String> dbNames = dbList.stream().filter(db -> aviatorRegexFilter.filter(db)).collect(Collectors.toList());

            dbReplicationTbls.addAll(buildDbReplications(dbNames, dbMap, srcMhaDbMappingMap, dstMhaDbMappingMap, srcTableName));
        }

        dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        logger.info("insertDbReplications size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);
        return dbReplicationTbls;
    }

    private List<DbReplicationTbl> buildDbReplications(List<String> dbList,
                                                       Map<String, Long> dbMap,
                                                       Map<Long, Long> srcMhaDbMappingMap,
                                                       Map<Long, Long> dstMhaDbMappingMap,
                                                       String srcTableName) {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        for (String dbName : dbList) {
            long dbId = dbMap.get(dbName);
            long srcMhaDbMappingId = srcMhaDbMappingMap.get(dbId);
            long dstMhaDbMappingId = dstMhaDbMappingMap.get(dbId);
            dbReplicationTbls.add(buildDbReplicationTbl(srcTableName, srcMhaDbMappingId, dstMhaDbMappingId, ReplicationTypeEnum.DB_TO_DB.getType()));
        }
        return dbReplicationTbls;
    }

    private DbReplicationTbl buildDbReplicationTbl(String srcTableName, long srcMhaDbMappingId, long dstMhaDbMappingId, int replicationType) {
        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        dbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        dbReplicationTbl.setSrcLogicTableName(srcTableName);
        dbReplicationTbl.setDstLogicTableName("");
        dbReplicationTbl.setReplicationType(replicationType);
        dbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return dbReplicationTbl;
    }

    private void configureApplierGroup(long mhaReplicationId, String applierInitGtid, List<String> applierIps) throws Exception {
        long applierGroupId = insertOrUpdateApplierGroup(mhaReplicationId, applierInitGtid);
        configureAppliers(applierGroupId, applierIps);
    }

    private void configureAppliers(long applierGroupId, List<String> applierIps) throws Exception {
        List<ApplierTblV2> existAppliers = applierTblV2Dao.queryByApplierGroupId(applierGroupId, BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(existAppliers)) {
            logger.info("delete appliers: {}", existAppliers);
            existAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            applierTblV2Dao.batchUpdate(existAppliers);
        }

        if (CollectionUtils.isEmpty(applierIps)) {
            return;
        }

        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(applierIps);
        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ApplierTblV2> applierTbls = applierIps.stream().map(ip -> {
            ApplierTblV2 applierTbl = new ApplierTblV2();
            applierTbl.setApplierGroupId(applierGroupId);
            applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
            applierTbl.setMaster(BooleanEnum.FALSE.getCode());
            applierTbl.setResourceId(resourceTblMap.get(ip));
            applierTbl.setDeleted(BooleanEnum.FALSE.getCode());

            return applierTbl;
        }).collect(Collectors.toList());

        logger.info("insert applierIps: {}", applierIps);
        applierTblV2Dao.batchInsert(applierTbls);
    }

    private long insertOrUpdateApplierGroup(long mhaReplicationId, String applierInitGtid) throws Exception {
        long applierGroupId;
        ApplierGroupTblV2 existApplierGroup = applierGroupTblDao.queryByMhaReplicationId(mhaReplicationId);
        if (existApplierGroup == null) {
            logger.info("buildDrc insert applierGroup, mhaReplicationId: {}", mhaReplicationId);
            ApplierGroupTblV2 applierGroupTbl = new ApplierGroupTblV2();
            applierGroupTbl.setMhaReplicationId(mhaReplicationId);
            applierGroupTbl.setGtidInit(applierInitGtid);
            applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());

            applierGroupId = applierGroupTblDao.insertWithReturnId(applierGroupTbl);
        } else {
            applierGroupId = existApplierGroup.getId();
            if (existApplierGroup.getDeleted() == BooleanEnum.TRUE.getCode()) {
                existApplierGroup.setDeleted(BooleanEnum.FALSE.getCode());
                applierGroupTblDao.update(existApplierGroup);
            }
        }
        return applierGroupId;
    }

    private void configureReplicatorGroup(long mhaId, String replicatorInitGtid, List<String> replicatorIps) throws Exception {
        long replicatorGroupId = insertOrUpdateReplicatorGroup(mhaId);
        configureReplicators(replicatorGroupId, replicatorInitGtid, replicatorIps);
    }

    private void configureReplicators(long replicatorGroupId, String replicatorInitGtid, List<String> replicatorIps) throws Exception {
        List<ReplicatorTbl> existReplicators = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupId), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(existReplicators)) {
            logger.info("delete replicators: {}", existReplicators);
            existReplicators.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            replicatorTblDao.batchUpdate(existReplicators);
        }

        if (CollectionUtils.isEmpty(replicatorIps)) {
            return;
        }
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIps(replicatorIps);
        Map<String, Long> resourceTblMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));

        List<ReplicatorTbl> replicatorTbls = new ArrayList<>();
        for (String ip : replicatorIps) {
            ReplicatorTbl replicatorTbl = new ReplicatorTbl();
            replicatorTbl.setRelicatorGroupId(replicatorGroupId);
            replicatorTbl.setGtidInit(replicatorInitGtid);
            replicatorTbl.setResourceId(resourceTblMap.get(ip));
            replicatorTbl.setPort(ConsoleConfig.DEFAULT_REPLICATOR_PORT);
            replicatorTbl.setApplierPort(metaInfoService.findAvailableApplierPort(ip));
            replicatorTbl.setMaster(BooleanEnum.FALSE.getCode());
            replicatorTbl.setDeleted(BooleanEnum.FALSE.getCode());

            replicatorTbls.add(replicatorTbl);
        }

        logger.info("insert replicatorIps: {}", replicatorIps);
        replicatorTblDao.batchInsert(replicatorTbls);
    }

    private long insertOrUpdateReplicatorGroup(long mhaId) throws Exception {
        long replicatorGroupId;
        ReplicatorGroupTbl existReplicatorGroup = replicatorGroupTblDao.queryByMhaId(mhaId);
        if (existReplicatorGroup == null) {
            logger.info("buildDrc insert replicatorGroup, mhaId: {}", mhaId);
            ReplicatorGroupTbl replicatorGroupTbl = new ReplicatorGroupTbl();
            replicatorGroupTbl.setMhaId(mhaId);
            replicatorGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return replicatorGroupTblDao.insertWithReturnId(replicatorGroupTbl);
        } else {
            replicatorGroupId = existReplicatorGroup.getId();
            if (existReplicatorGroup.getDeleted() == BooleanEnum.TRUE.getCode()) {
                existReplicatorGroup.setDeleted(BooleanEnum.FALSE.getCode());
                replicatorGroupTblDao.update(existReplicatorGroup);
            }
        }
        return replicatorGroupId;
    }

    private void insertMhaReplication(long srcMhaId, long dstMhaId) throws Exception {
        MhaReplicationTbl existMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId, BooleanEnum.FALSE.getCode());
        if (existMhaReplication != null) {
            logger.info("mhaReplication already exist, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            return;
        }

        MhaReplicationTbl mhaReplicationTbl = new MhaReplicationTbl();
        mhaReplicationTbl.setSrcMhaId(srcMhaId);
        mhaReplicationTbl.setDstMhaId(dstMhaId);
        mhaReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        mhaReplicationTbl.setDrcStatus(BooleanEnum.FALSE.getCode());

        logger.info("insertMhaReplication srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
        mhaReplicationTblDao.insert(mhaReplicationTbl);
    }

    private long insertMha(MhaTblV2 mhaTblV2) throws Exception {
        MhaTblV2 existMhaTbl = mhaTblDao.queryByMhaName(mhaTblV2.getMhaName());
        if (null == existMhaTbl) {
            return mhaTblDao.insertWithReturnId(mhaTblV2);
        } else if (existMhaTbl.getDeleted().equals(BooleanEnum.TRUE.getCode())) {
            existMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());
            mhaTblDao.update(existMhaTbl);
        }
        return existMhaTbl.getId();
    }

    private MhaTblV2 buildMhaTbl(String mhaName, long dcId, long buId, String clusterName) {
        MhaTblV2 mhaTblV2 = new MhaTblV2();
        mhaTblV2.setMhaName(mhaName);
        mhaTblV2.setDcId(dcId);
        mhaTblV2.setApplyMode(ApplyMode.transaction_table.getType());
        mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
        mhaTblV2.setBuId(buId);
        mhaTblV2.setClusterName(clusterName);
        mhaTblV2.setAppId(-1L);
        mhaTblV2.setDeleted(BooleanEnum.FALSE.getCode());

        mhaTblV2.setReadUser(monitorTableSourceProvider.getReadUserVal());
        mhaTblV2.setReadPassword(monitorTableSourceProvider.getReadPasswordVal());
        mhaTblV2.setWriteUser(monitorTableSourceProvider.getWriteUserVal());
        mhaTblV2.setWritePassword(monitorTableSourceProvider.getWritePasswordVal());
        mhaTblV2.setMonitorUser(monitorTableSourceProvider.getMonitorUserVal());
        mhaTblV2.setMonitorPassword(monitorTableSourceProvider.getMonitorPasswordVal());

        return mhaTblV2;
    }

    private void checkDrcBuildParam(DrcBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcBuildParam().getMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstBuildParam().getMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkArgument(!param.getSrcBuildParam().getMhaName().equals(param.getDstBuildParam().getMhaName()), "srcMha and dstMha are same!");
    }

    private void checkDrcMhaBuildParam(DrcMhaBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkId(param.getBuId(), "buId requires not null!");
        PreconditionUtils.checkId(param.getSrcDcId(), "srcDcId requires not null!");
        PreconditionUtils.checkId(param.getDstDcId(), "dstDcId requires not null!");
    }

    private void checkDbReplicationBuildParam(DbReplicationBuildParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMhaName(), "srcMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDstMhaName(), "dstMhaName requires not empty!");
        PreconditionUtils.checkString(param.getDbName(), "dbName requires not empty!");
        PreconditionUtils.checkString(param.getTableName(), "tableName requires not empty!");
    }
}
