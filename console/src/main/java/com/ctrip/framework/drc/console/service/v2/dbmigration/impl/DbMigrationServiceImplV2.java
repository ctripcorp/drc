package com.ctrip.framework.drc.console.service.v2.dbmigration.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MhaDbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MhaDbReplicationTblDao;
import com.ctrip.framework.drc.console.dto.v2.*;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.MigrationStatusEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.mysql.DrcDbMonitorTableCreateReq;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.NotifyCmService;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.meta.ReplicationTypeEnum.*;

/**
 * Created by shiruixin
 * 2024/9/24 16:44
 * Db migration in db mode
 */
@Service
public class DbMigrationServiceImplV2 implements DbMigrationService {
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
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
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
    private DcTblDao dcTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private DbDrcBuildService dbDrcBuildService;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;
    @Autowired
    private CacheMetaService cacheMetaService;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private MhaDbReplicationTblDao mhaDbReplicationTblDao;
    @Autowired
    private ApplierGroupTblV3Dao dbApplierGroupTblDao;
    @Autowired
    private ApplierTblV3Dao dbApplierTblDao;
    @Autowired
    private NotifyCmService notifyCmService;

    private RegionConfig regionConfig = RegionConfig.getInstance();

    private static final String PUT_BASE_API_URL = "/api/meta/clusterchange/%s/?operator={operator}";
    private static final String POST_BASE_API_URL = "/api/meta/clusterchange/%s/?operator={operator}&dcId={dcId}";
    // forbid to add "," in operate log
    private static final String OPERATE_LOG = "Operate: %s,Operator: %s,Time: %s";
    private static final String SEMICOLON = ";";

    @Override
    public boolean abandonTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        if (migrationTaskTbl == null) {
            throw ConsoleExceptionUtils.message("task not exist");
        }
        migrationTaskTbl.setDeleted(BooleanEnum.TRUE.getCode());
        migrationTaskTblDao.update(migrationTaskTbl);
        return true;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean cancelTask(Long taskId) throws Exception {
        MigrationTaskTbl taskTbl = migrationTaskTblDao.queryById(taskId);
        if (taskTbl == null) {
            throw ConsoleExceptionUtils.message("task not exist");
        }
        if (MigrationStatusEnum.PRE_STARTING.getStatus().equalsIgnoreCase(taskTbl.getStatus()) ||
                MigrationStatusEnum.PRE_STARTED.getStatus().equalsIgnoreCase(taskTbl.getStatus())) {
            deleteDrcConfig(taskTbl.getId(),false,true);
        } else if (MigrationStatusEnum.INIT.getStatus().equalsIgnoreCase(taskTbl.getStatus())) {
            // do nothing
        } else {
            throw ConsoleExceptionUtils.message("Not support cancel,task status is" + taskTbl.getStatus());
        }
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(taskTbl.getNewMha(),BooleanEnum.FALSE.getCode());
        if(!existMhaReplication(mhaTblV2.getId())) {
            logger.info("task {} newMha:{} Replication not exist,offline Replicator & mha", taskId,taskTbl.getNewMha());
            deleteReplicator(taskTbl.getNewMha());
            mhaServiceV2.offlineMha(taskTbl.getNewMha());
        }
        taskTbl.setStatus(MigrationStatusEnum.CANCELED.getStatus());
        taskTbl.setLog(appendLog(taskTbl.getLog(), "Task canceled",taskTbl.getOperator(), LocalDateTime.now()));
        migrationTaskTblDao.update(taskTbl);
        return true;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Pair<String, Long> dbMigrationCheckAndCreateTask(DbMigrationParam dbMigrationRequest) throws SQLException {
        logger.info("dbMigrationCheckAndCreateTask start, request: {}", JsonUtils.toJson(dbMigrationRequest));
        checkDbMigrationParam(dbMigrationRequest);
        // check task
        List<MigrationTaskTbl> migrationTasks = migrationTaskTblDao.queryByOldMhaDBA(dbMigrationRequest.getOldMha().getName());
        MigrationTaskTbl sameTask = findSameTask(dbMigrationRequest, migrationTasks);
        if (sameTask != null) {
            sameTask.setLog(sameTask.getLog() + SEMICOLON + String.format(OPERATE_LOG, "Repeat init task!", dbMigrationRequest.getOperator(), LocalDateTime.now()));
            migrationTaskTblDao.update(sameTask);
            return Pair.of("Repeated task, status is " + sameTask.getStatus(), sameTask.getId());
        }
        checkDbRepeatedMigrationTask(dbMigrationRequest, migrationTasks);
        // check migration drc related
        if (migrationDrcDoNotCare(dbMigrationRequest)) {
            return Pair.of(null, null);
        }
        MhaTblV2 oldMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getOldMha(),null);
        MhaTblV2 newMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getNewMha(),dbMigrationRequest.getOldMha());


        StringBuilder tips = new StringBuilder();
        StringBuilder errorInfo = new StringBuilder();

        List<String> migrateDbs = dbMigrationRequest.getDbs();
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        if (migrateDbs.size() != migrateDbTbls.size()) {
            throw ConsoleExceptionUtils.message("unknown db in drc, request size: " + migrateDbs.size() + " match size: " + migrateDbTbls.size());
        }

        // find migrateDbs drcRelated and record otherMhaTblsInDrcReplication
        ReplicationInfo replicationInfoInOldMha = getMigrateDbReplicationInfoInOldMha(migrateDbTbls, oldMhaTblV2);
        List<DbTbl> migrateDbTblsDrcRelated = replicationInfoInOldMha.migrateDbTblsDrcRelated;
        List<MhaTblV2> otherMhaTbls = Lists.newArrayList();
        otherMhaTbls.addAll(replicationInfoInOldMha.otherMhaTblsInSrc);
        otherMhaTbls.addAll(replicationInfoInOldMha.otherMhaTblsInDest);

        if (CollectionUtils.isEmpty(migrateDbTblsDrcRelated)) {
            logger.info("migrate dbs not related to drc, migrate dbs: {}", migrateDbs);
            return Pair.of(null, null);
        } else {
            if (migrateDbTblsDrcRelated.size() != migrateDbTbls.size()) {
                List<DbTbl> dbDrcNotRelated = Lists.newArrayList(migrateDbTbls);
                dbDrcNotRelated.removeAll(migrateDbTblsDrcRelated);
                tips.append("drc not related dbs is: ").append(dbDrcNotRelated.stream().map(DbTbl::getDbName).collect(Collectors.joining(",")));
            }
        }

        // check case1:migrate dbs effect multi mha-Replication in same region is not allowed;
        Map<String, List<MhaTblV2>> mhaTblsByRegion = groupByRegion(Lists.newArrayList(otherMhaTbls));

        mhaTblsByRegion.forEach((region, mhaTbls) -> {
            if (mhaTbls.size() > 1) {
                String mhasInSameRegion = mhaTbls.stream().map(MhaTblV2::getMhaName).collect(Collectors.joining(","));
                errorInfo.append(region).append(": multi mhaTbs in drcReplication, please check! mha: ").append(mhasInSameRegion);
            }
        });
        if (!StringUtils.isEmpty(errorInfo.toString())) {
            throw ConsoleExceptionUtils.message(errorInfo.toString());
        }

        MigrationTaskTbl migrationTaskTbl = new MigrationTaskTbl();
        migrationTaskTbl.setDbs(JsonUtils.toJson(dbMigrationRequest.getDbs().stream().map(String::toLowerCase).collect(Collectors.toList())));
        migrationTaskTbl.setOldMha(oldMhaTblV2.getMhaName());
        migrationTaskTbl.setNewMha(newMhaTblV2.getMhaName());
        migrationTaskTbl.setOldMhaDba(dbMigrationRequest.getOldMha().getName());
        migrationTaskTbl.setNewMhaDba(dbMigrationRequest.getNewMha().getName());
        migrationTaskTbl.setStatus(MigrationStatusEnum.INIT.getStatus());
        migrationTaskTbl.setOperator(dbMigrationRequest.getOperator());
        migrationTaskTbl.setLog(String.format(OPERATE_LOG, "Init task!", dbMigrationRequest.getOperator(), LocalDateTime.now()));
        Long taskId = migrationTaskTblDao.insertWithReturnId(migrationTaskTbl);
        tips.insert(0, "task init success! ");
        return Pair.of(tips.toString(), taskId);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean preStartDbMigrationTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        String oldMha = migrationTaskTbl.getOldMha();
        String newMha = migrationTaskTbl.getNewMha();
        String status = migrationTaskTbl.getStatus();
        String dbs = migrationTaskTbl.getDbs();
        if (MigrationStatusEnum.PRE_STARTING.getStatus().equals(status) || MigrationStatusEnum.PRE_STARTED.getStatus().equals(status)) {
            return true;
        }
        if (!MigrationStatusEnum.INIT.getStatus().equals(status)) {
            throw ConsoleExceptionUtils.message("task status is not INIT, can not exStart! taskId: " + taskId);
        }
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMha);
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMha);
        List<String> migrateDbs = JsonUtils.fromJsonToList(dbs, String.class);
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);

        // check mha config newMhaConfig should equal newMhaTbl
        cacheMetaService.refreshMetaCache();
        checkMhaConfig(oldMha,newMha,Sets.newHashSet());

        ReplicationInfo replicationInfoInOldMha = getMigrateDbReplicationInfoInOldMha(migrateDbTbls, oldMhaTbl);
        List<DbTbl> migrateDbTblsDrcRelated  = replicationInfoInOldMha.migrateDbTblsDrcRelated;
        List<MhaTblV2> otherMhaTblsInSrc = replicationInfoInOldMha.otherMhaTblsInSrc;
        List<MhaTblV2> otherMhaTblsInDest = replicationInfoInOldMha.otherMhaTblsInDest;

        //init monitor db tbl
        DrcDbMonitorTableCreateReq monitorTableCreateReq = new DrcDbMonitorTableCreateReq();
        monitorTableCreateReq.setMha(newMha);
        monitorTableCreateReq.setDbs(migrateDbTblsDrcRelated.stream().map(DbTbl::getDbName).collect(Collectors.toList()));
        if (!Boolean.TRUE.equals(mysqlServiceV2.createDrcMonitorDbTable(monitorTableCreateReq))) {
            logger.warn("createDrcMonitorDbTable fail");
            throw ConsoleExceptionUtils.message("Can not create DRC Db Monitor Table in newMha: " + newMha + ", taskId: " + taskId);
        }

        List<MhaDbMappingTbl> mappingsInOldMha = extractMhaDbMappings(replicationInfoInOldMha);
        Map<Long, MhaDbMappingTbl> mappingsInNewMha = initMhaDbMappings(newMhaTbl, mappingsInOldMha); //增加涉及的db复制，mq投递的 mha_db_mapping_tbl

        //增加 新mhadbmapping -> destMapping（db）和srcMapping -> 新mhadbmapping（db）的mha_db_replication_tbl, 返回值用于配置applierGroup
        List<MhaDbReplicationTbl> mhaDbReplicationWithNewMha = initMhaDbReplicationTblsDb2Db(
                newMhaTbl,
                mappingsInNewMha,
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInSrcPairs,
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInDestPairs
        );

        //增加 新mhadbmapping -> qmq/kafka的mha_db_replication_tbl
        initMhaDbReplicationTblsDb2Mq(
                newMhaTbl,
                mappingsInNewMha,
                replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs,
                replicationInfoInOldMha.db2KafkaReplicationTblsInOldMhaPairs
        );

        if (mhaDbReplicationWithNewMha != null) {
            initApplierGroupsV3(mhaDbReplicationWithNewMha);
        }
        initDbReplicationTblsInNewMha(
                newMhaTbl,
                mappingsInNewMha,
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInSrcPairs,
                replicationInfoInOldMha.dbReplicationTblsInOldMhaInDestPairs,
                replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs,
                replicationInfoInOldMha.db2KafkaReplicationTblsInOldMhaPairs
        );
        initMhaReplications(newMhaTbl,otherMhaTblsInSrc,otherMhaTblsInDest);
        initReplicatorGroupAndMessengerGroup(
                newMhaTbl,
                replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs,
                replicationInfoInOldMha.db2KafkaReplicationTblsInOldMhaPairs
        );

        drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(newMhaTbl);
        try {
            notifyCmService.pushConfigToCM(Lists.newArrayList(newMhaTbl.getId()),migrationTaskTbl.getOperator(),HttpRequestEnum.POST);
        } catch (Exception e) {
            logger.warn("[[migration=exStarting,newMha={}]] task:{} pushConfigToCM fail!", newMhaTbl.getMhaName(),taskId,e);
        }
        migrationTaskTbl.setStatus(MigrationStatusEnum.PRE_STARTING.getStatus());
        migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG,"PreStart task!",migrationTaskTbl.getOperator(),LocalDateTime.now()));
        migrationTaskTblDao.update(migrationTaskTbl);
        logger.info("[[migration=exStarting,newMha={}]] task:{} exStarting!", newMhaTbl.getMhaName(),taskId);
        return true;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public boolean startDbMigrationTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryByPk(taskId);
        if (!MigrationStatusEnum.PRE_STARTED.getStatus().equals(migrationTaskTbl.getStatus())) { // migrationTaskManager
            throw ConsoleExceptionUtils.message("task status is not exStarted, can not start! taskId: " + taskId);
        }

        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(migrationTaskTbl.getOldMha());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(migrationTaskTbl.getNewMha());
        List<String> migrateDbs = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);
        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbs);
        List<Long> migrateDbIds = migrateDbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());

        ReplicationInfo replicationInfoInOldMha = getMigrateDbReplicationInfoInOldMha(migrateDbTbls, oldMhaTbl);
        List<MhaTblV2> otherMhaTblsInSrc = replicationInfoInOldMha.otherMhaTblsInSrc;
        List<MhaTblV2> otherMhaTblsInDest = replicationInfoInOldMha.otherMhaTblsInDest;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs;
        List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs = replicationInfoInOldMha.db2KafkaReplicationTblsInOldMhaPairs;

        List<MhaDbMappingTbl> newMappings = mhaDbMappingTblDao.queryByMhaIdAndDbIds(newMhaTbl.getId(), migrateDbIds, BooleanEnum.FALSE.getCode());
        Map<Pair<Long, Long>, MhaDbMappingTbl> mhaIdDbId2NewMapping = newMappings.stream().collect(Collectors.toMap(e -> Pair.of(e.getMhaId(),e.getDbId()), Function.identity()));

        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryAllExist().stream()
                .filter(e -> DB_TO_DB.getType().equals(e.getReplicationType())).collect(Collectors.toList());
        Map<Pair<Long,Long>, MhaDbReplicationTbl> srcDestMappingId2MhaDbReplication = mhaDbReplicationTbls.stream()
                .collect(Collectors.toMap(e -> Pair.of(e.getSrcMhaDbMappingId(), e.getDstMhaDbMappingId()), Function.identity()));

        if (!CollectionUtils.isEmpty(otherMhaTblsInSrc)) { //otherMha -> newMha
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> dbReplicationPair : replicationInfoInOldMha.dbReplicationTblsInOldMhaInDestPairs) {
                MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
                Long dbId = oldMapping.getDbId();
                MhaDbMappingTbl newMapping = mhaIdDbId2NewMapping.get(Pair.of(newMhaTbl.getId(), dbId));
                List<DbReplicationTbl> oldDbReplicationTbls = dbReplicationPair.getRight();
                List<Long> srcMappingIds = oldDbReplicationTbls.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).distinct().collect(Collectors.toList());

                for (Long srcMappingId : srcMappingIds) { //新的MhaDbReplication：srcMappingId-newMapping.getId(), 老的MhaDbReplication：srcMappingId-oldMapping.getId()
                    MhaDbReplicationTbl newMhaDbReplication = srcDestMappingId2MhaDbReplication.get(Pair.of(srcMappingId, newMapping.getId()));
                    ApplierGroupTblV3 aGroupV3 = applierGroupTblV3Dao.queryByMhaDbReplicationId(newMhaDbReplication.getId(), BooleanEnum.FALSE.getCode());

                    MhaDbReplicationTbl oldMhaDbReplication = srcDestMappingId2MhaDbReplication.get(Pair.of(srcMappingId, oldMapping.getId()));
                    ApplierGroupTblV3 aGroupV3Old = applierGroupTblV3Dao.queryByMhaDbReplicationId(oldMhaDbReplication.getId(), BooleanEnum.FALSE.getCode());
                    //if old drc db link have no appliers, new link have no appliers either.
                    if (aGroupV3Old == null) {
                        continue;
                    }
                    List<ApplierTblV3> oldAppliers = applierTblV3Dao.queryByApplierGroupId(aGroupV3Old.getId(), BooleanEnum.FALSE.getCode());
                    if (CollectionUtils.isEmpty(oldAppliers)) {
                        continue;
                    }

                    String gtidInit = aGroupV3Old.getGtidInit();
                    Integer oldConcurrency = aGroupV3Old.getConcurrency();
                    logger.info("[[migration=start]] task:{} autoConfigAppliers, MhaDbMappingInSrc:{}->MhaDbMappingInDest:{}, gtidInit:{}", taskId, srcMappingId, newMapping.getId(), gtidInit);
                    MhaDbMappingTbl srcMapping = mhaDbMappingTblDao.queryById(srcMappingId);
                    MhaTblV2 srcMhaTbl = mhaTblV2Dao.queryById(srcMapping.getMhaId());
                    dbDrcBuildService.autoConfigDbAppliers(newMhaDbReplication, aGroupV3, srcMhaTbl, newMhaTbl, gtidInit, oldConcurrency, false);
                }
            }
        }

        if (!CollectionUtils.isEmpty(otherMhaTblsInDest)) {//newMha -> otherMha
            for (Pair<MhaDbMappingTbl, List<DbReplicationTbl>> dbReplicationPair : replicationInfoInOldMha.dbReplicationTblsInOldMhaInSrcPairs) {
                MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
                Long dbId = oldMapping.getDbId();
                MhaDbMappingTbl newMapping = mhaIdDbId2NewMapping.get(Pair.of(newMhaTbl.getId(), dbId));
                List<DbReplicationTbl> oldDbReplicationTbls = dbReplicationPair.getRight();
                List<Long> destMappingIds = oldDbReplicationTbls.stream().map(DbReplicationTbl::getDstMhaDbMappingId).distinct().collect(Collectors.toList());

                for (Long destMappingId : destMappingIds) {
                    MhaDbReplicationTbl newMhaDbReplication = srcDestMappingId2MhaDbReplication.get(Pair.of(newMapping.getId(), destMappingId));
                    ApplierGroupTblV3 aGroupV3 = applierGroupTblV3Dao.queryByMhaDbReplicationId(newMhaDbReplication.getId(), BooleanEnum.FALSE.getCode());

                    MhaDbReplicationTbl oldMhaDbReplication = srcDestMappingId2MhaDbReplication.get(Pair.of(oldMapping.getId(), destMappingId));
                    ApplierGroupTblV3 aGroupV3Old = applierGroupTblV3Dao.queryByMhaDbReplicationId(oldMhaDbReplication.getId(), BooleanEnum.FALSE.getCode());
                    // check if old drc link exist
                    if (aGroupV3Old == null) {
                        continue;
                    }
                    List<ApplierTblV3> oldAppliers = applierTblV3Dao.queryByApplierGroupId(aGroupV3Old.getId(), BooleanEnum.FALSE.getCode());
                    if (CollectionUtils.isEmpty(oldAppliers)) {
                        continue;
                    }

                    Integer oldConcurrency = aGroupV3Old.getConcurrency();
                    MhaDbMappingTbl destMapping = mhaDbMappingTblDao.queryById(destMappingId);
                    MhaTblV2 destMhaTbl = mhaTblV2Dao.queryById(destMapping.getMhaId());
                    dbDrcBuildService.autoConfigDbAppliersWithRealTimeGtid(newMhaDbReplication, aGroupV3, newMhaTbl, destMhaTbl, oldConcurrency);
                }
            }
        }

        // start qmq messengers
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(oldMhaTbl.getId(), MqType.qmq, BooleanEnum.FALSE.getCode());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
            if (!CollectionUtils.isEmpty(messengerTbls)) {
                drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(newMhaTbl, MqType.qmq,false);
            }
        }
        // start kafka messengers
        if (!CollectionUtils.isEmpty(db2KafkaReplicationTblsInOldMhaPairs)) {
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(oldMhaTbl.getId(), MqType.kafka, BooleanEnum.FALSE.getCode());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
            if (!CollectionUtils.isEmpty(messengerTbls)) {
                drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(newMhaTbl, MqType.kafka,false);
            }
        }

        try {
            // push to cm
            List<Long> mhaIdsStartRelated = Lists.newArrayList(newMhaTbl.getId());
            mhaIdsStartRelated.addAll(otherMhaTblsInSrc.stream().map(MhaTblV2::getId).collect(Collectors.toList()));
            mhaIdsStartRelated.addAll(otherMhaTblsInDest.stream().map(MhaTblV2::getId).collect(Collectors.toList()));
            notifyCmService.pushConfigToCM(mhaIdsStartRelated,migrationTaskTbl.getOperator(),HttpRequestEnum.PUT);
        } catch (Exception e) {
            logger.warn("[[migration=starting,newMha={}]] task:{} pushConfigToCM fail!", newMhaTbl.getMhaName(),taskId);
        }

        // update task status
        migrationTaskTbl.setStatus(MigrationStatusEnum.STARTING.getStatus());
        migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG,"Start task!",migrationTaskTbl.getOperator(),LocalDateTime.now()));
        migrationTaskTblDao.update(migrationTaskTbl);
        logger.info("[[migration=starting,newMha={}]] task:{} starting!", newMhaTbl.getMhaName(),taskId);
        return true;
    }

    private List<MhaDbMappingTbl> extractMhaDbMappingsDb2Db(ReplicationInfo replicationInfo) {
        List<MhaDbMappingTbl> res = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(replicationInfo.dbReplicationTblsInOldMhaInSrcPairs)) {
            res.addAll(replicationInfo.dbReplicationTblsInOldMhaInSrcPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(replicationInfo.dbReplicationTblsInOldMhaInDestPairs)) {
            res.addAll(replicationInfo.dbReplicationTblsInOldMhaInDestPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        return new ArrayList<>(res.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity(), (k1, k2) -> k1)).values());
    }

    private List<MhaDbMappingTbl> extractMhaDbMappings(ReplicationInfo replicationInfo) {
        List<MhaDbMappingTbl> res = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(replicationInfo.dbReplicationTblsInOldMhaInSrcPairs)) {
            res.addAll(replicationInfo.dbReplicationTblsInOldMhaInSrcPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(replicationInfo.dbReplicationTblsInOldMhaInDestPairs)) {
            res.addAll(replicationInfo.dbReplicationTblsInOldMhaInDestPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(replicationInfo.db2MqReplicationTblsInOldMhaPairs)) {
            res.addAll(replicationInfo.db2MqReplicationTblsInOldMhaPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        if (!CollectionUtils.isEmpty(replicationInfo.db2KafkaReplicationTblsInOldMhaPairs)) {
            res.addAll(replicationInfo.db2KafkaReplicationTblsInOldMhaPairs.stream().map(Pair::getKey).collect(Collectors.toList()));
        }
        return new ArrayList<>(res.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity(), (k1, k2) -> k1)).values());
    }

    private void checkDbRepeatedMigrationTask(DbMigrationParam dbMigrationRequest,List<MigrationTaskTbl> tasksByOldMha) {
        List<Long> repeatedDbTask = tasksByOldMha.stream()
                .filter(this::taskInProcessing)
                .filter(p -> dbRepeated(p,dbMigrationRequest))
                .map(MigrationTaskTbl::getId).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(repeatedDbTask)) {
            throw ConsoleExceptionUtils.message("oldMha has dbRepeated in processing, "
                    + "can not start new migration task! repeatedTask: "
                    + JsonUtils.toJson(repeatedDbTask));
        }
    }

    private MigrationTaskTbl findSameTask(DbMigrationParam dbMigrationRequest,List<MigrationTaskTbl> tasksByOldMha) {
        return tasksByOldMha.stream()
                .filter(this::taskInProcessing)
                .filter(task -> sameTask(task, dbMigrationRequest))
                .findFirst().orElse(null);
    }

    private boolean sameTask(MigrationTaskTbl migrationTaskTbl,DbMigrationParam dbMigrationRequest) {
        List<String> dbsInTsk = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class)
                .stream().map(String::toLowerCase).sorted().collect(Collectors.toList());
        List<String> dbsInRequest = dbMigrationRequest.getDbs().stream().map(String::toLowerCase).sorted()
                .collect(Collectors.toList());
        return migrationTaskTbl.getOldMhaDba().equals(dbMigrationRequest.getOldMha().getName())
                && migrationTaskTbl.getNewMhaDba().equals(dbMigrationRequest.getNewMha().getName())
                && migrationTaskTbl.getOperator().equals(dbMigrationRequest.getOperator())
                && dbsInTsk.equals(dbsInRequest);
    }

    private boolean dbRepeated(MigrationTaskTbl migrationTaskTbl,DbMigrationParam dbMigrationRequest) {
        String dbs = migrationTaskTbl.getDbs();
        Set<String> dbsInTask = JsonUtils.fromJsonToList(dbs, DbTbl.class).stream().map(DbTbl::getDbName).collect(Collectors.toSet());
        List<String> dbsInRequest = dbMigrationRequest.getDbs();
        for (String dbInRequest : dbsInRequest) {
            if (dbsInTask.contains(dbInRequest)) {
                logger.info("[[migration=starting,newMha={}]] task:{} dbRepeated!",
                        dbMigrationRequest.getNewMha().getName(),migrationTaskTbl.getId());
                return true;
            }
        }
        return false;

    }

    private boolean taskInProcessing(MigrationTaskTbl migrationTaskTbl) {
        return  (!migrationTaskTbl.getStatus().equals(MigrationStatusEnum.SUCCESS.getStatus())
                && !migrationTaskTbl.getStatus().equals(MigrationStatusEnum.FAIL.getStatus()))
                && !migrationTaskTbl.getStatus().equals(MigrationStatusEnum.CANCELED.getStatus());
    }

    // only contain migration db
    private ReplicationInfo getMigrateDbReplicationInfoInOldMha(List<DbTbl> migrateDbTbls, MhaTblV2 oldMhaTbl) throws SQLException{
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs = Lists.newArrayList();

        for (DbTbl migrateDbTbl : migrateDbTbls) {
            MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryByDbIdAndMhaId(migrateDbTbl.getId(), oldMhaTbl.getId());
            if (mhaDbMappingTbl == null) {
                continue;
            }
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

            // DB_TO_KAFKA Replication in OldMha
            List<DbReplicationTbl> dbReplicationTblsOldMhaInSrcKafka = dbReplicationTblDao.queryBySrcMappingIds(
                    Lists.newArrayList(mhaDbMappingTbl.getId()), DB_TO_KAFKA.getType());
            if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcKafka)) {
                db2KafkaReplicationTblsInOldMhaPairs.add(Pair.of(mhaDbMappingTbl, dbReplicationTblsOldMhaInSrcKafka));
            }

            if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)
                    || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)
                    || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)
                    || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcKafka)) {
                migrateDbTblsDrcRelated.add(migrateDbTbl);
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
        replicationInfo.db2KafkaReplicationTblsInOldMhaPairs = db2KafkaReplicationTblsInOldMhaPairs;
        return replicationInfo;
    }

    private void initReplicatorGroupAndMessengerGroup(MhaTblV2 newMhaTbl,
                                                      List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs,
                                                      List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs) throws SQLException {
        Long replicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(newMhaTbl.getId());
        logger.info("[[migration=exStarting,newMha={}]] initReplicatorGroup:{}", newMhaTbl.getMhaName(), replicatorGroupId);

        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            Long mGroupId = messengerGroupTblDao.upsertIfNotExist(newMhaTbl.getId(),replicatorGroupId, "", MqType.qmq);
            logger.info("[[migration=exStarting,newMha={},mqType=qmq]] initMessengerGroup:{}", newMhaTbl.getMhaName(), mGroupId);

        }
        if (!CollectionUtils.isEmpty(db2KafkaReplicationTblsInOldMhaPairs)) {
            Long mGroupId = messengerGroupTblDao.upsertIfNotExist(newMhaTbl.getId(), replicatorGroupId, "", MqType.kafka);
            logger.info("[[migration=exStarting,newMha={},mqType=kafka]] initMessengerGroup:{}", newMhaTbl.getMhaName(), mGroupId);

        }
    }

    private String appendLog(String curLog,String operation, String operator,LocalDateTime time) {
        return curLog + SEMICOLON + String.format(OPERATE_LOG, operation, operator, time);
    }

    private void initMhaReplications(MhaTblV2 newMhaTbl, List<MhaTblV2> otherMhaTblsInSrc, List<MhaTblV2> otherMhaTblsInDest) throws SQLException {
        for (MhaTblV2 otherMhaTblInSrc : otherMhaTblsInSrc) {
            Long mhaReplicationId = mhaReplicationTblDao.insertOrReCover(otherMhaTblInSrc.getId(), newMhaTbl.getId());
            logger.info("[[migration=exStarting,newMha={}]] initMhaReplicaton:{}", newMhaTbl.getMhaName(), mhaReplicationId);
        }
        for (MhaTblV2 otherMhaTblInDest : otherMhaTblsInDest) {
            Long mhaReplicationId = mhaReplicationTblDao.insertOrReCover(newMhaTbl.getId(), otherMhaTblInDest.getId());
            logger.info("[[migration=exStarting,newMha={}]] initMhaReplicaton:{}", newMhaTbl.getMhaName(), mhaReplicationId);
        }
    }

    private void initApplierGroupsV3(List<MhaDbReplicationTbl> mhaDbReplicationTbls) throws SQLException {
        for (MhaDbReplicationTbl mhaDbReplicationTbl : mhaDbReplicationTbls) {
            Long applierGroupV3Id = applierGroupTblV3Dao.insertOrReCover(mhaDbReplicationTbl.getId(), null);
            logger.info("[[migration=exStarting,mhaDbReplication={}]] initApplierGroup:{}", mhaDbReplicationTbl.getId(), applierGroupV3Id);
        }
    }

    private Map<Long,MhaDbMappingTbl> initMhaDbMappings(MhaTblV2 newMhaTbl,List<MhaDbMappingTbl> mhaDbMappingInOldMha) throws SQLException {
        mhaDbMappingService.copyAndInitMhaDbMappings(newMhaTbl,mhaDbMappingInOldMha);
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(
                mhaDbMappingInOldMha.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()),
                Lists.newArrayList(newMhaTbl.getId()));
        return mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, Function.identity()));
    }

    /**
     * return null if no need to add new mha_db_replication_tbl(db->db) item
     * should be no existing MhaDbReplicationTbl before initing
     */
    private List<MhaDbReplicationTbl> initMhaDbReplicationTblsDb2Db(MhaTblV2 newMhaTbl,
                                                               Map<Long,MhaDbMappingTbl> dbId2MappingInNewMha,
                                                               List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs,
                                                               List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs) throws SQLException {

        List<MhaDbReplicationTbl> newMhaDbReplications = Lists.newArrayList();
        for (Pair<MhaDbMappingTbl,List<DbReplicationTbl>> dbReplicationPair : dbReplicationTblsInOldMhaInSrcPairs) {
            MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
            Long dbId = oldMapping.getDbId();
            MhaDbMappingTbl newMapping = dbId2MappingInNewMha.get(dbId);
            List<DbReplicationTbl> oldDbReplicationTbls = dbReplicationPair.getRight();
            List<Long> destMappingIds = oldDbReplicationTbls.stream().map(DbReplicationTbl::getDstMhaDbMappingId).distinct().collect(Collectors.toList());
            // <newMapping-> destMapping ,db-db> in mha_db_replication_tbl
            List<MhaDbReplicationTbl> mhaDbReplicationTbls = destMappingIds.stream().map(destMappingId -> {
                MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
                mhaDbReplicationTbl.setSrcMhaDbMappingId(newMapping.getId());
                mhaDbReplicationTbl.setDstMhaDbMappingId(destMappingId);
                mhaDbReplicationTbl.setReplicationType(DB_TO_DB.getType());
                return mhaDbReplicationTbl;
            }).collect(Collectors.toList());
            newMhaDbReplications.addAll(mhaDbReplicationTbls);
        }
        for (Pair<MhaDbMappingTbl,List<DbReplicationTbl>> dbReplicationPair : dbReplicationTblsInOldMhaInDestPairs) {
            MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
            Long dbId = oldMapping.getDbId();
            MhaDbMappingTbl newMapping = dbId2MappingInNewMha.get(dbId);
            List<DbReplicationTbl> oldDbReplicationTbls = dbReplicationPair.getRight();
            List<Long> srcMappingIds = oldDbReplicationTbls.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).distinct().collect(Collectors.toList());
            // <srcMapping-> newMapping ,db-db> in mha_db_replication_tbl
            List<MhaDbReplicationTbl> mhaDbReplicationTbls = srcMappingIds.stream().map(srcMappingId -> {
                MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
                mhaDbReplicationTbl.setSrcMhaDbMappingId(srcMappingId);
                mhaDbReplicationTbl.setDstMhaDbMappingId(newMapping.getId());
                mhaDbReplicationTbl.setReplicationType(DB_TO_DB.getType());
                return mhaDbReplicationTbl;
            }).collect(Collectors.toList());
            newMhaDbReplications.addAll(mhaDbReplicationTbls);
        }

        if (CollectionUtils.isEmpty(newMhaDbReplications)) {
            return null;
        }

        List<MhaDbReplicationTbl> exist = mhaDbReplicationTblDao.queryBySamples(newMhaDbReplications).stream()
                .filter(e -> BooleanEnum.FALSE.getCode().equals(e.getDeleted())).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(exist)) {
            throw ConsoleExceptionUtils.message("mhaDbReplicationTbls already exist in newMha:" + newMhaTbl.getMhaName() + ",please contact drcTeam!");
        }

        int[] ints = mhaDbReplicationTblDao.batchInsert(newMhaDbReplications);
        logger.info("initMhaDbReplicationTbls in newMha : {}, expected size: {}, res: {}",
                newMhaTbl.getMhaName(), newMhaDbReplications.size(), Arrays.stream(ints).sum());

        List<MhaDbReplicationTbl> insertedMhaDbReplicationTbls = mhaDbReplicationTblDao.queryBySamples(newMhaDbReplications).stream()
                .filter(e -> BooleanEnum.FALSE.getCode().equals(e.getDeleted())).collect(Collectors.toList());
        return insertedMhaDbReplicationTbls;
    }

    /**
     * return null if no need to add new mha_db_replication_tbl(db->qmq/kafka) item
     * should be no existing MhaDbReplicationTbl before initing
     */
    private List<MhaDbReplicationTbl> initMhaDbReplicationTblsDb2Mq(MhaTblV2 newMhaTbl,
                                                                    Map<Long,MhaDbMappingTbl> dbId2MappingInNewMha,
                                                                    List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs,
                                                                    List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs) throws SQLException {

        List<MhaDbReplicationTbl> newMhaDbReplications = Lists.newArrayList();

        for (Pair<MhaDbMappingTbl,List<DbReplicationTbl>> dbReplicationPair : db2MqReplicationTblsInOldMhaPairs) {
            MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
            Long dbId = oldMapping.getDbId();
            MhaDbMappingTbl newMapping = dbId2MappingInNewMha.get(dbId);
            MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
            mhaDbReplicationTbl.setSrcMhaDbMappingId(newMapping.getId());
            mhaDbReplicationTbl.setDstMhaDbMappingId(-1L);
            mhaDbReplicationTbl.setReplicationType(DB_TO_MQ.getType());
            // <newMapping-> destMapping ,db-mq> in mha_db_replication_tbl
            newMhaDbReplications.add(mhaDbReplicationTbl);
        }

        for (Pair<MhaDbMappingTbl,List<DbReplicationTbl>> dbReplicationPair : db2KafkaReplicationTblsInOldMhaPairs) {
            MhaDbMappingTbl oldMapping = dbReplicationPair.getLeft();
            Long dbId = oldMapping.getDbId();
            MhaDbMappingTbl newMapping = dbId2MappingInNewMha.get(dbId);
            MhaDbReplicationTbl mhaDbReplicationTbl = new MhaDbReplicationTbl();
            mhaDbReplicationTbl.setSrcMhaDbMappingId(newMapping.getId());
            mhaDbReplicationTbl.setDstMhaDbMappingId(-1L);
            mhaDbReplicationTbl.setReplicationType(DB_TO_KAFKA.getType());
            // <newMapping-> destMapping ,db-kafka> in mha_db_replication_tbl
            newMhaDbReplications.add(mhaDbReplicationTbl);
        }

        if (CollectionUtils.isEmpty(newMhaDbReplications)) {
            return null;
        }

        List<MhaDbReplicationTbl> exist = mhaDbReplicationTblDao.queryBySamples(newMhaDbReplications).stream()
                .filter(e -> BooleanEnum.FALSE.getCode().equals(e.getDeleted())).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(exist)) {
            throw ConsoleExceptionUtils.message("mhaDbReplicationTbls already exist in newMha:" + newMhaTbl.getMhaName() + ",please contact drcTeam!");
        }

        int[] ints = mhaDbReplicationTblDao.batchInsert(newMhaDbReplications);
        logger.info("initMhaDbReplicationTbls in newMha : {}, expected size: {}, res: {}",
                newMhaTbl.getMhaName(), newMhaDbReplications.size(), Arrays.stream(ints).sum());

        List<MhaDbReplicationTbl> insertedMhaDbReplicationTbls = mhaDbReplicationTblDao.queryBySamples(newMhaDbReplications).stream()
                .filter(e -> BooleanEnum.FALSE.getCode().equals(e.getDeleted())).collect(Collectors.toList());
        return insertedMhaDbReplicationTbls;
    }

    // before dbMigration, dbMigrationdbs' dbReplicationTbls & mqReplicationTbls should not exist in newMha
    private void initDbReplicationTblsInNewMha(MhaTblV2 newMhaTbl,
                                               Map<Long,MhaDbMappingTbl> dbId2mhaDbMappingMapInNewMha,
                                               List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs,
                                               List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs,
                                               List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs,
                                               List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs) throws SQLException {

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryMappingIds(
                dbId2mhaDbMappingMapInNewMha.values().stream().map(MhaDbMappingTbl::getId)
                        .collect(Collectors.toList()));
        if (!CollectionUtils.isEmpty(dbReplicationTbls)) {
            throw ConsoleExceptionUtils.message("dbReplicationTbls already exist in newMha:" + newMhaTbl.getMhaName() + ",please contact drcTeam!");
        }

        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha,dbReplicationTblsInOldMhaInSrcPairs,true);
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha,dbReplicationTblsInOldMhaInDestPairs,false);
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha, db2MqReplicationTblsInOldMhaPairs, true);
        initDbReplicationsAndConfigsTbls(newMhaTbl, dbId2mhaDbMappingMapInNewMha, db2KafkaReplicationTblsInOldMhaPairs, true);
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
                    logger.info("[[migration=exStarting,newMha={}]] copyDbReplication:{}", newMhaTbl.getMhaName(),
                            dbReplicationTbl);
                    // copy DbReplicationFilterMappingTbl and insert
                    DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = dbReplicaId2FilterMappingMap.get(dbReplicationTblInOldMha.getId());
                    if (dbReplicationFilterMappingTbl == null) {
                        continue;
                    }
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
        copy.setDeleted(origin.getDeleted());
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
        deleteDrcConfig(taskId, false, false);
    }

    @Override
    public void rollBackNewDrcConfig(long taskId) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("rollBackNewDrcConfig", String.valueOf(taskId));
        deleteDrcConfig(taskId, true, false);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteReplicator(String mhaName) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(mhaName + " not exist!");
        }
        if (existMhaReplication(mhaTblV2.getId())) {
            throw ConsoleExceptionUtils.message(mhaName + " exist replication!");
        }

        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (replicatorGroupTbl == null) {
            logger.info("mhaName: {} not exist replicatorGroup", mhaName);
            return;
        }
        replicatorGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("delete replicatorGroup: {}", replicatorGroupTbl);
        replicatorGroupTblDao.update(replicatorGroupTbl);

        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        if (!CollectionUtils.isEmpty(replicatorTbls)) {
            replicatorTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            replicatorTblDao.update(replicatorTbls);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void migrateMhaReplication(String newMhaName, String oldMhaName) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.MHA.MIGRATE", oldMhaName);

        MhaTblV2 oldMha = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 newMha = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());
        if (oldMha == null || newMha == null) {
            throw ConsoleExceptionUtils.message("oldMha or newMha not exit");
        }
        newMha.setMonitorSwitch(oldMha.getMonitorSwitch());
        mhaTblV2Dao.update(newMha);

        long oldMhaId = oldMha.getId();
        long newMhaId = newMha.getId();

        //update mhaDbMhaMapping
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(oldMhaId);
        for (MhaDbMappingTbl mhaDbMappingTbl : mhaDbMappingTbls) {
            mhaDbMappingTbl.setMhaId(newMhaId);
        }
        mhaDbMappingTblDao.update(mhaDbMappingTbls);

        //update mhaReplication
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(oldMhaId));
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            if (mhaReplicationTbl.getSrcMhaId() == oldMhaId) {
                mhaReplicationTbl.setSrcMhaId(newMhaId);
            } else if (mhaReplicationTbl.getDstMhaId() == oldMhaId) {
                mhaReplicationTbl.setDstMhaId(newMhaId);
            }
        }
        mhaReplicationTblDao.update(mhaReplicationTbls);

        String gtidInit = mysqlServiceV2.getMhaExecutedGtid(newMhaName);
        if (StringUtils.isEmpty(gtidInit)) {
            throw ConsoleExceptionUtils.message(newMhaName + " query gtid fail");
        }

        configDbAppliers(newMha, mhaDbMappingTbls, gtidInit);

        String messengerGtidInit = consoleConfig.getSgpMessengerGtidInit(newMhaName);
        configMessenger(oldMha, newMha, messengerGtidInit, MqType.qmq);
        configMessenger(oldMha, newMha, messengerGtidInit, MqType.kafka);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void preStartReplicator(String newMhaName, String oldMhaName) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.Replicator.PreStart", newMhaName);
        MhaTblV2 oldMha = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        if (oldMha == null) {
            throw ConsoleExceptionUtils.message("oldMha not exist");
        }
        MhaTblV2 newMha = drcBuildServiceV2.syncMhaInfoFormDbaApi(newMhaName, oldMhaName);
        DcTbl dcTbl = dcTblDao.queryById(newMha.getDcId());
        if (dcTbl == null || !dcTbl.getRegionName().equals("sgp")) {
            throw ConsoleExceptionUtils.message("only support mha from sgp");
        }

        newMha.setMonitorSwitch(BooleanEnum.TRUE.getCode());
        newMha.setTag(oldMha.getTag());
        newMha.setBuId(oldMha.getBuId());
        mhaTblV2Dao.update(newMha);

        checkMhaConfig(oldMhaName, newMhaName,Sets.newHashSet("binlogTransactionDependencyHistorySize"));

//        MhaTblV2 newMha = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());

        String gtidInit = mysqlServiceV2.getMhaExecutedGtid(newMhaName);
        if (StringUtils.isEmpty(gtidInit)) {
            throw ConsoleExceptionUtils.message(newMhaName + " query gtid fail");
        }
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());

        configReplicators(newMha, gtidInit, resourceTbls);
    }


    private void configMessenger(MhaTblV2 oldMha, MhaTblV2 newMha, String gtidInit, MqType mqType) throws SQLException {
        ReplicatorGroupTbl newReplicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(newMha.getId(), BooleanEnum.FALSE.getCode());
        if (newReplicatorGroupTbl == null) {
            throw ConsoleExceptionUtils.message("newMha replicatorGroup not exit");
        }
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(oldMha.getId(), mqType, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            return;
        }
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupIds(Lists.newArrayList(messengerGroupTbl.getId()));
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return;
        }
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(newMha.getMhaName(), ModuleEnum.MESSENGER.getCode(), new ArrayList<>()));
        if (resourceViews.size() != 2) {
            throw ConsoleExceptionUtils.message("cannot select tow messenger for newMha");
        }
        messengerGroupTbl.setGtidExecuted(gtidInit);
        messengerGroupTbl.setMhaId(newMha.getId());
        messengerGroupTbl.setReplicatorGroupId(newReplicatorGroupTbl.getId());

        messengerTbls.get(0).setResourceId(resourceViews.get(0).getResourceId());
        messengerTbls.get(1).setResourceId(resourceViews.get(1).getResourceId());
        messengerGroupTblDao.update(messengerGroupTbl);
        messengerTblDao.update(messengerTbls);

    }

    private void configDbAppliers(MhaTblV2 newMha, List<MhaDbMappingTbl> mhaDbMappingTbls, String gtidInit) throws SQLException {
        List<ResourceView> dbApplierResourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(newMha.getMhaName(), ModuleEnum.APPLIER.getCode(), new ArrayList<>()));
        if (dbApplierResourceViews.size() != 2) {
            throw ConsoleExceptionUtils.message("cannot select tow appliers for newMha");
        }
        List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryByMhaDbMappingIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()));
        for (MhaDbReplicationTbl mhaDbReplicationTbl : mhaDbReplicationTbls) {
            List<ApplierGroupTblV3> applierGroupTblV3s = dbApplierGroupTblDao.queryByMhaDbReplicationIds(Lists.newArrayList(mhaDbReplicationTbl.getId()));
            if (CollectionUtils.isEmpty(applierGroupTblV3s)) {
                continue;
            }
            List<ApplierTblV3> applierTblV3s = dbApplierTblDao.queryByApplierGroupId(applierGroupTblV3s.get(0).getId(), BooleanEnum.FALSE.getCode());
            if (CollectionUtils.isEmpty(applierTblV3s)) {
                continue;
            }
            if (mhaDbMappingIds.contains(mhaDbReplicationTbl.getSrcMhaDbMappingId())) {
                applierGroupTblV3s.get(0).setGtidInit(gtidInit);
                dbApplierGroupTblDao.update(applierGroupTblV3s);
            } else {
                applierTblV3s.get(0).setResourceId(dbApplierResourceViews.get(0).getResourceId());
                applierTblV3s.get(1).setResourceId(dbApplierResourceViews.get(1).getResourceId());
                dbApplierTblDao.update(applierTblV3s);
            }
        }
    }

    private Long configReplicators(MhaTblV2 mha, String gtidInit, List<ResourceTbl> resourceTbls) throws Exception {
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(mha.getMhaName(), ModuleEnum.REPLICATOR.getCode(), new ArrayList<>()));
        if (resourceViews.size() != 2) {
            throw ConsoleExceptionUtils.message("cannot select two replicators");
        }
        List<String> replicatorIps = resourceViews.stream().map(ResourceView::getIp).collect(Collectors.toList());
        return drcBuildServiceV2.configureReplicatorGroup(mha, gtidInit, replicatorIps, resourceTbls);
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDrcConfig(long taskId, boolean rollBack,boolean cancel) throws Exception {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
        if (migrationTaskTbl == null) {
            throw ConsoleExceptionUtils.message("taskId: " + taskId + " not exist!");
        }
        if (cancel) {
            rollBack = true;
        } else {
            String currentStatus = migrationTaskTbl.getStatus();
            List<String> statusCanRollback = Lists.newArrayList(
                    MigrationStatusEnum.STARTING.getStatus(),
                    MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(),
                    MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus()
            );
            List<String> statusCanCommit = Lists.newArrayList(
                    MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus()
            );
            if (rollBack && !statusCanRollback.contains(currentStatus)) {
                throw ConsoleExceptionUtils.message("task status is: " + currentStatus + ", not ready to rollback");
            }
            if (!rollBack && !statusCanCommit.contains(currentStatus)) {
                throw ConsoleExceptionUtils.message("task status is: " + currentStatus + ", not ready to commit");
            }
            String status = rollBack ? MigrationStatusEnum.FAIL.getStatus() : MigrationStatusEnum.SUCCESS.getStatus();
            migrationTaskTbl.setStatus(status);
            migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG, rollBack ? "RollBack" : "Commit", migrationTaskTbl.getOperator(), LocalDateTime.now()));
        }
        migrationTaskTblDao.update(migrationTaskTbl);

        String oldMhaName = migrationTaskTbl.getOldMha();
        String newMhaName = migrationTaskTbl.getNewMha();
        String operator = migrationTaskTbl.getOperator();
        List<String> migrateDbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);

        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 newMhaTbl = mhaTblV2Dao.queryByMhaName(newMhaName, BooleanEnum.FALSE.getCode());
        long oldMhaId = oldMhaTbl.getId();
        long newMhaId = newMhaTbl.getId();

        List<DbTbl> migrateDbTbls = dbTblDao.queryByDbNames(migrateDbNames);
        List<Long> migrateDbIds = migrateDbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());

        List<MhaDbMappingTbl> migrateMhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIds(migrateDbIds);
        List<Long> relatedMhaIds = migrateMhaDbMappingTbls.stream()
                .map(MhaDbMappingTbl::getMhaId)
                .filter(mhaId -> !mhaId.equals(oldMhaId) && !mhaId.equals(newMhaId))
                .distinct()
                .collect(Collectors.toList());
        logger.info("offlineDrcConfig relatedMhaIds: {}", relatedMhaIds);

        List<MhaDbMappingTbl> newMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(newMhaId);
        List<MhaDbMappingTbl> oldMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(oldMhaId);

        List<MhaDbMappingTbl> oldMigrateMhaDbMappings = oldMhaDbMappings.stream().filter(e -> migrateDbIds.contains(e.getDbId())).collect(Collectors.toList());

        List<MhaDbMappingTbl> allNewRelatedMhaDbMappings = newMhaDbMappings.stream().filter(e -> migrateDbIds.contains(e.getDbId())).collect(Collectors.toList());

        if (rollBack) {
            deleteMqReplication(newMhaId, newMhaDbMappings, allNewRelatedMhaDbMappings, MqType.qmq);
            deleteMqReplication(newMhaId, newMhaDbMappings, allNewRelatedMhaDbMappings, MqType.kafka);
        } else {
            deleteMqReplication(oldMhaId, oldMhaDbMappings, oldMigrateMhaDbMappings, MqType.qmq);
            deleteMqReplication(oldMhaId, oldMhaDbMappings, oldMigrateMhaDbMappings, MqType.kafka);
        }

        for (long relateMhaId : relatedMhaIds) {
            List<MhaDbMappingTbl> relateMhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(relateMhaId);
            if (rollBack) {//删除【newMha-其他Mha】和【其他Mha-newMha】的db_replication_tbl｜mha_db_replication_tbl|applier_group_tbl_v3｜db_replication_filter_mapping_tbl
                deleteDbReplications(allNewRelatedMhaDbMappings, relateMhaDbMappingTbls);
                deleteDbReplications(relateMhaDbMappingTbls, allNewRelatedMhaDbMappings);
            } else {
                deleteDbReplications(oldMigrateMhaDbMappings, relateMhaDbMappingTbls);
                deleteDbReplications(relateMhaDbMappingTbls, oldMigrateMhaDbMappings);
                deleteMhaReplicationIfNeed(oldMhaId, relateMhaId, oldMhaDbMappings, relateMhaDbMappingTbls);
            }
            deleteMhaReplicationIfNeed(newMhaId, relateMhaId, newMhaDbMappings, relateMhaDbMappingTbls);
        }

        long targetMhaId = rollBack ? newMhaId : oldMhaId;
        List<MhaDbMappingTbl> deleteMhaDbMappingTbls = migrateMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(targetMhaId)).collect(Collectors.toList());
        mhaDbMappingTblDao.delete(deleteMhaDbMappingTbls);

        if (!existMhaReplication(targetMhaId)) {
            String mhaName = rollBack ? newMhaName : oldMhaName;
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.Empty.Replication", mhaName);
            // todo: offline mha & machine
        }

        try {
            List<Long> mhaIds = Lists.newArrayList(relatedMhaIds);
            mhaIds.add(targetMhaId);
            notifyCmService.pushConfigToCM(mhaIds, operator, HttpRequestEnum.PUT);
        } catch (Exception e) {
            logger.warn("pushConfigToCM failed, mhaIds: {}", relatedMhaIds, e);
        }
    }

    private void deleteMhaReplicationIfNeed(long srcMhaId,
                                            long dstMhaId,
                                            List<MhaDbMappingTbl> srcMhaDbMappings,
                                            List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<DbReplicationTbl> srcDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        List<DbReplicationTbl> dstExistDbReplications = getExistDbReplications(dstMhaDbMappings, srcMhaDbMappings);
        boolean deleted = CollectionUtils.isEmpty(srcDbReplications) && CollectionUtils.isEmpty(dstExistDbReplications);
        if (CollectionUtils.isEmpty(srcDbReplications)) {
            logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            deleteMhaReplication(srcMhaId, dstMhaId, deleted);
        }

        if (CollectionUtils.isEmpty(dstExistDbReplications)) {
            logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", dstMhaId, srcMhaId);
            deleteMhaReplication(dstMhaId, srcMhaId, deleted);
        }
    }


    private void switchMessenger(long mhaId, MqType mqType) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaId, mqType, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            logger.info("deleteMessengers messengerGroupTbl not exist, mhaId: {}, mqType: {}", mhaId, mqType.name());
            return;
        }

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return;
        }

        MhaTblV2 mhaTbl = mhaTblV2Dao.queryById(mhaId);
        List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
        List<ResourceView> resourceViews = autoSwitchMessengers(resourceIds, mhaTbl.getMhaName());
        if (resourceViews.size() != messengerTbls.size()) {
            logger.warn("switchMessenger fail, mhaId: {}, mqType: {}", mhaId, mqType.name());
            DefaultEventMonitorHolder.getInstance().logEvent("switchMessengerFail", mhaTbl.getMhaName());
        } else {
            messengerTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("delete messengerTbls: {}, mqType: {}", messengerTbls, mqType.name());
            messengerTblDao.update(messengerTbls);

            insertNewMessengers(resourceViews, messengerGroupTbl.getId());
        }
    }

    private List<ResourceView> autoSwitchAppliers(List<Long> resourceIds, String mhaName) throws Exception {
        List<String> ips = resourceTblDao.queryByIds(resourceIds).stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.APPLIER.getCode());
        selectParam.setMhaName(mhaName);
        selectParam.setSelectedIps(ips);
        List<ResourceView> resourceViews = resourceService.handOffResource(selectParam);
        return resourceViews;
    }

    private List<ResourceView> autoSwitchMessengers(List<Long> resourceIds, String mhaName) throws Exception {
        List<String> ips = resourceTblDao.queryByIds(resourceIds).stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.MESSENGER.getCode());
        selectParam.setMhaName(mhaName);
        selectParam.setSelectedIps(ips);
        List<ResourceView> resourceViews = resourceService.handOffResource(selectParam);
        return resourceViews;
    }


    private void insertNewMessengers(List<ResourceView> resourceViews, long messengerGroupId) throws Exception {
        List<MessengerTbl> insertMessengers = Lists.newArrayList();
        for (ResourceView resourceView : resourceViews) {
            MessengerTbl messenger = new MessengerTbl();
            messenger.setMessengerGroupId(messengerGroupId);
            messenger.setResourceId(resourceView.getResourceId());
            messenger.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
            messenger.setDeleted(BooleanEnum.FALSE.getCode());
            insertMessengers.add(messenger);
        }
        messengerTblDao.batchInsert(insertMessengers);
        logger.info("insertNewMessengers success, messengerGroupId: {}", messengerGroupId);
    }

    private boolean existMhaReplication(long mhaId) throws Exception {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationTblDao.queryByRelatedMhaId(Lists.newArrayList(mhaId));
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaId, MqType.qmq, BooleanEnum.FALSE.getCode());
        MessengerGroupTbl messengerGroupTblKafka = messengerGroupTblDao.queryByMhaIdAndMqType(mhaId, MqType.kafka,BooleanEnum.FALSE.getCode());

        List<MhaReplicationTbl> existReplicationTbls = mhaReplicationTbls.stream().filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        List<MessengerTbl> messengerTbls = new ArrayList<>();
        if (messengerGroupTbl != null) {
            messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        }
        List<MessengerTbl> messengerTblsKafka = new ArrayList<>();
        if (messengerGroupTblKafka != null) {
            messengerTblsKafka = messengerTblDao.queryByGroupId(messengerGroupTblKafka.getId());
        }

        if (CollectionUtils.isEmpty(existReplicationTbls) && CollectionUtils.isEmpty(messengerTbls) && CollectionUtils.isEmpty(messengerTblsKafka)) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
            mhaTblV2Dao.update(mhaTblV2);
        }

        return !CollectionUtils.isEmpty(mhaReplicationTbls) || messengerGroupTbl != null || messengerGroupTblKafka != null;
    }

    private void deleteMqReplication(long mhaId, List<MhaDbMappingTbl> allMhaDbMappingTbls, List<MhaDbMappingTbl> deleteMhaDbMappingTbls, MqType mqType) throws Exception {
        if (CollectionUtils.isEmpty(deleteMhaDbMappingTbls)) {
            return;
        }

        boolean deleted = deleteMqDbReplications(mhaId, deleteMhaDbMappingTbls, mqType.getReplicationType());
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(allMhaDbMappingTbls, mqType.getReplicationType());
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplications are empty, delete messenger mhaId: {}, mqType: {}", mhaId, mqType.name());
            deleteMessengers(mhaId, mqType);
        } else if (deleted) {
            logger.info("switch messenger: {}", mhaId);
            switchMessenger(mhaId, mqType);
        }
    }


    private void deleteMessengers(long mhaId, MqType mqType) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaIdAndMqType(mhaId, mqType, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            logger.info("deleteMessengers messengerGroupTbl not exist, mhaId: {}, mqType: {}", mhaId, mqType.name());
            return;
        }
        messengerGroupTbl.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("deleteMessengerGroup mhaId: {}, messengerGroupTbl: {}, mqType: {}", mhaId, messengerGroupTbl, mqType.name());
        messengerGroupTblDao.update(messengerGroupTbl);

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (!CollectionUtils.isEmpty(messengerTbls)) {
            messengerTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("deleteMessengers, mhaId: {}, messengerTbls: {}, mqType: {}", messengerTbls, mqType.name());
            messengerTblDao.update(messengerTbls);
        }
    }

    private boolean deleteMqDbReplications(long mhaId, List<MhaDbMappingTbl> deleteMhaDbMappingTbls, ReplicationTypeEnum replicationTypeEnum) throws Exception {
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(deleteMhaDbMappingTbls, replicationTypeEnum);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplications from mhaId: {} , replicationType: {} , not exist", mhaId, replicationTypeEnum.name());
            return false;
        }
        List<Long> mqDbReplicationIds = mqDbReplications.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        mqDbReplications.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete mqDbReplicationTblIds: {}", mqDbReplicationIds);
        dbReplicationTblDao.update(mqDbReplications);
        mhaDbReplicationService.offlineMhaDbReplication(mqDbReplications);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(mqDbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dbReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
        return true;
    }

    private boolean deleteDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<DbReplicationTbl> dbReplicationTbls = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        if (CollectionUtils.isEmpty(dbReplicationTbls)) {
            return false;
        }

        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        dbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("delete dbReplicationTblIds: {}", dbReplicationIds);
        dbReplicationTblDao.update(dbReplicationTbls);
        mhaDbReplicationService.offlineMhaDbReplicationAndApplierV3(dbReplicationTbls);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        if (!CollectionUtils.isEmpty(dbReplicationFilterMappingTbls)) {
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            List<Long> dbFilterMappingIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getId).collect(Collectors.toList());
            logger.info("delete dbFilterMappingIds: {}", dbFilterMappingIds);
            dbReplicationFilterMappingTblDao.update(dbReplicationFilterMappingTbls);
        }
        return true;
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
    }


    private List<DbReplicationTbl> getExistDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());
        return existDbReplications;
    }

    private List<DbReplicationTbl> getExistMqDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, ReplicationTypeEnum replicationTypeEnum) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryBySrcMappingIds(srcMhaDbMappingIds, replicationTypeEnum.getType());
        return existDbReplications;
    }

    private List<DbReplicationTbl> getExistMqDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings) throws Exception {
        return getExistMqDbReplications(srcMhaDbMappings, DB_TO_MQ);
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


    private boolean migrationDrcDoNotCare(DbMigrationParam dbMigrationRequest) throws SQLException {
        DbMigrationParam.MigrateMhaInfo oldMha = dbMigrationRequest.getOldMha();
        MachineTbl mhaMaterNode = machineTblDao.queryByIpPort(oldMha.getMasterIp(), oldMha.getMasterPort());
        if (mhaMaterNode == null) {
            return true;
        }
        boolean dbReplicationExist = mhaDbReplicationService.isDbReplicationExist(mhaMaterNode.getMhaId(), dbMigrationRequest.getDbs());
        return !dbReplicationExist;
    }


    // init new Mha;Copy Account info From OldMha
    private MhaTblV2 checkAndInitMhaInfo(DbMigrationParam.MigrateMhaInfo mhaInfo, DbMigrationParam.MigrateMhaInfo oldMhaInfo) throws SQLException {
        MhaTblV2 mhaTblV2;
        MachineTbl mhaMaterNode = machineTblDao.queryByIpPort(mhaInfo.getMasterIp(), mhaInfo.getMasterPort());
        if (mhaMaterNode == null) {
            mhaTblV2 = drcBuildServiceV2.syncMhaInfoFormDbaApi(mhaInfo.getName(),oldMhaInfo.getName());
        } else if (mhaMaterNode.getMaster().equals(BooleanEnum.FALSE.getCode())) {
            throw ConsoleExceptionUtils.message(mhaInfo.getName() + ",masterNode metaInfo not match,Please contact DRC team!");
        } else {
            Long mhaId = mhaMaterNode.getMhaId();
            mhaTblV2 = mhaTblV2Dao.queryByPk(mhaId);
            String newMhaNameInDrc = mhaTblV2.getMhaName();
            mhaTblV2.setMonitorSwitch(1);
            mhaTblV2.setDeleted(0);
            mhaTblV2Dao.update(mhaTblV2);
            logger.info("mha:{} recover and monitor switch turn on", mhaTblV2.getMhaName());

            if (!newMhaNameInDrc.equals(mhaInfo.getName())) {
                logger.warn("drcMha:{},dbRequestMha:{},mhaName not match....", newMhaNameInDrc, mhaInfo.getName());
            }
        }
        return mhaTblV2;
    }

    private void checkDbMigrationParam(DbMigrationParam dbMigrationRequest) {
        PreconditionUtils.checkNotNull(dbMigrationRequest, "dbMigrationRequest is null");
        PreconditionUtils.checkCollection(dbMigrationRequest.getDbs(), "dbs is empty");
        PreconditionUtils.checkString(dbMigrationRequest.getOperator(), "operator is empty");

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
    public Pair<String, String> getAndUpdateTaskStatus(Long taskId,boolean careNewMha) {
        try {
            MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
            if (migrationTaskTbl == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "task not exist: " + taskId);
            }
            List<String> dbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);
            String oldMha = migrationTaskTbl.getOldMha();
            String newMha = migrationTaskTbl.getNewMha();
            if (careNewMha) {
                // not STARTING or READY_TO_SWITCH_DAL status, return
                List<String> statusList = Lists.newArrayList(MigrationStatusEnum.STARTING.getStatus(), MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus());
                if (!statusList.contains(migrationTaskTbl.getStatus())) {
                    return Pair.of(null, migrationTaskTbl.getStatus());
                }
                Pair<String, Boolean> res = this.isRelatedDelaySmallDbGranularity(dbNames,Lists.newArrayList(newMha));
                String message = res.getLeft();
                Boolean allReady = res.getRight();
                String currStatus = migrationTaskTbl.getStatus();
                String targetStatus = allReady ? MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus() : MigrationStatusEnum.STARTING.getStatus();
                boolean needUpdate = !targetStatus.equals(currStatus);
                if (needUpdate) {
                    migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG,
                            "GetAndUpdateTaskStatus before dal switch res: " + targetStatus, migrationTaskTbl.getOperator(), LocalDateTime.now()));
                    migrationTaskTbl.setStatus(targetStatus);
                    migrationTaskTblDao.update(migrationTaskTbl);
                }
                return Pair.of(message, targetStatus);
            } else {
                List<String> statusList = Lists.newArrayList(MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus(),MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus());
                if (!statusList.contains(migrationTaskTbl.getStatus())) {
                    return Pair.of(null, migrationTaskTbl.getStatus());
                }
                Pair<String, Boolean> res = this.isRelatedDelaySmallDbGranularity(dbNames,Lists.newArrayList(oldMha));
                String message = res.getLeft();
                Boolean allReady = res.getRight();
                String currStatus = migrationTaskTbl.getStatus();
                String targetStatus = allReady ? MigrationStatusEnum.READY_TO_COMMIT_TASK.getStatus() : MigrationStatusEnum.READY_TO_SWITCH_DAL.getStatus();
                boolean needUpdate = !targetStatus.equals(currStatus);
                if (needUpdate) {
                    migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG,
                            "GetAndUpdateTaskStatus after dal switch res: " + targetStatus, migrationTaskTbl.getOperator(), LocalDateTime.now()));
                    migrationTaskTbl.setStatus(targetStatus);
                    migrationTaskTblDao.update(migrationTaskTbl);
                }
                return Pair.of(message, targetStatus);

            }
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


    private Pair<String,Boolean> isRelatedDelaySmallDbGranularity(List<String> dbNames, List<String> mhas) {
        try {
            // 1. get mha db replication delay info
            List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNamesAndMhaNames(dbNames, mhas, DB_TO_DB);
            mhaDbReplicationDtos = mhaDbReplicationDtos.stream().filter(e -> Boolean.TRUE.equals(e.getDrcStatus())).collect(Collectors.toList());
            List<Long> all = mhaDbReplicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
            List<MhaDbDelayInfoDto> mhaDbReplicationDelays = mhaDbReplicationService.getReplicationDelays(all);
            logger.info("Mha:{}, db:{}, delay info: {}", mhas, dbNames, mhaDbReplicationDelays);
            if (mhaDbReplicationDelays.size() != all.size()) {
                throw ConsoleExceptionUtils.message("query delay fail[1]");
            }
            if (mhaDbReplicationDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw ConsoleExceptionUtils.message("query delay fail[2]");
            }
            // 2. get mha messenger delay info
            List<MhaMessengerDto> messengerDtoList = messengerServiceV2.getRelatedMhaMessenger(mhas, dbNames);
            messengerDtoList = messengerDtoList.stream().filter(e -> BooleanEnum.TRUE.getCode().equals(e.getStatus())).collect(Collectors.toList());

            List<MhaDelayInfoDto> messengerDelays = messengerServiceV2.getMhaMessengerDelays(messengerDtoList);
            logger.info("messenger Mha:{}, db:{}, delay info: {}", mhas, dbNames, messengerDelays);
            if (messengerDelays.size() != messengerDtoList.size()) {
                throw ConsoleExceptionUtils.message("query delay fail[3]");
            }
            if (messengerDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw ConsoleExceptionUtils.message("query delay fail[4]");
            }
            // 3. ready condition: all related mha delay < 10s (given by DBA)
            List<MhaDelayInfoDto> mhaReplicationNotReadyList = mhaDbReplicationDelays.stream().filter(e -> e.getDelay() > TimeUnit.SECONDS.toMillis(10)).collect(Collectors.toList());
            List<MhaDelayInfoDto> messengerNotReadyList = messengerDelays.stream().filter(e -> e.getDelay() > TimeUnit.SECONDS.toMillis(10)).collect(Collectors.toList());
            boolean allReady = CollectionUtils.isEmpty(mhaReplicationNotReadyList) && CollectionUtils.isEmpty(messengerNotReadyList);
            String message = this.buildMessage(messengerNotReadyList, mhaReplicationNotReadyList);
            return Pair.of(message, allReady);
        } catch (ConsoleException e) {
            logger.error("isRelatedDelaySmall exception: " + e.getMessage(), e);
            return Pair.of(e.getMessage(), false);
        }

    }

    private String buildMessage(List<MhaDelayInfoDto> messengerNotReadyList, List<MhaDelayInfoDto> mhaReplicationNotReadyList) {
        // build message
        StringBuilder sb = new StringBuilder();
        if (!CollectionUtils.isEmpty(mhaReplicationNotReadyList)) {
            sb.append("drc not ready: ");
            for (MhaDelayInfoDto delayInfoDto : mhaReplicationNotReadyList) {
                sb.append(delayInfoDto.toString()).append('\n');
            }
        }
        if (!CollectionUtils.isEmpty(messengerNotReadyList)) {
            sb.append("drc messenger not ready: \n");
            for (MhaDelayInfoDto delayInfoDto : messengerNotReadyList) {
                sb.append(delayInfoDto.toString()).append('\n');
            }
        }
        return sb.toString();
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

    @Override
    public void checkMhaConfig(String oldMha, String newMha, Set<String> ignoreConfigName) throws ConsoleException {
        Map<String, Object> configInOldMha = mysqlServiceV2.preCheckMySqlConfig(oldMha);
        Map<String, Object> configInNewMha = mysqlServiceV2.preCheckMySqlConfig(newMha);
        if (!CollectionUtils.isEmpty(ignoreConfigName)) {
            configInOldMha.keySet().removeAll(ignoreConfigName);
            configInNewMha.keySet().removeAll(ignoreConfigName);
        }

        MapDifference<String, Object> configsDiff = Maps.difference(configInOldMha, configInNewMha);
        if (consoleConfig.getConfgiCheckSwitch() && !configsDiff.areEqual()) {
            Map<String, MapDifference.ValueDifference<Object>> valueDiff = configsDiff.entriesDiffering();
            String diff = valueDiff.entrySet().stream().map(
                    entry -> "config:" + entry.getKey() +
                            ",oldMha:" + entry.getValue().leftValue() +
                            ",newMha:" + entry.getValue().rightValue()).collect(Collectors.joining(";"));
            throw ConsoleExceptionUtils.message("MhaConfigs not equals!" + diff);
        }
    }

    private static class ReplicationInfo {
        List<DbTbl> migrateDbTblsDrcRelated = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2KafkaReplicationTblsInOldMhaPairs = Lists.newArrayList();
    }

    public List<MhaApplierDto> getMhaDbReplicationDelayFromMigrateTask(Long taskId) throws SQLException {
        MigrationTaskTbl migrationTaskTbl = migrationTaskTblDao.queryById(taskId);
        if (migrationTaskTbl == null) {
            return Lists.newArrayList();
        }
        List<String> dbNames = JsonUtils.fromJsonToList(migrationTaskTbl.getDbs(), String.class);
        String oldMha = migrationTaskTbl.getOldMha();
        String newMha = migrationTaskTbl.getNewMha();

        List<MhaDbReplicationDto> mhaDbReplicationDtosAll = mhaDbReplicationService.queryByDbNamesAndMhaNames(dbNames, Lists.newArrayList(oldMha,newMha), DB_TO_DB);
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationDtosAll.stream().filter(e -> Boolean.TRUE.equals(e.getDrcStatus())).collect(Collectors.toList());
        List<Long> all = mhaDbReplicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
        List<MhaDbDelayInfoDto> mhaDbReplicationDelays = mhaDbReplicationService.getReplicationDelays(all);
        Map<String, MhaDbDelayInfoDto> delayMap = mhaDbReplicationDelays.stream()
                .collect(Collectors.toMap(delayDto -> delayDto.getSrcMha()+"."+delayDto.getDstMha()+"."+delayDto.getDbName(), Function.identity()));

        List<MhaApplierDto> mhaApplierDtos = mhaDbReplicationDtosAll.stream()
                .map(dto -> {
                    String key = dto.getSrc().getMhaName()+"."+dto.getDst().getMhaName()+"."+dto.getSrc().getDbName();
                    if (delayMap.containsKey(key)) {
                        MhaApplierDto applierDto = MhaApplierDto.from(dto, delayMap.get(key));
                        return applierDto;
                    } else {
                        return MhaApplierDto.from(dto, null);
                    }
                }).collect(Collectors.toList());
        return mhaApplierDtos;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Map<String, List<Long>> cleanApplierDirtyData(boolean showonly) throws SQLException{
        Map<String, List<Long>> dirtyAGroupAndApplier = Maps.newHashMap();
        List<MhaDbReplicationTbl> mhaDbReplicationTbls = mhaDbReplicationTblDao.queryAll();
        List<MhaDbReplicationTbl> deletedMhaDbReplications = mhaDbReplicationTbls.stream().filter(e -> BooleanEnum.TRUE.getCode().equals(e.getDeleted())).collect(Collectors.toList());
        List<Long> deletedMhaDbReplicationIds = deletedMhaDbReplications.stream().map(MhaDbReplicationTbl::getId).collect(Collectors.toList());
        List<ApplierGroupTblV3> dirtyApplierGroups = applierGroupTblV3Dao.queryByMhaDbReplicationIds(deletedMhaDbReplicationIds);
        if (!CollectionUtils.isEmpty(dirtyApplierGroups)) {
            List<Long> dirtyApplierGroupIds = dirtyApplierGroups.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList());
            dirtyAGroupAndApplier.put("dirtyAGroup", dirtyApplierGroupIds);
            List<ApplierTblV3> dirtyAppliers = applierTblV3Dao.queryByApplierGroupIds(dirtyApplierGroupIds, BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(dirtyAppliers)) {
                List<Long> dirtyApplierIds = dirtyAppliers.stream().map(ApplierTblV3::getId).collect(Collectors.toList());
                dirtyAGroupAndApplier.put("dirtyApplier", dirtyApplierIds);
                if (!showonly) {
                    logger.info("delete dirty appliers:" + dirtyApplierIds.toString());
                    dirtyAppliers.forEach(e -> {
                        e.setDeleted(BooleanEnum.TRUE.getCode());
                    });
                    applierTblV3Dao.update(dirtyAppliers);
                }
            } else {
                dirtyAGroupAndApplier.put("dirtyApplier", Lists.newArrayList());
            }

            if (!showonly) {
                logger.info("delete dirty applier groups:" + dirtyApplierGroups.toString());
                dirtyApplierGroups.forEach(e -> {
                    e.setDeleted(BooleanEnum.TRUE.getCode());
                });
                applierGroupTblV3Dao.update(dirtyApplierGroups);
            }
        } else {
            dirtyAGroupAndApplier.put("dirtyAGroup", Lists.newArrayList());
        }
        return dirtyAGroupAndApplier;
    }
}
