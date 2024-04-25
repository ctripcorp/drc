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
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam;
import com.ctrip.framework.drc.console.dto.v2.DbMigrationParam.MigrateMhaInfo;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.enums.*;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MigrationTaskQuery;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.dbmigration.DbMigrationService;
import com.ctrip.framework.drc.console.service.v2.impl.MetaGeneratorV3;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
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
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
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
    private ReplicatorTblDao replicatorTblDao;
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
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MhaDbMappingService mhaDbMappingService;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
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
        MhaTblV2 oldMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getOldMha());
        MhaTblV2 newMhaTblV2 = checkAndInitMhaInfo(dbMigrationRequest.getNewMha());
        
        forbidMhaExistAnyDbMode(oldMhaTblV2, newMhaTblV2);
        
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
        Set<String> regions = mhaTblsByRegion.keySet();
        Set<String> vpcRegions = consoleConfig.getLocalConfigCloudDc();
        vpcRegions.retainAll(regions);
        if (!CollectionUtils.isEmpty(vpcRegions)) {
            throw ConsoleExceptionUtils.message("migrate db in vpc region: " + vpcRegions + ", please contact drcTeam!");
        }
        if (!StringUtils.isEmpty(errorInfo.toString())) {
            throw ConsoleExceptionUtils.message(errorInfo.toString());
        }

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
                errorInfo.append(commonMhas.stream().map(MhaTblV2::getMhaName).collect(Collectors.joining(",")));
                throw ConsoleExceptionUtils.message("newMha and oldMha have common mha in Replication, please check! commomMhas: " + errorInfo.toString());
            }
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

        ReplicationInfo allDbReplicationInfoInOldMha = getAllDbReplicationInfoInOldMha(oldMhaTbl, replicationInfoInOldMha);
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = allDbReplicationInfoInOldMha.dbReplicationTblsInOldMhaInSrcPairs;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = allDbReplicationInfoInOldMha.dbReplicationTblsInOldMhaInDestPairs;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = allDbReplicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs;


//        // init mhaDBMappingTbls about newMha and migrateDbTbls
//        Map<Long, MhaDbMappingTbl> dbId2MhaDbMappingMapInNewMha = initMhaDbMappingTblsInNewMha(newMhaTbl, migrateDbTblsDrcRelated);
//        // copy dbReplications and their configTbls  to newMha
//        initDbReplicationTblsInNewMha(
//                newMhaTbl,
//                dbId2MhaDbMappingMapInNewMha,
//                dbReplicationTblsInOldMhaInSrcPairs,
//                dbReplicationTblsInOldMhaInDestPairs,
//                db2MqReplicationTblsInOldMhaPairs
//        );

        // init mhaDBMappingTbls & replication & Config in newMha
        List<MhaDbMappingTbl> mhaDbMappingsInOldMha = extractMhaDbMappings(allDbReplicationInfoInOldMha);
        Map<Long, MhaDbMappingTbl> dbId2MhaDbMappingMapInNewMha = initMhaDbMappings(newMhaTbl,mhaDbMappingsInOldMha);
        initDbReplicationTblsInNewMha(
                newMhaTbl,
                dbId2MhaDbMappingMapInNewMha,
                dbReplicationTblsInOldMhaInSrcPairs,
                dbReplicationTblsInOldMhaInDestPairs,
                db2MqReplicationTblsInOldMhaPairs
        );
        // init mhaDBMappingTbls about newMha and migrateDbTbls
        initMhaReplicationsAndApplierGroups(newMhaTbl,otherMhaTblsInSrc,otherMhaTblsInDest);
        initReplicatorGroupAndMessengerGroup(newMhaTbl,db2MqReplicationTblsInOldMhaPairs);

        drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(newMhaTbl);
        try {
            pushConfigToCM(Lists.newArrayList(newMhaTbl.getId()),migrationTaskTbl.getOperator(),HttpRequestEnum.POST);
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

        ReplicationInfo replicationInfoInOldMha = getMigrateDbReplicationInfoInOldMha(migrateDbTbls, oldMhaTbl);
        List<MhaTblV2> otherMhaTblsInSrc = replicationInfoInOldMha.otherMhaTblsInSrc;
        List<MhaTblV2> otherMhaTblsInDest = replicationInfoInOldMha.otherMhaTblsInDest;
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = replicationInfoInOldMha.db2MqReplicationTblsInOldMhaPairs;
        
        // start appliers 
        if (!CollectionUtils.isEmpty(otherMhaTblsInSrc)) {
            for (MhaTblV2 mhaInSrc : otherMhaTblsInSrc) {
                MhaReplicationTbl mhaReplicationInOld = mhaReplicationTblDao.queryByMhaId(mhaInSrc.getId(), oldMhaTbl.getId(), BooleanEnum.FALSE.getCode());
                if (mhaReplicationInOld.getDrcStatus().equals(0)) {
                    continue;
                }
                MhaReplicationTbl mhaReplicationInNew = mhaReplicationTblDao.queryByMhaId(mhaInSrc.getId(), newMhaTbl.getId(), BooleanEnum.FALSE.getCode());
                ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationInNew.getId(), BooleanEnum.FALSE.getCode());
                
                // ->sha,use old applierGroup's gtidInit
                ApplierGroupTblV2 applierGroupOld = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationInOld.getId(), BooleanEnum.FALSE.getCode());
                String gtidInit = applierGroupOld.getGtidInit();
                logger.info("[[migration=start]] task:{} autoConfigAppliers, {}->mhaInDest:{}, gtidInit:{}", taskId, mhaInSrc.getMhaName(), newMhaTbl.getMhaName(), gtidInit);
                drcBuildServiceV2.autoConfigAppliers(mhaReplicationInNew, applierGroupTblV2, mhaInSrc, newMhaTbl,gtidInit);
            }
        }

        if (!CollectionUtils.isEmpty(otherMhaTblsInDest)) {
            for (MhaTblV2 mhaInDest : otherMhaTblsInDest) {
                MhaReplicationTbl mhaReplicationInOld = mhaReplicationTblDao.queryByMhaId(oldMhaTbl.getId(), mhaInDest.getId(), BooleanEnum.FALSE.getCode());
                if (mhaReplicationInOld.getDrcStatus().equals(0)) {
                    continue;
                }
                MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(newMhaTbl.getId(), mhaInDest.getId(), BooleanEnum.FALSE.getCode());
                ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
                drcBuildServiceV2.autoConfigAppliersWithRealTimeGtid(mhaReplicationTbl, applierGroupTblV2, newMhaTbl, mhaInDest);
            }
        }

        // start messengers
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(oldMhaTbl.getId(), BooleanEnum.FALSE.getCode());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
            if (!CollectionUtils.isEmpty(messengerTbls)) {
                drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(newMhaTbl);
            }
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
        migrationTaskTbl.setLog(migrationTaskTbl.getLog() + SEMICOLON + String.format(OPERATE_LOG,"Start task!",migrationTaskTbl.getOperator(),LocalDateTime.now()));
        migrationTaskTblDao.update(migrationTaskTbl);
        logger.info("[[migration=starting,newMha={}]] task:{} starting!", newMhaTbl.getMhaName(),taskId);
        return true;
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
    
    // contain all dbs in migration related mhaReplications
    private ReplicationInfo getAllDbReplicationInfoInOldMha(MhaTblV2 oldMha, ReplicationInfo migrateDbsReplicationInfo) throws SQLException{
        ReplicationInfo res = new ReplicationInfo();
        List<MhaDbMappingTbl> mhaDbMappingsInOldMha = mhaDbMappingTblDao.queryByMhaId(oldMha.getId());
        Map<Long, MhaDbMappingTbl> mhaDbMappingbyIdInOldMha = mhaDbMappingsInOldMha.stream().
                collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));

        // DB_TO_DB Replication OldMha In Src
        for (MhaTblV2 destMha : migrateDbsReplicationInfo.otherMhaTblsInDest) {
            List<MhaDbMappingTbl> mhaDbMappingsInDest = mhaDbMappingTblDao.queryByMhaId(destMha.getId());
            List<DbReplicationTbl> dbReplicationOldMhaInSrc = dbReplicationTblDao.queryByMappingIds(
                    mhaDbMappingsInOldMha.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                    mhaDbMappingsInDest.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                    DB_TO_DB.getType());
            dbReplicationOldMhaInSrc.stream().collect(Collectors.groupingBy(DbReplicationTbl::getSrcMhaDbMappingId))
                    .forEach((srcMhaDbMappingId, dbReplications) -> {
                        MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingbyIdInOldMha.get(srcMhaDbMappingId);
                        res.dbReplicationTblsInOldMhaInSrcPairs.add(Pair.of(srcMhaDbMapping, dbReplications));
                    });
        }

        // DB_TO_DB Replication OldMha In Dest
        for (MhaTblV2 srcMha : migrateDbsReplicationInfo.otherMhaTblsInSrc) {
            List<MhaDbMappingTbl> mhaDbMappingsInSrc = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
            List<DbReplicationTbl> dbReplicationsOldMhaInDest = dbReplicationTblDao.queryByMappingIds(
                    mhaDbMappingsInSrc.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                    mhaDbMappingsInOldMha.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                    DB_TO_DB.getType());
            dbReplicationsOldMhaInDest.stream().collect(Collectors.groupingBy(DbReplicationTbl::getDstMhaDbMappingId))
                    .forEach((destMhaDbMappingId, dbReplications) -> {
                        MhaDbMappingTbl destMhaDbMapping = mhaDbMappingbyIdInOldMha.get(destMhaDbMappingId);
                        res.dbReplicationTblsInOldMhaInDestPairs.add(Pair.of(destMhaDbMapping, dbReplications));
                    });
        }

        // DB_TO_MQ Replication in OldMha
        if (!CollectionUtils.isEmpty(migrateDbsReplicationInfo.db2MqReplicationTblsInOldMhaPairs)) {
            List<DbReplicationTbl> mqReplicationInOldMha = dbReplicationTblDao.queryBySrcMappingIds(
                    mhaDbMappingsInOldMha.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList()),
                    DB_TO_MQ.getType());
            mqReplicationInOldMha.stream().collect(Collectors.groupingBy(DbReplicationTbl::getSrcMhaDbMappingId))
                    .forEach((srcMhaDbMappingId, dbReplications) -> {
                        MhaDbMappingTbl srcMhaDbMapping = mhaDbMappingbyIdInOldMha.get(srcMhaDbMappingId);
                        res.db2MqReplicationTblsInOldMhaPairs.add(Pair.of(srcMhaDbMapping, dbReplications));
                    });
        }
        return res;
    }

    // only contain migration db
    private ReplicationInfo getMigrateDbReplicationInfoInOldMha(List<DbTbl> migrateDbTbls, MhaTblV2 oldMhaTbl) throws SQLException{
        List<DbTbl> migrateDbTblsDrcRelated  = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInSrc = Lists.newArrayList();
        List<MhaTblV2> otherMhaTblsInDest = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInSrcPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> dbReplicationTblsInOldMhaInDestPairs = Lists.newArrayList();
        List<Pair<MhaDbMappingTbl,List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs = Lists.newArrayList();

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

            if (!CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrc)
                    || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInDest)
                    || !CollectionUtils.isEmpty(dbReplicationTblsOldMhaInSrcMQ)) {
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
        return replicationInfo;
    }

    private void initReplicatorGroupAndMessengerGroup(MhaTblV2 newMhaTbl,
            List<Pair<MhaDbMappingTbl, List<DbReplicationTbl>>> db2MqReplicationTblsInOldMhaPairs) throws SQLException {
        Long replicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(newMhaTbl.getId());
        if (!CollectionUtils.isEmpty(db2MqReplicationTblsInOldMhaPairs)) {
            Long mGroupId = messengerGroupTblDao.upsertIfNotExist(newMhaTbl.getId(),replicatorGroupId, "");
            logger.info("[[migration=exStarting,newMha={}]] initReplicatorGroup:{},initMessengerGroup:{}", newMhaTbl.getMhaName(),
                    replicatorGroupId,mGroupId);
            
        }
    }
    
    private String appendLog(String curLog,String operation, String operator,LocalDateTime time) {
        return curLog + SEMICOLON + String.format(OPERATE_LOG, operation, operator, time);
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
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(migrateDbTblsDrcRelated.stream().map(DbTbl::getId).collect(Collectors.toList()),
                Lists.newArrayList(newMhaTbl.getId()));
        return  mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, Function.identity()));
    }

    private Map<Long,MhaDbMappingTbl> initMhaDbMappings(MhaTblV2 newMhaTbl,List<MhaDbMappingTbl> mhaDbMappingInOldMha) throws SQLException {
        mhaDbMappingService.copyAndInitMhaDbMappings(newMhaTbl,mhaDbMappingInOldMha);
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(
                mhaDbMappingInOldMha.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()),
                Lists.newArrayList(newMhaTbl.getId()));
        return mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getDbId, Function.identity()));
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

        configAppliers(newMha, mhaReplicationTbls, gtidInit);
        configDbAppliers(newMha, mhaDbMappingTbls, gtidInit);

        String messengerGtidInit = consoleConfig.getSgpMessengerGtidInit(newMhaName);
        configMessenger(oldMha, newMha, messengerGtidInit);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void preStartReplicator(String newMhaName, String oldMhaName) throws Exception {
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.Replicator.PreStart", newMhaName);
        MhaTblV2 oldMha = mhaTblV2Dao.queryByMhaName(oldMhaName, BooleanEnum.FALSE.getCode());
        if (oldMha == null) {
            throw ConsoleExceptionUtils.message("oldMha not exist");
        }
        MhaTblV2 newMha = drcBuildServiceV2.syncMhaInfoFormDbaApi(newMhaName);
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

    private void configMessenger(MhaTblV2 oldMha, MhaTblV2 newMha, String gtidInit) throws SQLException {
        ReplicatorGroupTbl newReplicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(newMha.getId(), BooleanEnum.FALSE.getCode());
        if (newReplicatorGroupTbl == null) {
            throw ConsoleExceptionUtils.message("newMha replicatorGroup not exit");
        }
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(oldMha.getId(), BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            return;
        }
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupIds(Lists.newArrayList(messengerGroupTbl.getId()));
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return;
        }
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(newMha.getMhaName(), ModuleEnum.APPLIER.getCode(), new ArrayList<>()));
        if (resourceViews.size() != 2) {
            throw ConsoleExceptionUtils.message("cannot select tow appliers for newMha");
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

    private void configAppliers(MhaTblV2 newMha, List<MhaReplicationTbl> mhaReplicationTbls, String gtidInit) throws SQLException {
        List<ResourceView> resourceViews = resourceService.autoConfigureResource(new ResourceSelectParam(newMha.getMhaName(), ModuleEnum.APPLIER.getCode(), new ArrayList<>()));
        if (resourceViews.size() != 2) {
            throw ConsoleExceptionUtils.message("cannot select two appliers for newMha");
        }
        for (MhaReplicationTbl mhaReplicationTbl : mhaReplicationTbls) {
            ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
            if (applierGroupTblV2 == null) {
                continue;
            }
            List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryByApplierGroupId(applierGroupTblV2.getId(), BooleanEnum.FALSE.getCode());
            if (CollectionUtils.isEmpty(applierTblV2s)) {
                continue;
            }

            if (mhaReplicationTbl.getSrcMhaId().equals(newMha.getId())) {
                applierGroupTblV2.setGtidInit(gtidInit);
                applierGroupTblV2Dao.update(applierGroupTblV2);
            } else {
                applierTblV2s.get(0).setResourceId(resourceViews.get(0).getResourceId());
                applierTblV2s.get(1).setResourceId(resourceViews.get(1).getResourceId());
                applierTblV2Dao.update(applierTblV2s);
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

        List<Long> allDbIds = oldMhaDbMappings.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> unMigrateDbIds = allDbIds.stream().filter(e -> !migrateDbIds.contains(e)).collect(Collectors.toList());
        List<MhaDbMappingTbl> newUnMigratedDbMappings = newMhaDbMappings.stream().filter(e -> unMigrateDbIds.contains(e.getDbId())).collect(Collectors.toList());
        List<MhaDbMappingTbl> allNewRelatedMhaDbMappings = newMhaDbMappings.stream().filter(e -> allDbIds.contains(e.getDbId())).collect(Collectors.toList());

        List<MhaDbMappingTbl> oldMigrateMhaDbMappings = oldMhaDbMappings.stream().filter(e -> migrateDbIds.contains(e.getDbId())).collect(Collectors.toList());

        if (rollBack) {
            deleteMqReplication(newMhaId, newMhaDbMappings, allNewRelatedMhaDbMappings);
        } else {
            deleteMqReplication(oldMhaId, oldMhaDbMappings, oldMigrateMhaDbMappings);
            deleteMqReplication(newMhaId, newMhaDbMappings, newUnMigratedDbMappings);
        }

        for (long relateMhaId : relatedMhaIds) {
            boolean srcDbReplicationDeleted;
            boolean dstDbReplicationDeleted;
            List<MhaDbMappingTbl> relateMhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(relateMhaId);
            if (rollBack) {
                srcDbReplicationDeleted = deleteDbReplications(allNewRelatedMhaDbMappings, relateMhaDbMappingTbls);
                dstDbReplicationDeleted = deleteDbReplications(relateMhaDbMappingTbls, allNewRelatedMhaDbMappings);
            } else {
                srcDbReplicationDeleted = deleteDbReplications(oldMigrateMhaDbMappings, relateMhaDbMappingTbls);
                dstDbReplicationDeleted = deleteDbReplications(relateMhaDbMappingTbls, oldMigrateMhaDbMappings);
                deleteMhaReplicationOrSwitchApplier(oldMhaId, relateMhaId, oldMhaDbMappings, relateMhaDbMappingTbls, srcDbReplicationDeleted, dstDbReplicationDeleted);

                srcDbReplicationDeleted = deleteDbReplications(newUnMigratedDbMappings, relateMhaDbMappingTbls);
                dstDbReplicationDeleted = deleteDbReplications(relateMhaDbMappingTbls, newUnMigratedDbMappings);
            }
            deleteMhaReplicationOrSwitchApplier(newMhaId, relateMhaId, newMhaDbMappings, relateMhaDbMappingTbls, srcDbReplicationDeleted, dstDbReplicationDeleted);
        }

        long targetMhaId = rollBack ? newMhaId : oldMhaId;
        List<MhaDbMappingTbl> deleteMhaDbMappingTbls = migrateMhaDbMappingTbls.stream().filter(e -> e.getMhaId().equals(targetMhaId)).collect(Collectors.toList());
        List<MhaDbMappingTbl> deleteNewUnMigrateMappings = newMhaDbMappings.stream().filter(e -> unMigrateDbIds.contains(e.getDbId())).collect(Collectors.toList());
        deleteMhaDbMappingTbls.addAll(deleteNewUnMigrateMappings);
        mhaDbMappingTblDao.delete(deleteMhaDbMappingTbls);

        if (!existMhaReplication(targetMhaId)) {
            String mhaName = rollBack ? newMhaName : oldMhaName;
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.Empty.Replication", mhaName);
            logger.info("task {} Mha:{} Replication not exist,offline Replicator & mha", taskId,mhaName);
            deleteReplicator(mhaName);
            mhaServiceV2.offlineMha(mhaName);
        }

        try {
            List<Long> mhaIds = Lists.newArrayList(relatedMhaIds);
            mhaIds.add(targetMhaId);
            pushConfigToCM(mhaIds, operator, HttpRequestEnum.PUT);
        } catch (Exception e) {
            logger.warn("pushConfigToCM failed, mhaIds: {}", relatedMhaIds, e);
        }
    }

    private void deleteMhaReplicationOrSwitchApplier(long srcMhaId,
                                                     long dstMhaId,
                                                     List<MhaDbMappingTbl> srcMhaDbMappings,
                                                     List<MhaDbMappingTbl> dstMhaDbMappings,
                                                     boolean srcDbReplicationDeleted,
                                                     boolean dstDbReplicationDeleted) throws Exception {
        List<DbReplicationTbl> srcDbReplications = getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
        List<DbReplicationTbl> dstExistDbReplications = getExistDbReplications(dstMhaDbMappings, srcMhaDbMappings);
        boolean deleted = CollectionUtils.isEmpty(srcDbReplications) && CollectionUtils.isEmpty(dstExistDbReplications);
        if (CollectionUtils.isEmpty(srcDbReplications)) {
            logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            deleteMhaReplication(srcMhaId, dstMhaId, deleted);
        } else if (srcDbReplicationDeleted) {
            logger.info("dbReplication exist, switch applier, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            switchApplier(srcMhaId, dstMhaId);
        }

        if (CollectionUtils.isEmpty(dstExistDbReplications)) {
            logger.info("dbReplication is empty, delete mhaReplication, srcMhaId: {}, dstMhaId: {}", dstMhaId, srcMhaId);
            deleteMhaReplication(dstMhaId, srcMhaId, deleted);
        } else if (dstDbReplicationDeleted) {
            logger.info("dbReplication exist, switch applier, srcMhaId: {}, dstMhaId: {}", dstMhaId, srcMhaId);
            switchApplier(dstMhaId, srcMhaId);
        }
    }


    private void switchApplier(long srcMhaId, long dstMhaId) throws Exception {
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaId, dstMhaId, BooleanEnum.FALSE.getCode());
        if (mhaReplicationTbl == null) {
            logger.info("mhaReplication from srcMhaId: {} to dstMhaId: {} not exist", srcMhaId, dstMhaId);
            return;
        }
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
        if (applierGroupTblV2 == null) {
            logger.info("applierGroupTblV2 not exist, mhaReplicationId: {}", mhaReplicationTbl.getId());
            return;
        }

        List<ApplierTblV2> applierTblV2s = applierTblV2Dao.queryByApplierGroupId(applierGroupTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(applierTblV2s)) {
            return;
        }

        MhaTblV2 dstMha = mhaTblV2Dao.queryById(dstMhaId);
        List<Long> resourceIds = applierTblV2s.stream().map(ApplierTblV2::getResourceId).collect(Collectors.toList());
        List<ResourceView> resourceViews = autoSwitchAppliers(resourceIds, dstMha.getMhaName());
        if (resourceViews.size() != applierTblV2s.size()) {
            logger.warn("switchApplier fail, srcMhaId: {}, dstMhaId: {}", srcMhaId, dstMhaId);
            DefaultEventMonitorHolder.getInstance().logEvent("switchApplierFail", String.valueOf(mhaReplicationTbl.getId()));
        } else {
            applierTblV2s.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("delete applierTblV2s: {}", applierTblV2s);
            applierTblV2Dao.update(applierTblV2s);

            insertNewAppliers(resourceViews, applierGroupTblV2.getId());
        }
    }

    private void switchMessenger(long mhaId) throws Exception {
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            logger.info("deleteMessengers messengerGroupTbl not exist, mhaId: {}", mhaId);
            return;
        }

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        if (CollectionUtils.isEmpty(messengerTbls)) {
            return;
        }

        MhaTblV2 mhaTbl = mhaTblV2Dao.queryById(mhaId);
        List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
        List<ResourceView> resourceViews = autoSwitchAppliers(resourceIds, mhaTbl.getMhaName());
        if (resourceViews.size() != messengerTbls.size()) {
            logger.warn("switchMessenger fail, mhaId: {}", mhaId);
            DefaultEventMonitorHolder.getInstance().logEvent("switchMessengerFail", mhaTbl.getMhaName());
        } else {
            messengerTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("delete messengerTbls: {}", messengerTbls);
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

    private void insertNewAppliers(List<ResourceView> resourceViews, long applierGroupId) throws Exception {
        List<ApplierTblV2> insertAppliers = Lists.newArrayList();
        for (ResourceView resourceView : resourceViews) {
            ApplierTblV2 applierTbl = new ApplierTblV2();
            applierTbl.setApplierGroupId(applierGroupId);
            applierTbl.setResourceId(resourceView.getResourceId());
            applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
            applierTbl.setMaster(BooleanEnum.FALSE.getCode());
            applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
            insertAppliers.add(applierTbl);
        }
        applierTblV2Dao.batchInsert(insertAppliers);
        logger.info("insertNewAppliers success, applierGroupId: {}", applierGroupId);
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
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());

        List<MhaReplicationTbl> existReplicationTbls = mhaReplicationTbls.stream().filter(e -> e.getDrcStatus().equals(BooleanEnum.TRUE.getCode())).collect(Collectors.toList());
        List<MessengerTbl> messengerTbls = new ArrayList<>();
        if (messengerGroupTbl != null) {
            messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        }

        if (CollectionUtils.isEmpty(existReplicationTbls) && CollectionUtils.isEmpty(messengerTbls)) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            mhaTblV2.setMonitorSwitch(BooleanEnum.FALSE.getCode());
            mhaTblV2Dao.update(mhaTblV2);
        }

        return !CollectionUtils.isEmpty(mhaReplicationTbls) || messengerGroupTbl != null;
    }

    private void pushConfigToCM(List<Long> mhaIds, String operator, HttpRequestEnum httpRequestEnum) throws Exception {
        Map<String, String> cmRegionUrls = regionConfig.getCMRegionUrls();
        for (long mhaId : mhaIds) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
            String dbClusterId = mhaTblV2.getClusterName() + "." + mhaTblV2.getMhaName();
            try {
                String url = null;
                Map<String, String> paramMap = new HashMap<>();
                paramMap.put("operator", operator);
                if (httpRequestEnum.equals(HttpRequestEnum.POST)) {
                    paramMap.put("dcId", dcTbl.getDcName());
                    url = cmRegionUrls.get(dcTbl.getRegionName()) + String.format(POST_BASE_API_URL, dbClusterId);
                    HttpUtils.post(url, null, ApiResult.class, paramMap);
                } else if (httpRequestEnum.equals(HttpRequestEnum.PUT)) {
                    url = cmRegionUrls.get(dcTbl.getRegionName()) + String.format(PUT_BASE_API_URL, dbClusterId);
                    HttpUtils.put(url, null, ApiResult.class, paramMap);
                }
            } catch (Exception e) {
                logger.error("pushConfigToCM fail: {}", mhaTblV2.getMhaName(), e);
            }
        }
    }

    private void deleteMqReplication(long mhaId, List<MhaDbMappingTbl> allMhaDbMappingTbls, List<MhaDbMappingTbl> deleteMhaDbMappingTbls) throws Exception {
        if (CollectionUtils.isEmpty(deleteMhaDbMappingTbls)) {
            return;
        }

        boolean deleted = deleteMqDbReplications(mhaId, deleteMhaDbMappingTbls);
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(allMhaDbMappingTbls);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplications are empty, delete messenger mhaId: {}", mhaId);
            deleteMessengers(mhaId);
        } else if (deleted) {
            logger.info("switch messenger: {}", mhaId);
            switchMessenger(mhaId);
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

    private boolean deleteMqDbReplications(long mhaId, List<MhaDbMappingTbl> deleteMhaDbMappingTbls) throws Exception {
        List<DbReplicationTbl> mqDbReplications = getExistMqDbReplications(deleteMhaDbMappingTbls);
        if (CollectionUtils.isEmpty(mqDbReplications)) {
            logger.info("mqDbReplications from mhaId: {} not exist", mhaId);
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
        mhaDbReplicationService.offlineMhaDbReplication(dbReplicationTbls);

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


    private boolean migrationDrcDoNotCare(DbMigrationParam dbMigrationRequest) throws SQLException {
        MigrateMhaInfo oldMha = dbMigrationRequest.getOldMha();
        MachineTbl mhaMaterNode = machineTblDao.queryByIpPort(oldMha.getMasterIp(), oldMha.getMasterPort());
        if (mhaMaterNode == null) {
            return true;
        }
        boolean dbReplicationExist = mhaDbReplicationService.isDbReplicationExist(mhaMaterNode.getMhaId(), dbMigrationRequest.getDbs());
        return !dbReplicationExist;
    }
    
    private void forbidMhaExistAnyDbMode(MhaTblV2 oldMhaTblV2, MhaTblV2 newMhaTblV2) {
        List<MhaReplicationTbl> mhaReplicationTbls = mhaReplicationServiceV2.queryAllHasActiveMhaDbReplications();
        mhaReplicationTbls.forEach(mhaReplicationTbl -> {
            if (oldMhaTblV2.getId().equals(mhaReplicationTbl.getSrcMhaId()) || 
                    oldMhaTblV2.getId().equals(mhaReplicationTbl.getDstMhaId())) {
                throw ConsoleExceptionUtils.message(
                        oldMhaTblV2.getMhaName() + "Mha has db mode replication, please contact DRC team!");
            }
            if (newMhaTblV2.getId().equals(mhaReplicationTbl.getSrcMhaId()) || 
                    newMhaTblV2.getId().equals(mhaReplicationTbl.getDstMhaId())) {
                throw ConsoleExceptionUtils.message(
                        newMhaTblV2.getMhaName() + "Mha has db mode replication, please contact DRC team!");
            }
        });
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
                Pair<String, Boolean> res = this.isRelatedDelaySmall(dbNames,Lists.newArrayList(newMha));
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
                Pair<String, Boolean> res = this.isRelatedDelaySmall(dbNames,Lists.newArrayList(oldMha));
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
    
    
    private Pair<String,Boolean> isRelatedDelaySmall(List<String> dbNames, List<String> mhas) {
        try {
            // 1. get mha replication delay info
            List<MhaReplicationDto> all = mhaReplicationServiceV2.queryRelatedReplications(mhas, dbNames);
            // only concern active mha replication
            all = all.stream().filter(e -> BooleanEnum.TRUE.getCode().equals(e.getStatus())).collect(Collectors.toList());
            List<MhaDelayInfoDto> mhaReplicationDelays = mhaReplicationServiceV2.getMhaReplicationDelays(all);
            logger.info("Mha:{}, db:{}, delay info: {}", mhas, dbNames, mhaReplicationDelays);
            if (mhaReplicationDelays.size() != all.size()) {
                throw ConsoleExceptionUtils.message("query delay fail[1]");
            }
            if (mhaReplicationDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw ConsoleExceptionUtils.message("query delay fail[2]");
            }
            // 2. get mha messenger delay info
            List<MhaMessengerDto> messengerDtoList = messengerServiceV2.getRelatedMhaMessenger(mhas, dbNames);
            messengerDtoList = messengerDtoList.stream().filter(e -> BooleanEnum.TRUE.getCode().equals(e.getStatus())).collect(Collectors.toList());
            List<MhaDelayInfoDto> messengerDelays = messengerServiceV2.getMhaMessengerDelays(messengerDtoList);
            logger.info("messenger Mha:{}, db:{}, delay info: {}", mhas, dbNames, mhaReplicationDelays);
            if (messengerDelays.size() != messengerDtoList.size()) {
                throw ConsoleExceptionUtils.message("query delay fail[3]");
            }
            if (messengerDelays.stream().anyMatch(e -> e.getDelay() == null)) {
                throw ConsoleExceptionUtils.message("query delay fail[4]");
            }
    
            // 3. ready condition: all related mha delay < 10s (given by DBA)
            List<MhaDelayInfoDto> mhaReplicationNotReadyList = mhaReplicationDelays.stream().filter(e -> e.getDelay() > TimeUnit.SECONDS.toMillis(10)).collect(Collectors.toList());
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
            Map<String, ValueDifference<Object>> valueDiff = configsDiff.entriesDiffering();
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
    }

}
