package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerTblV3Dao;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.dto.v3.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.error.AutoBuildErrorEnum;
import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.*;
import com.ctrip.framework.drc.console.param.v2.resource.ResourceSelectParam;
import com.ctrip.framework.drc.console.service.NotifyCmService;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.DbaApiService;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DalclusterUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.v2.ColumnsConfigView;
import com.ctrip.framework.drc.console.vo.v2.MqMetaCreateResultView;
import com.ctrip.framework.drc.console.vo.v2.ResourceView;
import com.ctrip.framework.drc.console.vo.v2.RowsFilterConfigView;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.ckafka.KafkaApiService;
import com.ctrip.framework.drc.core.service.ckafka.KafkaTopicCreateVo;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Lazy
public class DbDrcBuildServiceImpl implements DbDrcBuildService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Logger autoConfigLogger = LoggerFactory.getLogger("autoConfig");

    @Autowired
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MetaProviderV2 metaProviderV2;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private ApplierTblV3Dao applierTblV3Dao;
    @Autowired
    private ApplierGroupTblV3Dao applierGroupTblV3Dao;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;
    @Autowired
    private MessengerTblV3Dao messengerTblV3Dao;
    @Autowired
    private MessengerGroupTblV3Dao messengerGroupTblV3Dao;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;
    @Autowired
    private ResourceService resourceService;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private DbaApiService dbaApiService;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblV2Dao;
    @Autowired
    private ColumnsFilterTblV2Dao columnFilterTblV2Dao;
    @Autowired
    private DrcBuildServiceV2 drcBuildServiceV2;
    @Autowired
    private DrcAutoBuildService drcAutoBuildService;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;
    @Autowired
    private MessengerBatchConfigService messengerBatchConfigService;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private NotifyCmService notifyCmService;
    @Autowired
    private DLockService dLockService;


    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "drcMetaRefreshV2");

    private final DbClusterApiService dbClusterService = ApiContainer.getDbClusterApiServiceImpl();

    private final KafkaApiService kafkaApiService = ApiContainer.getKafkaApiServiceImpl();

    @Override
    public List<DbApplierDto> getMhaDbAppliers(String srcMhaName, String dstMhaName) {
        // mha pair -> mha db replication
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha(srcMhaName, dstMhaName, null);
        setMhaDbAppliers(replicationDtos);
        return replicationDtos.stream().map(MhaDbReplicationDto::getDbApplierDto).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public void setMhaDbAppliers(List<MhaDbReplicationDto> replicationDtos) {
        try { // appliers
            List<Long> replicationIds = replicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
            List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryByMhaDbReplicationIds(replicationIds);

            List<Long> applierGroupIds = applierGroupTblV3s.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList());
            List<ApplierTblV3> applierTblV3s = applierTblV3Dao.queryByApplierGroupIds(Lists.newArrayList(applierGroupIds), 0);

            List<Long> resourceIds = applierTblV3s.stream().map(ApplierTblV3::getResourceId).collect(Collectors.toList());
            Map<Long, String> resouceMap = resourceTblDao.queryByIds(resourceIds).stream()
                    .collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));

            // build data
            Map<Long, ApplierGroupTblV3> applierGroupTblV3Map = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getMhaDbReplicationId, e -> e));
            Map<Long, List<ApplierTblV3>> applierMap = applierTblV3s.stream().collect(Collectors.groupingBy(ApplierTblV3::getApplierGroupId));
            replicationDtos.stream().forEach(replicationTbl -> {
                ApplierGroupTblV3 applierGroupTblV3 = applierGroupTblV3Map.get(replicationTbl.getId());
                if (replicationTbl.getId() == null || applierGroupTblV3 == null) {
                    replicationTbl.setDbApplierDto(new DbApplierDto(null, null, replicationTbl.getDst().getDbName(), null));
                    return;
                }
                List<ApplierTblV3> appliers = applierMap.getOrDefault(applierGroupTblV3.getId(), Collections.emptyList());
                List<String> ips = appliers.stream().map(e -> resouceMap.get(e.getResourceId())).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(ips) && CollectionUtils.isEmpty(replicationTbl.getDbReplicationDtos())) {
                    // skip
                    replicationTbl.setDbApplierDto(null);
                    return;
                }
                replicationTbl.setDbApplierDto(new DbApplierDto(ips, applierGroupTblV3.getGtidInit(), replicationTbl.getDst().getDbName(), applierGroupTblV3.getConcurrency()));
            });
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<DbApplierDto> getMhaDbMessengers(String mhaName, MqType mqType) {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryMqByMha(mhaName, null, mqType);
        setMhaDbMessengers(replicationDtos, mqType);
        return replicationDtos.stream().map(MhaDbReplicationDto::getDbApplierDto).collect(Collectors.toList());
    }


    @Override
    public void setMhaDbMessengers(List<MhaDbReplicationDto> replicationDtos, MqType mqType) {
        try {
            List<Long> ids = replicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());

            // mq replications -> messenger
            List<MessengerGroupTblV3> messengerGroupTblV3s = messengerGroupTblV3Dao.queryByMhaDbReplicationIdsAndMqType(ids, mqType);
            Map<Long, MessengerGroupTblV3> groupMap = messengerGroupTblV3s.stream().collect(Collectors.toMap(MessengerGroupTblV3::getMhaDbReplicationId, e -> e));

            List<MessengerTblV3> messengerTblV3s = messengerTblV3Dao.queryByGroupIds(messengerGroupTblV3s.stream().map(MessengerGroupTblV3::getId).collect(Collectors.toList()));
            Map<Long, List<MessengerTblV3>> messengersByGroupMap = messengerTblV3s.stream().collect(Collectors.groupingBy(MessengerTblV3::getMessengerGroupId));

            List<Long> resourceIds = messengerTblV3s.stream().map(MessengerTblV3::getResourceId).collect(Collectors.toList());
            Map<Long, String> resouceMap = resourceTblDao.queryByIds(resourceIds).stream()
                    .collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));


            // build data
            replicationDtos.forEach(replicationTbl -> {
                MessengerGroupTblV3 messengerGroupTblV3 = groupMap.get(replicationTbl.getId());
                if (replicationTbl.getId() == null || messengerGroupTblV3 == null) {
                    replicationTbl.setDbApplierDto(new DbApplierDto(null, null, replicationTbl.getSrc().getDbName()));
                } else {
                    List<MessengerTblV3> messengers = messengersByGroupMap.getOrDefault(messengerGroupTblV3.getId(), Collections.emptyList());
                    List<String> ips = messengers.stream().map(e -> resouceMap.get(e.getResourceId())).collect(Collectors.toList());
                    replicationTbl.setDbApplierDto(new DbApplierDto(ips, messengerGroupTblV3.getGtidExecuted(), replicationTbl.getSrc().getDbName()));
                }
            });
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void buildDbApplier(DrcBuildParam param) throws Exception {
        DrcBuildBaseParam srcBuildParam = param.getSrcBuildParam();
        DrcBuildBaseParam dstBuildParam = param.getDstBuildParam();
        String srcMhaName = srcBuildParam.getMhaName();
        String dstMhaName = dstBuildParam.getMhaName();
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        List<DbApplierDto> dstDbAppliers = dstBuildParam.getDbApplierDtos();


        // config db applier
        if (!CollectionUtils.isEmpty(dstDbAppliers)) {
            // src -> dst
            this.checkDbAppliers(dstDbAppliers);
            List<MhaDbReplicationDto> srcToDstMhaDbReplicationDtos = mhaDbReplicationService.queryByMha(srcMhaName, dstMhaName, dstDbAppliers.stream().map(DbApplierDto::getDbName).collect(Collectors.toList()));
            this.configureDbAppliers(srcToDstMhaDbReplicationDtos, dstDbAppliers);
        }

        // sync mha replication status
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId(), BooleanEnum.FALSE.getCode());
        if (mhaReplicationTbl != null) {
            BooleanEnum drcStatus = getMhaReplicationStatus(srcMhaName, dstMhaName, dstDbAppliers);
            mhaReplicationTbl.setDrcStatus(drcStatus.getCode());
            mhaReplicationTblDao.update(mhaReplicationTbl);
        }

        // refresh
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProvider scheduledTask error", e);
        }
    }

    private BooleanEnum getMhaReplicationStatus(String srcMhaName, String dstMhaName, List<DbApplierDto> mhaDbAppliers) throws Exception {
        // judge by input
        boolean dbDrcStatus = getDbDrcStatus(mhaDbAppliers.stream());
        if (dbDrcStatus) {
            return BooleanEnum.TRUE;
        }
        // rest mha db appliers
        mhaDbAppliers = getMhaDbAppliers(srcMhaName, dstMhaName);
        dbDrcStatus = getDbDrcStatus(mhaDbAppliers.stream());
        return dbDrcStatus ? BooleanEnum.TRUE : BooleanEnum.FALSE;
    }

    private static boolean getDbDrcStatus(Stream<DbApplierDto> mhaDbAppliers) {
        return mhaDbAppliers.anyMatch(e -> !CollectionUtils.isEmpty(e.getIps()));
    }


    private void configureDbAppliers(List<MhaDbReplicationDto> mhaDbReplicationDtos, List<DbApplierDto> dbApplierDtos) throws SQLException {
        // 0. check dbReplication exist;
        checkExistDbReplication(mhaDbReplicationDtos, dbApplierDtos);

        // 1. applier group
        List<ApplierGroupTblV3> applierGroupTblV3s = insertOrUpdateApplierGroups(mhaDbReplicationDtos, dbApplierDtos);

        // 2. prepare data
        List<ApplierTblV3> allExistAppliers = applierTblV3Dao.queryByApplierGroupIds(applierGroupTblV3s.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList()), 0);
        Map<Long, List<ApplierTblV3>> appliersByGroupIdMap = allExistAppliers.stream().collect(Collectors.groupingBy(ApplierTblV3::getApplierGroupId));

        List<ResourceTbl> resourceTbls = resourceTblDao.queryByType(ModuleEnum.APPLIER.getCode());
        Map<Long, String> resourceIdToIpMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));
        Map<String, Long> resourceIpToIdMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));
        Map<String, MhaDbReplicationDto> map = mhaDbReplicationDtos.stream().collect(Collectors.toMap(e -> e.getSrc().getDbName(), e -> e));
        Map<Long, ApplierGroupTblV3> applierGroupMap = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getMhaDbReplicationId, e -> e));

        // 3. build
        List<ApplierTblV3> insertAppliers = Lists.newArrayList();
        List<ApplierTblV3> deleteAppliers = Lists.newArrayList();
        for (DbApplierDto dbApplierDto : dbApplierDtos) {
            MhaDbReplicationDto dto = map.get(dbApplierDto.getDbName());
            ApplierGroupTblV3 applierGroupTblV3 = applierGroupMap.get(dto.getId());

            List<ApplierTblV3> existAppliers = appliersByGroupIdMap.getOrDefault(applierGroupTblV3.getId(), Collections.emptyList());
            List<String> existIps = existAppliers.stream().map(e -> resourceIdToIpMap.get(e.getResourceId())).collect(Collectors.toList());
            List<String> targetApplierIps = dbApplierDto.getIps();
            Pair<List<String>, List<String>> ipPairs = this.getAddAndDeleteResourceIps(targetApplierIps, existIps);
            List<String> insertIps = ipPairs.getLeft();
            List<String> deleteIps = ipPairs.getRight();

            if (!CollectionUtils.isEmpty(insertIps)) {
                for (String ip : insertIps) {
                    Long resourceId = Optional.ofNullable(resourceIpToIdMap.get(ip)).orElseThrow(() -> ConsoleExceptionUtils.message("ip not exist: " + ip));
                    ApplierTblV3 applierTbl = new ApplierTblV3();
                    applierTbl.setApplierGroupId(applierGroupTblV3.getId());
                    applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
                    applierTbl.setMaster(BooleanEnum.FALSE.getCode());
                    applierTbl.setResourceId(resourceId);
                    applierTbl.setDeleted(BooleanEnum.FALSE.getCode());

                    insertAppliers.add(applierTbl);
                }
            }

            if (!CollectionUtils.isEmpty(deleteIps)) {
                Map<Long, ApplierTblV3> existApplierMap = existAppliers.stream().collect(Collectors.toMap(ApplierTblV3::getResourceId, Function.identity()));
                for (String ip : deleteIps) {
                    Long resourceId = Optional.ofNullable(resourceIpToIdMap.get(ip)).orElseThrow(() -> ConsoleExceptionUtils.message("ip not exist: " + ip));
                    ApplierTblV3 applierTbl = existApplierMap.get(resourceId);
                    applierTbl.setDeleted(BooleanEnum.TRUE.getCode());
                    deleteAppliers.add(applierTbl);
                }
            }
        }
        applierTblV3Dao.batchInsert(insertAppliers);
        applierTblV3Dao.batchUpdate(deleteAppliers);

        logger.info("insert appliers: {}\ndelete appliers: {}", insertAppliers, deleteAppliers);
    }

    private List<ApplierGroupTblV3> insertOrUpdateApplierGroups(List<MhaDbReplicationDto> mhaDbReplicationDtos, List<DbApplierDto> dbApplierDtos) throws SQLException {
        List<Long> replicationIds = mhaDbReplicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
        List<ApplierGroupTblV3> applierGroupTblV3s = applierGroupTblV3Dao.queryByMhaDbReplicationIds(replicationIds);

        Map<String, MhaDbReplicationDto> replciationMap = mhaDbReplicationDtos.stream().collect(Collectors.toMap(e -> e.getSrc().getDbName(), e -> e));
        Map<Long, ApplierGroupTblV3> groupMap = applierGroupTblV3s.stream().collect(Collectors.toMap(ApplierGroupTblV3::getMhaDbReplicationId, e -> e));
        List<ApplierGroupTblV3> insertTbls = Lists.newArrayList();
        List<ApplierGroupTblV3> updateTbls = Lists.newArrayList();
        for (DbApplierDto dbApplierDto : dbApplierDtos) {
            MhaDbReplicationDto replicationDto = replciationMap.get(dbApplierDto.getDbName());
            if (replicationDto == null) {
                throw ConsoleExceptionUtils.message("replicationDto not exist");
            }
            ApplierGroupTblV3 existApplierGroup = groupMap.get(replicationDto.getId());
            if (existApplierGroup == null) {
                ApplierGroupTblV3 applierGroupTbl = new ApplierGroupTblV3();
                applierGroupTbl.setMhaDbReplicationId(replicationDto.getId());
                applierGroupTbl.setGtidInit(dbApplierDto.getGtidInit());
                applierGroupTbl.setConcurrency(dbApplierDto.getConcurrency());
                applierGroupTbl.setDeleted(BooleanEnum.FALSE.getCode());
                insertTbls.add(applierGroupTbl);
            } else {
                existApplierGroup.setDeleted(BooleanEnum.FALSE.getCode());
                existApplierGroup.setGtidInit(dbApplierDto.getGtidInit());
                existApplierGroup.setConcurrency(dbApplierDto.getConcurrency());
                updateTbls.add(existApplierGroup);
            }
        }
        applierGroupTblV3Dao.batchUpdate(updateTbls);
        applierGroupTblV3Dao.batchInsertWithReturnId(insertTbls);

        List<ApplierGroupTblV3> all = Lists.newArrayList(updateTbls);
        all.addAll(insertTbls);
        return all;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public String buildDbMessenger(DrcBuildBaseParam param) throws Exception {
        String mhaName = param.getMhaName();
        List<DbApplierDto> dbApplierDtos = param.getDbApplierDtos();

        MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message("mha not exist");
        }
        if (CollectionUtils.isEmpty(dbApplierDtos)) {
            throw ConsoleExceptionUtils.message("empty config");
        }

        this.checkDbAppliers(dbApplierDtos);
        List<String> dbNames = dbApplierDtos.stream().map(DbApplierDto::getDbName).collect(Collectors.toList());
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryMqByMha(mhaName, dbNames, param.getMqTypeEnum());

        this.checkExistDbReplication(replicationDtos, dbApplierDtos);


        // upsert messenger group
        List<MessengerGroupTblV3> groupTblV3List = this.configureMessengerGroups(dbApplierDtos, replicationDtos, param.getMqTypeEnum());
        // configure messengers
        this.configureMessengers(dbApplierDtos, replicationDtos, groupTblV3List);


        // refresh
        Drc drc = metaInfoService.getDrcMessengerConfig(mhaName, param.getMqTypeEnum());
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProvider scheduledTask error", e);
        }
        return XmlUtils.formatXML(drc.toString());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void switchAppliers(List<DbApplierSwitchReqDto> reqDtos) throws Exception {
        for (DbApplierSwitchReqDto reqDto : reqDtos) {
            String srcMhaName = reqDto.getSrcMhaName();
            String dstMhaName = reqDto.getDstMhaName();
            MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
            MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
            if (srcMha == null || dstMha == null) {
                throw ConsoleExceptionUtils.message("mha not exist: " + srcMhaName + " or " + dstMhaName);
            }

            List<DbApplierDto> mhaDbAppliers = this.getMhaDbAppliers(srcMhaName, dstMhaName);
            boolean newDrc = !getDbDrcStatus(mhaDbAppliers.stream());

            String gtidInit = this.getGtidInit(newDrc, srcMha);
            this.autoConfigDbAppliers(srcMhaName, dstMhaName, reqDto.getDbNames(), gtidInit, reqDto.isSwitchOnly());
        }
    }

    private String getGtidInit(boolean newDrc, MhaTblV2 srcMha) {
        String gtidInit = null;
        if (newDrc) {
            gtidInit = mysqlServiceV2.getMhaExecutedGtid(srcMha.getMhaName());
            if (StringUtils.isBlank(gtidInit)) {
                throw ConsoleExceptionUtils.message("fail to query realtime gtid for mha: " + srcMha.getMhaName());
            }
        }
        return gtidInit;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void switchMessengers(List<MessengerSwitchReqDto> reqDtos) throws Exception {
        for (MessengerSwitchReqDto reqDto : reqDtos) {
            String mhaName = reqDto.getSrcMhaName();
            MqType mqType = reqDto.getMqTypeEnum();

            MhaTblV2 srcMha = mhaTblDao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
            if (srcMha == null) {
                throw ConsoleExceptionUtils.message("mha not exist: " + mhaName);
            }

            List<DbApplierDto> mhaDbMessengers = this.getMhaDbMessengers(mhaName, mqType);
            boolean dbApplyMode = getDbDrcStatus(mhaDbMessengers.stream());

            if (dbApplyMode) {
                throw new IllegalArgumentException("db messenger not support yet");
            }
            String messengerGtidExecuted = messengerServiceV2.getMessengerGtidExecuted(mhaName, mqType);
            if (StringUtils.isBlank(messengerGtidExecuted)) {
                drcBuildServiceV2.autoConfigMessengersWithRealTimeGtid(srcMha, mqType, reqDto.isSwitchOnly());
            } else {
                drcBuildServiceV2.autoConfigMessenger(srcMha, null, mqType, reqDto.isSwitchOnly());
            }
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoConfigDbAppliers(MhaDbReplicationTbl mhaDbReplication, ApplierGroupTblV3 applierGroup, MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, String dbExecutedGtid, Integer concurrency, boolean switchOnly) throws SQLException {
        List<ApplierTblV3> existAppliers = applierTblV3Dao.queryByApplierGroupId(applierGroup.getId(), BooleanEnum.FALSE.getCode());
        if (switchOnly && CollectionUtils.isEmpty(existAppliers)) {
            logger.info("[[mha={}, mhaDbReplication={}]] appliers not exist,do nothing when switchOnly", destMhaTbl.getMhaName(), mhaDbReplication.getId());
            return;
        }
        // applier group gtid
        if (!StringUtils.isBlank(dbExecutedGtid)) {
            applierGroup.setGtidInit(dbExecutedGtid);
            applierGroup.setConcurrency(concurrency);
            applierGroupTblV3Dao.update(applierGroup);
            logger.info("[[mha={}, mhaDbReplication={}]] autoConfigAppliers with gtid:{}", destMhaTbl.getMhaName(), mhaDbReplication.getId(), dbExecutedGtid);
        }
        // mha replication
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), destMhaTbl.getId());
        if (mhaReplicationTbl.getDrcStatus() == 0) {
            mhaReplicationTbl.setDrcStatus(1);
            mhaReplicationTblDao.update(mhaReplicationTbl);
            logger.info("[[mha={}]] autoConfigAppliers update mhaReplicationTbl drcStatus to 1", destMhaTbl.getMhaName());
        }

        // appliers
        List<Long> inUseResourceId = existAppliers.stream().map(ApplierTblV3::getResourceId).collect(Collectors.toList());
        List<String> inUseIps = resourceTblDao.queryByIds(inUseResourceId).stream().map(ResourceTbl::getIp).collect(Collectors.toList());

        ResourceSelectParam selectParam = new ResourceSelectParam();
        selectParam.setType(ModuleEnum.APPLIER.getCode());
        selectParam.setMhaName(destMhaTbl.getMhaName());
        selectParam.setSelectedIps(inUseIps);
        List<ResourceView> resourceViews = resourceService.handOffResource(selectParam);
        if (CollectionUtils.isEmpty(resourceViews)) {
            logger.error("[[mha={}, mhaDbReplication={}]] autoConfigAppliers failed", destMhaTbl.getMhaName(), mhaDbReplication.getId());
            throw new ConsoleException("autoConfigAppliers failed!");
        }
        // insert new appliers
        List<ApplierTblV3> insertAppliers = resourceViews.stream()
                .filter(e -> !inUseResourceId.contains(e.getResourceId()))
                .map(e -> buildApplierTblV3(applierGroup, e))
                .collect(Collectors.toList());

        // delete old appliers
        List<Long> newResourceId = resourceViews.stream().map(ResourceView::getResourceId).collect(Collectors.toList());
        List<ApplierTblV3> deleteAppliers = existAppliers.stream()
                .filter(e -> !newResourceId.contains(e.getResourceId()))
                .collect(Collectors.toList());
        deleteAppliers.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));

        applierTblV3Dao.batchInsert(insertAppliers);
        applierTblV3Dao.batchUpdate(deleteAppliers);
        logger.info("[[mha={}, mhaDbReplication={}]] autoConfigAppliers success", destMhaTbl.getMhaName(), mhaDbReplication.getId());
    }

    private static ApplierTblV3 buildApplierTblV3(ApplierGroupTblV3 applierGroup, ResourceView resourceView) {
        ApplierTblV3 applierTbl = new ApplierTblV3();
        applierTbl.setApplierGroupId(applierGroup.getId());
        applierTbl.setResourceId(resourceView.getResourceId());
        applierTbl.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
        applierTbl.setMaster(BooleanEnum.FALSE.getCode());
        applierTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return applierTbl;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoConfigDbAppliers(String srcMha, String dstMha, List<String> dbNames, String initGtid, boolean switchOnly) throws Exception {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha(srcMha, dstMha, dbNames);
        this.setMhaDbAppliers(replicationDtos);

        List<ResourceView> mhaDbAvailableResource = resourceService.getMhaDbAvailableResource(dstMha, ModuleEnum.APPLIER.getCode());

        List<DbApplierDto> newDbAppliers = Lists.newArrayList();

        for (MhaDbReplicationDto replicationDto : replicationDtos) {
            String dbName = replicationDto.getSrc().getDbName();
            DbApplierDto dbApplierDto = replicationDto.getDbApplierDto();
            if (switchOnly && (dbApplierDto == null || CollectionUtils.isEmpty(dbApplierDto.getIps()))) {
                continue;
            }
            // gtid
            String newGtid = getInitGtid(initGtid, dbApplierDto, dbName);
            // ips
            List<String> inUseIps = dbApplierDto == null || CollectionUtils.isEmpty(dbApplierDto.getIps()) ? Collections.emptyList() : dbApplierDto.getIps();
            List<ResourceView> resourceViews = resourceService.handOffResource(inUseIps, mhaDbAvailableResource);
            List<String> newIps = resourceViews.stream().map(ResourceView::getIp).collect(Collectors.toList());

            Integer concurrency = Optional.ofNullable(dbApplierDto).map(DbApplierDto::getConcurrency).orElse(null);
            newDbAppliers.add(new DbApplierDto(newIps, newGtid.trim(), dbName, concurrency));
        }
        DrcBuildParam drcBuildParam = new DrcBuildParam();
        DrcBuildBaseParam srcBuildParam = new DrcBuildBaseParam();
        srcBuildParam.setMhaName(srcMha);
        drcBuildParam.setSrcBuildParam(srcBuildParam);
        DrcBuildBaseParam dstBuildParam = new DrcBuildBaseParam();
        dstBuildParam.setMhaName(dstMha);
        dstBuildParam.setDbApplierDtos(newDbAppliers);
        drcBuildParam.setDstBuildParam(dstBuildParam);
        this.buildDbApplier(drcBuildParam);
        logger.info("autoConfigDbAppliers success: {}->{} ({});\ninitGtid: {}", srcMha, dstMha, dbNames, initGtid);
    }

    private String getInitGtid(String givenGtid, DbApplierDto dbApplierDto, String dbName) {
        if (dbApplierDto != null && !StringUtils.isBlank(dbApplierDto.getGtidInit())) {
            return dbApplierDto.getGtidInit();
        }

        if (StringUtils.isBlank(givenGtid)) {
            throw ConsoleExceptionUtils.message("init gtid required for db: " + dbName);
        }

        return givenGtid;

    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void autoConfigDbAppliersWithRealTimeGtid(MhaDbReplicationTbl mhaDbReplication, ApplierGroupTblV3 applierGroup,
                                                     MhaTblV2 srcMhaTbl, MhaTblV2 destMhaTbl, Integer concurrency) throws SQLException {
        String mhaExecutedGtid = mysqlServiceV2.getMhaExecutedGtid(srcMhaTbl.getMhaName());
        if (StringUtils.isBlank(mhaExecutedGtid)) {
            logger.error("[[mha={}]] getMhaExecutedGtid failed", srcMhaTbl.getMhaName());
            throw new ConsoleException("getMhaExecutedGtid failed!");
        }
        this.autoConfigDbAppliers(mhaDbReplication, applierGroup, srcMhaTbl, destMhaTbl, mhaExecutedGtid, concurrency, false);
    }

    @Override
    public List<DbDrcConfigInfoDto> getExistDbReplicationDirections(String dbName) {
        List<String> dbNames = this.getDbNamesWithinSameDalCluster(dbName).getDbNames();
        return this.getExistDbReplicationDirections(dbNames);
    }

    @Override
    public List<DbMqConfigInfoDto> getExistDbMqConfigDcOption(String dbName, MqType mqType) {
        List<String> dbNames = this.getDbNamesWithinSameDalCluster(dbName).getDbNames();
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, mqType.getReplicationType());
        return mhaDbReplicationDtos.stream()
                .map(e -> new DbMqConfigInfoDto(e.getSrc().getRegionName()))
                .distinct().collect(Collectors.toList());
    }

    private List<DbDrcConfigInfoDto> getExistDbReplicationDirections(List<String> dbNames) {
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, ReplicationTypeEnum.DB_TO_DB);
        return mhaDbReplicationDtos.stream()
                .map(e -> new DbDrcConfigInfoDto(e.getSrc().getRegionName(), e.getDst().getRegionName()))
                .distinct().collect(Collectors.toList());
    }

    @Override
    public DbDrcConfigInfoDto getDbDrcConfig(String dbName, String srcRegionName, String dstRegionName) {
        List<String> dbNames = this.getDbNamesWithinSameDalCluster(dbName).getDbNames();
        return getDbDrcConfig(dbNames, srcRegionName, dstRegionName);
    }

    @Override
    public DbMqConfigInfoDto getDbMqConfig(String dbName, String srcRegionName, MqType mqType) {
        ShardDatabaseInfoDto infoDto = this.getDbNamesWithinSameDalCluster(dbName);
        return getDbMqConfig(infoDto.getDalClusterName(), infoDto.getDbNames(), srcRegionName, mqType);
    }

    private DbDrcConfigInfoDto getDbDrcConfig(List<String> dbNames, String srcRegionName, String dstRegionName) {
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, ReplicationTypeEnum.DB_TO_DB)
                .stream().filter(e -> e.getSrc().getRegionName().equals(srcRegionName) && e.getDst().getRegionName().equals(dstRegionName)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(mhaDbReplicationDtos)) {
            throw ConsoleExceptionUtils.message("drc config empty");
        }

        // 1. logic table config consistency check
        long count = mhaDbReplicationDtos.stream().map(e -> Sets.newHashSet(e.getDbReplicationDtos().stream().map(DbReplicationDto::getLogicTableConfig).collect(Collectors.toList()))).distinct().count();
        if (count > 1) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DB_REPLICATION_NOT_CONSISTENT);
        }

        // 2. group by mha
        Map<MhaReplicationDto, List<MhaDbReplicationDto>> collect = mhaDbReplicationDtos.stream().collect(Collectors.groupingBy(e -> {
            MhaReplicationDto mhaReplicationDto = new MhaReplicationDto();
            mhaReplicationDto.setSrcMha(MhaDto.from(e.getSrc()));
            mhaReplicationDto.setDstMha(MhaDto.from(e.getDst()));
            return mhaReplicationDto;
        }));

        for (Map.Entry<MhaReplicationDto, List<MhaDbReplicationDto>> entry : collect.entrySet()) {
            MhaReplicationDto mhaReplicationDto = entry.getKey();
            List<MhaDbReplicationDto> list = entry.getValue();
            mhaReplicationDto.setMhaDbReplications(list);
            if (isAdmin()) {
                // drc resource info detail
                setMhaDbAppliers(list);
                mhaReplicationDto.getSrcMha().setReplicatorInfoDtos(mhaServiceV2.getMhaReplicatorsV2(mhaReplicationDto.getSrcMha().getName()));
            }
        }
        DbDrcConfigInfoDto dbDrcConfigInfoDto = new DbDrcConfigInfoDto(srcRegionName, dstRegionName);
        dbDrcConfigInfoDto.setMhaReplications(Lists.newArrayList(collect.keySet()));
        dbDrcConfigInfoDto.setLogicTableSummaryDtos(LogicTableSummaryDto.from(mhaDbReplicationDtos));
        dbDrcConfigInfoDto.setDbNames(dbNames);
        return dbDrcConfigInfoDto;
    }

    public DbMqConfigInfoDto getDbMqConfig(String dalclusterName, List<String> dbNames, String srcRegionName, MqType mqType) {
        try {
            List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, mqType.getReplicationType())
                    .stream().filter(e -> e.getSrc().getRegionName().equals(srcRegionName)).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(mhaDbReplicationDtos)) {
                throw ConsoleExceptionUtils.message("drc mq config empty");
            }

            // 1. logic table config consistency check
            List<Set<LogicTableConfig>> distinctConfig = mhaDbReplicationDtos.stream().map(e -> Sets.newHashSet(
                    e.getDbReplicationDtos().stream().map(DbReplicationDto::getLogicTableConfig).collect(Collectors.toList()))).distinct().collect(Collectors.toList());
            long count = distinctConfig.size();
            if (count > 1) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DB_REPLICATION_NOT_CONSISTENT, JsonUtils.toJson(distinctConfig));
            }

            // 2. group by mha
            Map<MhaMqDto, List<MhaDbReplicationDto>> collect = mhaDbReplicationDtos.stream().collect(Collectors.groupingBy(e -> {
                MhaMqDto mhaMqDto = new MhaMqDto();
                mhaMqDto.setSrcMha(MhaDto.from(e.getSrc()));
                return mhaMqDto;
            }));

            for (Map.Entry<MhaMqDto, List<MhaDbReplicationDto>> entry : collect.entrySet()) {
                MhaMqDto mhaMqDto = entry.getKey();
                List<MhaDbReplicationDto> list = entry.getValue();
                mhaMqDto.setMhaDbReplications(list);
                // drc resource info detail
                mhaMqDto.getSrcMha().setReplicatorInfoDtos(mhaServiceV2.getMhaReplicatorsV2(mhaMqDto.getSrcMha().getName()));
                setMhaDbMessengers(list, mqType);
                boolean dbApplyMode = getDbDrcStatus(list.stream().map(MhaDbReplicationDto::getDbApplierDto));
                if (!dbApplyMode) {
                    setMhaMessengers(mhaMqDto, mqType);
                }
            }
            DbMqConfigInfoDto dbDrcConfigInfoDto = new DbMqConfigInfoDto(srcRegionName);
            dbDrcConfigInfoDto.setMhaMqDtos(Lists.newArrayList(collect.keySet()));
            List<LogicTableSummaryDto> logicTableSummaryDtos = LogicTableSummaryDto.from(mhaDbReplicationDtos);
            List<Long> messengerFilerIds = logicTableSummaryDtos.stream().map(e -> e.getConfig().getMessengerFilterId()).filter(Objects::nonNull).collect(Collectors.toList());

            List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryByIds(messengerFilerIds);
            Map<Long, MqConfig> idToMqConfig = messengerFilterTbls.stream().collect(Collectors.toMap(
                    MessengerFilterTbl::getId,
                    e -> JsonUtils.fromJson(e.getProperties(), MqConfig.class
                    )
            ));

            List<MqLogicTableSummaryDto> mqLogicTableSummaryDtos = logicTableSummaryDtos.stream().map(e -> {
                MqLogicTableSummaryDto mqLogicTableSummaryDto = new MqLogicTableSummaryDto(e.getDbReplicationIds(), e.getConfig(), e.getDatachangeLasttime());

                Long messengerFilterId = mqLogicTableSummaryDto.getConfig().getMessengerFilterId();
                MqConfig mqConfig = idToMqConfig.get(messengerFilterId);
                mqLogicTableSummaryDto.setMqType(mqConfig.getMqType());
                mqLogicTableSummaryDto.setSerialization(mqConfig.getSerialization());
                mqLogicTableSummaryDto.setOrder(mqConfig.isOrder());
                mqLogicTableSummaryDto.setOrderKey(mqConfig.getOrderKey());
                mqLogicTableSummaryDto.setPersistent(mqConfig.isPersistent());
                mqLogicTableSummaryDto.setExcludeFilterTypes(mqConfig.getExcludeFilterTypes());
                mqLogicTableSummaryDto.setDelayTime(mqConfig.getDelayTime());
                mqLogicTableSummaryDto.setFilterFields(mqConfig.getFilterFields());
                mqLogicTableSummaryDto.setSendOnlyUpdated(mqConfig.isSendOnlyUpdated());
                mqLogicTableSummaryDto.setExcludeColumn(mqConfig.isExcludeColumn());
                return mqLogicTableSummaryDto;
            }).collect(Collectors.toList());
            mqLogicTableSummaryDtos.sort(
                    Comparator.comparing(MqLogicTableSummaryDto::getDatachangeLasttime, Comparator.nullsLast(Comparator.naturalOrder()))
            );
            dbDrcConfigInfoDto.setLogicTableSummaryDtos(mqLogicTableSummaryDtos);
            dbDrcConfigInfoDto.setDbNames(dbNames);
            dbDrcConfigInfoDto.setDalclusterName(dalclusterName);
            return dbDrcConfigInfoDto;
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }


    private void setMhaMessengers(MhaMqDto mhaReplicationDto, MqType mqType) {
        String srcMhaName = mhaReplicationDto.getSrcMha().getName();
        MhaMessengerDto mhaMessengerDto = mhaServiceV2.getMhaMessengers(srcMhaName, mqType);
        mhaReplicationDto.setMhaMessengerDto(mhaMessengerDto);
    }

    @Override
    public RowsFilterConfigView getRowsConfigViewById(long rowsFilterId) {
        try {
            RowsFilterTblV2 rowsFilterTblV2 = rowsFilterTblV2Dao.queryById(rowsFilterId);
            return RowsFilterConfigView.from(rowsFilterTblV2);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public ColumnsConfigView getColumnsConfigViewById(long colsFilterId) {
        try {
            ColumnsFilterTblV2 columnsFilterTblV2 = columnFilterTblV2Dao.queryById(colsFilterId);
            return ColumnsConfigView.from(columnsFilterTblV2);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void createMhaDbDrcReplication(MhaDbReplicationCreateDto createDto) throws Exception {
        createDto.validAndTrimForCreateReq();
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setDbName(createDto.getDbName());
        req.setMode(DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME.getValue());
        req.setSrcRegionName(createDto.getSrcRegionName());
        req.setDstRegionName(createDto.getDstRegionName());
        req.setTblsFilterDetail(new DrcAutoBuildReq.TblsFilterDetail());
        List<DrcAutoBuildParam> drcBuildParam = drcAutoBuildService.getDrcBuildParam(req);
        List<String> dbNames = drcBuildParam.stream().flatMap(e -> e.getDbName().stream()).collect(Collectors.toList());
        // check existence
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, ReplicationTypeEnum.DB_TO_DB)
                .stream().filter(e -> e.getSrc().getRegionName().equals(createDto.getSrcRegionName()) && e.getDst().getRegionName().equals(createDto.getDstRegionName()))
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(mhaDbReplicationDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.CREATE_MHA_DB_REPLICATION_FAIL_CONFIG_EXIST);
        }
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        List<String> buCodes = dbTbls.stream().map(DbTbl::getBuCode).distinct().collect(Collectors.toList());
        if (CollectionUtils.isEmpty(buCodes) || buCodes.size() > 1) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.GET_BU_CODE_FOR_DB_FAIL);
        }
        req.setBuName(buCodes.get(0));
        req.autoSetTag();


        for (DrcAutoBuildParam param : drcBuildParam) {
            param.setBuName(req.getBuName());
            param.setTag(req.getTag());
            // 1.(if needed) build mha
            DrcMhaBuildParam mhaBuildParam = new DrcMhaBuildParam(
                    param.getSrcMhaName(),
                    param.getDstMhaName(),
                    param.getSrcDcName(),
                    param.getDstDcName(),
                    param.getBuName(),
                    param.getTag(),
                    param.getTag(),
                    param.getSrcMachines(),
                    param.getDstMachines()
            );
            drcBuildServiceV2.buildMhaAndReplication(mhaBuildParam);
            if (param.getSrcMhaName().equals(param.getDstMhaName())) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DRC_SAME_MHA_NOT_SUPPORTED, String.format("src: %s, dst: %s", param.getSrcMhaName(), param.getDstMhaName()));
            }
            MhaTblV2 srcMhaTbl = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
            MhaTblV2 dstMhaTbl = mhaTblDao.queryByMhaName(param.getDstMhaName(), BooleanEnum.FALSE.getCode());

            // check result
            if (srcMhaTbl == null || dstMhaTbl == null) {
                throw ConsoleExceptionUtils.message("init mha fail");
            }
            //  2. mha replication
            MhaReplicationTbl srcToDstMhaReplication = mhaReplicationTblDao.queryByMhaId(srcMhaTbl.getId(), dstMhaTbl.getId(), BooleanEnum.FALSE.getCode());
            MhaReplicationTbl dstToSrcMhaReplication = mhaReplicationTblDao.queryByMhaId(dstMhaTbl.getId(), srcMhaTbl.getId(), BooleanEnum.FALSE.getCode());
            if (srcToDstMhaReplication == null || dstToSrcMhaReplication == null) {
                throw ConsoleExceptionUtils.message("init mhaReplication fail");
            }

            // 3. sync mha db info
            drcBuildServiceV2.syncMhaDbInfoFromDbaApiIfNeeded(srcMhaTbl, param.getSrcMachines());
            drcBuildServiceV2.syncMhaDbInfoFromDbaApiIfNeeded(dstMhaTbl, param.getDstMachines());

            // 4. mha db replication
            mhaDbReplicationService.maintainMhaDbReplication(param.getSrcMhaName(), param.getDstMhaName(), Lists.newArrayList(param.getDbName()));
        }
    }

    /**
     * buildMha and prepareMha should not be done in a transaction
     */
    @Override
    public void createMhaDbReplicationForMq(MhaDbReplicationCreateDto createDto) throws Exception {
        createDto.validAndTrimForCreateReq();
        List<DrcAutoBuildParam> drcBuildParam = buildMha(createDto);
        prepareMha(createDto, drcBuildParam);
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w", exceptionWrappedByDalException = false)
    protected void prepareMha(MhaDbReplicationCreateDto createDto, List<DrcAutoBuildParam> drcBuildParam) throws SQLException {
        Integer replicationType = createDto.getReplicationType();
        ReplicationTypeEnum replicationTypeEnum = ReplicationTypeEnum.getByType(replicationType);

        for (DrcAutoBuildParam param : drcBuildParam) {
            MhaTblV2 srcMhaTbl = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());
            try {

                // 4. mha db replication
                mhaDbReplicationService.maintainMhaDbReplicationForMq(param.getSrcMhaName(), Lists.newArrayList(param.getDbName()), replicationTypeEnum);

                // 5. replicator
                replicatorGroupTblDao.upsertIfNotExist(srcMhaTbl.getId());
                drcBuildServiceV2.autoConfigReplicatorsWithRealTimeGtid(srcMhaTbl);
            } catch (Exception e) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.CONFIGURE_MESSENGER_MHA_FAIL, "mha: " + srcMhaTbl.getMhaName(), e);
            }
        }
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w", exceptionWrappedByDalException = false)
    protected List<DrcAutoBuildParam> buildMha(MhaDbReplicationCreateDto createDto) throws Exception {
        Integer replicationType = createDto.getReplicationType();
        ReplicationTypeEnum replicationTypeEnum = ReplicationTypeEnum.getByType(replicationType);
        MqType mqType = MqType.parseByReplicationType(replicationTypeEnum);

        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setDbName(createDto.getDbName());
        req.setMode(DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME.getValue());
        req.setSrcRegionName(createDto.getSrcRegionName());
        req.setDstRegionName(createDto.getDstRegionName());
        req.setTblsFilterDetail(new DrcAutoBuildReq.TblsFilterDetail());
        req.setReplicationType(replicationType);
        req.setMqType(mqType.name());

        List<DrcAutoBuildParam> drcBuildParam = drcAutoBuildService.getDrcBuildParam(req);
        List<String> dbNames = drcBuildParam.stream().flatMap(e -> e.getDbName().stream()).collect(Collectors.toList());
        // check existence
        List<MhaDbReplicationDto> mhaDbReplicationDtos = mhaDbReplicationService.queryByDbNames(dbNames, replicationTypeEnum)
                .stream().filter(e -> e.getSrc().getRegionName().equals(createDto.getSrcRegionName()))
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(mhaDbReplicationDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.CREATE_MHA_DB_REPLICATION_FAIL_CONFIG_EXIST);
        }
        List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbNames);
        List<String> buCodes = dbTbls.stream().map(DbTbl::getBuCode).distinct().collect(Collectors.toList());
        if (CollectionUtils.isEmpty(buCodes) || buCodes.size() > 1) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.GET_BU_CODE_FOR_DB_FAIL);
        }
        req.setBuName(buCodes.get(0));
        req.autoSetTag();


        for (DrcAutoBuildParam param : drcBuildParam) {
            param.setBuName(req.getBuName());
            param.setTag(req.getTag());


            // 1.(if needed) build mha
            MessengerMhaBuildParam mhaBuildParam = new MessengerMhaBuildParam();
            mhaBuildParam.setTag(param.getTag());
            mhaBuildParam.setMhaName(param.getSrcMhaName());
            mhaBuildParam.setDc(param.getSrcDcName());
            mhaBuildParam.setBuName(param.getBuName());
            mhaBuildParam.setMachineDto(param.getSrcMachines());
            mhaBuildParam.setMqType(mqType.name());
            drcBuildServiceV2.buildMessengerMha(mhaBuildParam);

            MhaTblV2 srcMhaTbl = mhaTblDao.queryByMhaName(param.getSrcMhaName(), BooleanEnum.FALSE.getCode());

            // check result
            if (srcMhaTbl == null) {
                throw ConsoleExceptionUtils.message("init mha fail");
            }
            try {
                // 3. sync mha db info
                drcBuildServiceV2.syncMhaDbInfoFromDbaApiIfNeeded(srcMhaTbl, param.getSrcMachines());
            } catch (Exception e) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.CONFIGURE_MESSENGER_MHA_FAIL, "mha: " + srcMhaTbl.getMhaName(), e);
            }
        }
        return drcBuildParam;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void editDbReplication(DbReplicationEditDto editDto) throws Exception {
        editDto.validAndTrim();
        // check original config not modified
        DbDrcConfigInfoDto dbDrcConfig = this.getDbDrcConfig(editDto.getDbNames(), editDto.getSrcRegionName(), editDto.getDstRegionName());
        List<LogicTableSummaryDto> logicTableSummaryDtos = dbDrcConfig.getLogicTableSummaryDtos();
        Set<Long> requestEditIdSet = Sets.newHashSet(editDto.getDbReplicationIds());
        List<LogicTableSummaryDto> summaryDtos = logicTableSummaryDtos.stream().filter(e -> Sets.newHashSet(e.getDbReplicationIds()).equals(requestEditIdSet)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(summaryDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
        }

        List<MhaReplicationDto> mhaReplications = dbDrcConfig.getMhaReplications();

        // convert
        List<DbReplicationBuildParam> dbReplicationBuildParams = Lists.newArrayList();
        for (MhaReplicationDto mhaReplication : mhaReplications) {
            DbReplicationBuildParam param = new DbReplicationBuildParam();
            param.setSrcMhaName(mhaReplication.getSrcMha().getName());
            param.setDstMhaName(mhaReplication.getDstMha().getName());
            param.setRowsFilterCreateParam(editDto.getRowsFilterCreateParam());
            param.setColumnsFilterCreateParam(editDto.getColumnsFilterCreateParam());
            param.setDbNames(mhaReplication.getMhaDbReplications().stream().map(e -> e.getSrc().getDbName()).collect(Collectors.toList()));
            List<LogicTableSummaryDto> oldSummaryDtos = LogicTableSummaryDto.from(mhaReplication.getMhaDbReplications());
            List<LogicTableSummaryDto> targetOriginSummaryDtos = oldSummaryDtos.stream().filter(e -> e.getConfig().equals(editDto.getOriginLogicTableConfig())).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(targetOriginSummaryDtos) || targetOriginSummaryDtos.size() != 1) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
            }
            LogicTableSummaryDto logicTableSummaryDto = targetOriginSummaryDtos.get(0);
            param.setDbReplicationIds(logicTableSummaryDto.getDbReplicationIds());
            param.setTableName(editDto.getLogicTableConfig().getLogicTable());

            dbReplicationBuildParams.add(param);
        }
        this.check(dbReplicationBuildParams);
        // do config
        for (DbReplicationBuildParam buildParam : dbReplicationBuildParams) {
            drcBuildServiceV2.buildDbReplicationConfig(buildParam);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void createDbReplication(DbReplicationCreateDto createDto) throws Exception {
        createDto.validAndTrim();
        DbDrcConfigInfoDto dbDrcConfig = this.getDbDrcConfig(createDto.getDbNames(), createDto.getSrcRegionName(), createDto.getDstRegionName());
        List<MhaReplicationDto> mhaReplications = dbDrcConfig.getMhaReplications();

        // convert
        List<DbReplicationBuildParam> dbReplicationBuildParams = Lists.newArrayList();
        for (MhaReplicationDto mhaReplication : mhaReplications) {
            DbReplicationBuildParam param = new DbReplicationBuildParam();
            param.setSrcMhaName(mhaReplication.getSrcMha().getName());
            param.setDstMhaName(mhaReplication.getDstMha().getName());
            param.setRowsFilterCreateParam(createDto.getRowsFilterCreateParam());
            param.setColumnsFilterCreateParam(createDto.getColumnsFilterCreateParam());
            param.setTableName(createDto.getLogicTableConfig().getLogicTable());
            param.setDbNames(mhaReplication.getMhaDbReplications().stream().map(e -> e.getSrc().getDbName()).collect(Collectors.toList()));
            dbReplicationBuildParams.add(param);
        }
        this.check(dbReplicationBuildParams);
        // do config
        for (DbReplicationBuildParam buildParam : dbReplicationBuildParams) {
            drcBuildServiceV2.buildDbReplicationConfig(buildParam);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void createDbMqReplication(DbMqCreateDto createDto) {
        createDto.validAndTrim();
        DbMqConfigInfoDto dbMqConfig = this.getDbMqConfig(createDto.getDalclusterName(), createDto.getDbNames(), createDto.getSrcRegionName(), createDto.getMqConfig().getMqTypeEnum());
        messengerBatchConfigService.processCreateMqConfig(createDto, dbMqConfig);
        // refresh registry
        this.refreshRegistryConfig(createDto);
    }

    private void refreshRegistryConfig(DbMqCreateDto createDto) {
        if (createDto.getMqConfig().getMqTypeEnum().notSupportDalClient()) {
            return;
        }
        DbMqConfigInfoDto dbMqConfig;
        dbMqConfig = this.getDbMqConfig(createDto.getDalclusterName(), createDto.getDbNames(), createDto.getSrcRegionName(), createDto.getMqConfig().getMqTypeEnum());
        messengerBatchConfigService.refreshRegistryConfig(dbMqConfig);
    }


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w", exceptionWrappedByDalException = false)
    public void editDbMqReplication(DbMqEditDto editDto) {
        editDto.validAndTrim();
        DbMqConfigInfoDto dbMqConfig = this.getDbMqConfig(editDto.getDalclusterName(), editDto.getDbNames(), editDto.getSrcRegionName(), editDto.getMqConfig().getMqTypeEnum());
        // check original config not modified
        List<MqLogicTableSummaryDto> logicTableSummaryDtos = dbMqConfig.getLogicTableSummaryDtos();
        Set<Long> requestEditIdSet = Sets.newHashSet(editDto.getDbReplicationIds());
        List<LogicTableSummaryDto> summaryDtos = logicTableSummaryDtos.stream().filter(e -> Sets.newHashSet(e.getDbReplicationIds()).equals(requestEditIdSet)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(summaryDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
        }

        messengerBatchConfigService.processUpdateMqConfig(editDto, dbMqConfig);
        this.refreshRegistryConfig(editDto);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDbMqReplication(DbMqEditDto editDto) {
        editDto.validAndTrim();
        DbMqConfigInfoDto dbMqConfig = this.getDbMqConfig(editDto.getDalclusterName(), editDto.getDbNames(), editDto.getSrcRegionName(), editDto.getMqConfig().getMqTypeEnum());
        // check original config not modified
        List<MqLogicTableSummaryDto> logicTableSummaryDtos = dbMqConfig.getLogicTableSummaryDtos();
        Set<Long> requestEditIdSet = Sets.newHashSet(editDto.getDbReplicationIds());
        List<LogicTableSummaryDto> summaryDtos = logicTableSummaryDtos.stream().filter(e -> Sets.newHashSet(e.getDbReplicationIds()).equals(requestEditIdSet)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(summaryDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
        }

        messengerBatchConfigService.processDeleteMqConfig(editDto, dbMqConfig);
        this.refreshRegistryConfig(editDto);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDbReplication(DbReplicationEditDto editDto) throws Exception {
        editDto.validAndTrim();
        // check original config not modified
        DbDrcConfigInfoDto dbDrcConfig = this.getDbDrcConfig(editDto.getDbNames(), editDto.getSrcRegionName(), editDto.getDstRegionName());
        List<LogicTableSummaryDto> logicTableSummaryDtos = dbDrcConfig.getLogicTableSummaryDtos();
        Set<Long> requestEditIdSet = Sets.newHashSet(editDto.getDbReplicationIds());
        List<LogicTableSummaryDto> summaryDtos = logicTableSummaryDtos.stream().filter(e -> Sets.newHashSet(e.getDbReplicationIds()).equals(requestEditIdSet)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(summaryDtos)) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
        }

        List<MhaReplicationDto> mhaReplications = dbDrcConfig.getMhaReplications();

        // convert
        List<DbReplicationBuildParam> dbReplicationBuildParams = Lists.newArrayList();
        for (MhaReplicationDto mhaReplication : mhaReplications) {
            List<LogicTableSummaryDto> oldSummaryDtos = LogicTableSummaryDto.from(mhaReplication.getMhaDbReplications());
            List<LogicTableSummaryDto> deleteTarget = oldSummaryDtos.stream().filter(e -> e.getConfig().equals(editDto.getOriginLogicTableConfig())).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(deleteTarget) || deleteTarget.size() != 1) {
                throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.ORIGINAL_DB_REPLICATION_CONFIG_NOT_EXIST);
            }

            DbReplicationBuildParam param = new DbReplicationBuildParam();
            param.setDbReplicationIds(deleteTarget.get(0).getDbReplicationIds());
            dbReplicationBuildParams.add(param);
        }
        this.check(dbReplicationBuildParams);
        // do config
        for (DbReplicationBuildParam param : dbReplicationBuildParams) {
            drcBuildServiceV2.deleteDbReplications(param.getDbReplicationIds());
        }
    }


    private void check(List<DbReplicationBuildParam> dbReplicationBuildParams) {
        return;
    }

    private boolean isAdmin() {
        return true;
    }

    private ShardDatabaseInfoDto getDbNamesWithinSameDalCluster(String dbName) {
        String dalClusterName = getDalclusterName(dbName);
        List<String> dbNamesByDalClusterName = getDbNamesByDalClusterName(dalClusterName);
        return new ShardDatabaseInfoDto(dalClusterName, dbNamesByDalClusterName);

    }

    private List<String> getDbNamesByDalClusterName(String dalClusterName) {
        try {
            List<DbClusterInfoDto> databaseClusterInfoList = dbaApiService.getDatabaseClusterInfoList(dalClusterName);
            return databaseClusterInfoList.stream().map(DbClusterInfoDto::getDbName).collect(Collectors.toList());
        } catch (Exception e) {
            if (EnvUtils.pro()) {
                throw e;
            }
        }
        // test shard db
        try {
            List<DbTbl> dbTbls = dbTblDao.queryAllExist();
            return dbTbls.stream().map(DbTbl::getDbName).filter(db -> dalClusterName.equals(DalclusterUtils.getDalClusterName(db))).collect(Collectors.toList());
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE, e);
        }
    }

    private final LoadingCache<String, String> cache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public String load(@NotNull String dbName) {
                    return getDalClusterName(dbName);
                }
            });

    private String getDalclusterName(String dbName) {
        try {
            cache.getUnchecked(dbName);
        } catch (Exception e) {
            if (EnvUtils.pro()) {
                throw e;
            }
        }

        String dalClusterName = DalclusterUtils.getDalClusterName(dbName);
        logger.info("get dalcluster from dba api fail. DbName: {}, Assume dalclusterName: {}  ", dbName, dalClusterName);
        return dalClusterName;

    }

    private String getDalClusterName(String dbName) {
        return dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(), dbName);
    }

    private void checkExistDbReplication(List<MhaDbReplicationDto> mhaDbReplicationDtos, List<DbApplierDto> dbApplierDtos) {
        Map<String, MhaDbReplicationDto> map = mhaDbReplicationDtos.stream().collect(Collectors.toMap(e -> e.getSrc().getDbName(), e -> e));
        List<String> notConfiguredDbNames = Lists.newArrayList();
        for (DbApplierDto applierDto : dbApplierDtos) {
            MhaDbReplicationDto dto = map.get(applierDto.getDbName());
            if (!CollectionUtils.isEmpty(applierDto.getIps()) && CollectionUtils.isEmpty(dto.getDbReplicationDtos())) {
                notConfiguredDbNames.add(applierDto.getDbName());
            }
        }
        if (!notConfiguredDbNames.isEmpty()) {
            throw ConsoleExceptionUtils.message("dbReplication not config yet, cannot config applier: " + String.join(",", notConfiguredDbNames));
        }
    }

    private List<MessengerGroupTblV3> configureMessengerGroups(List<DbApplierDto> dbApplierDtos, List<MhaDbReplicationDto> replicationDtos, MqType mqType) throws SQLException {
        Map<String, DbApplierDto> applierMap = dbApplierDtos.stream().collect(Collectors.toMap(DbApplierDto::getDbName, e -> e));
        List<MessengerGroupTblV3> groupTblV3List = replicationDtos.stream().map(e -> {
            MessengerGroupTblV3 messengerGroupTblV3 = new MessengerGroupTblV3();
            messengerGroupTblV3.setMhaDbReplicationId(e.getId());
            messengerGroupTblV3.setGtidExecuted(applierMap.get(e.getSrc().getDbName()).getGtidInit());
            messengerGroupTblV3.setMqType(mqType.name());
            messengerGroupTblV3.setDeleted(0);
            return messengerGroupTblV3;
        }).collect(Collectors.toList());
        messengerGroupTblV3Dao.upsert(groupTblV3List, mqType);
        return groupTblV3List;
    }

    private void configureMessengers(List<DbApplierDto> dbApplierDtos, List<MhaDbReplicationDto> replicationDtos, List<MessengerGroupTblV3> groupTblV3List) throws SQLException {
        List<ResourceTbl> resourceTbls = resourceTblDao.queryAllExist();
        Map<Long, String> resourceIdToIpMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));
        Map<String, Long> resourceIpToIdMap = resourceTbls.stream().collect(Collectors.toMap(ResourceTbl::getIp, ResourceTbl::getId));


        List<MessengerTblV3> allExistAppliers = messengerTblV3Dao.queryByGroupIds(groupTblV3List.stream().map(MessengerGroupTblV3::getId).collect(Collectors.toList()));
        Map<Long, List<MessengerTblV3>> messengersByGroupIdMap = allExistAppliers.stream().collect(Collectors.groupingBy(MessengerTblV3::getMessengerGroupId));


        Map<String, MhaDbReplicationDto> map = replicationDtos.stream().collect(Collectors.toMap(e -> e.getSrc().getDbName(), e -> e));
        Map<Long, MessengerGroupTblV3> applierGroupMap = groupTblV3List.stream().collect(Collectors.toMap(MessengerGroupTblV3::getMhaDbReplicationId, e -> e));

        // upsert messenger
        List<MessengerTblV3> insertMessengers = Lists.newArrayList();
        List<MessengerTblV3> deleteMessengers = Lists.newArrayList();

        for (DbApplierDto dbApplierDto : dbApplierDtos) {
            MhaDbReplicationDto dto = map.get(dbApplierDto.getDbName());
            MessengerGroupTblV3 messengerGroupTblV3 = applierGroupMap.get(dto.getId());
            List<MessengerTblV3> existMessengers = messengersByGroupIdMap.getOrDefault(messengerGroupTblV3.getId(), Collections.emptyList());

            List<String> existIps = existMessengers.stream().map(e -> resourceIdToIpMap.get(e.getResourceId())).collect(Collectors.toList());
            List<String> targetMessengerIps = dbApplierDto.getIps();
            Pair<List<String>, List<String>> ipPairs = this.getAddAndDeleteResourceIps(targetMessengerIps, existIps);
            List<String> insertIps = ipPairs.getLeft();
            List<String> deleteIps = ipPairs.getRight();

            if (!CollectionUtils.isEmpty(insertIps)) {
                for (String ip : insertIps) {
                    MessengerTblV3 messengerTblV3 = new MessengerTblV3();
                    messengerTblV3.setMessengerGroupId(messengerGroupTblV3.getId());
                    messengerTblV3.setPort(ConsoleConfig.DEFAULT_APPLIER_PORT);
                    messengerTblV3.setResourceId(resourceIpToIdMap.get(ip));
                    messengerTblV3.setDeleted(BooleanEnum.FALSE.getCode());
                    insertMessengers.add(messengerTblV3);
                }
            }

            if (!CollectionUtils.isEmpty(deleteIps)) {
                Map<Long, MessengerTblV3> existApplierMap = existMessengers.stream().collect(Collectors.toMap(MessengerTblV3::getResourceId, Function.identity()));
                for (String ip : deleteIps) {
                    MessengerTblV3 messengerTblV3 = existApplierMap.get(resourceIpToIdMap.get(ip));
                    messengerTblV3.setDeleted(BooleanEnum.TRUE.getCode());
                    deleteMessengers.add(messengerTblV3);
                }
            }
        }
        messengerTblV3Dao.batchInsert(insertMessengers);
        messengerTblV3Dao.batchUpdate(deleteMessengers);

        logger.info("insert messengers: {}\ndelete messengers: {}", insertMessengers, deleteMessengers);
    }

    private void checkDbAppliers(List<DbApplierDto> list) {
        Set<String> duplicates = list.stream().map(DbApplierDto::getDbName).filter(i -> Collections.frequency(list, i) > 1).collect(Collectors.toSet());
        if (!CollectionUtils.isEmpty(duplicates)) {
            throw ConsoleExceptionUtils.message("check fail, duplicate dbs:" + duplicates);
        }

        for (DbApplierDto dbApplierDto : list) {
            List<String> ips = dbApplierDto.getIps();
            if (CollectionUtils.isEmpty(ips)) {
                continue;
            }
            Set<String> duplicateIps = ips.stream().filter(i -> Collections.frequency(ips, i) > 1).collect(Collectors.toSet());
            if (!CollectionUtils.isEmpty(duplicateIps)) {
                throw ConsoleExceptionUtils.message("check fail, duplicate ips:" + duplicates + " for db: " + dbApplierDto.getDbName());
            }
        }
    }

    public Pair<List<String>, List<String>> getAddAndDeleteResourceIps(List<String> insertIps, List<String> existIps) {
        if (CollectionUtils.isEmpty(existIps) || CollectionUtils.isEmpty(insertIps)) {
            return Pair.of(insertIps, existIps);
        }
        List<String> addIps = new ArrayList<>();
        List<String> deleteIps = new ArrayList<>();
        for (String ip : insertIps) {
            if (!existIps.contains(ip)) {
                addIps.add(ip);
            }
        }

        for (String ip : existIps) {
            if (!insertIps.contains(ip)) {
                deleteIps.add(ip);
            }
        }

        return Pair.of(addIps, deleteIps);
    }


    @Override
    public MqMetaCreateResultView autoCreateMq(MqAutoCreateRequestDto createDto) throws Exception {
        autoConfigLogger.info("[[tag=autoconfig]] start autoCreateMq: {}", createDto.toString());
        Transaction transaction = Cat.newTransaction("DRC.autocreate.mq", createDto.getDbName());
        transaction.addProperty("createDto", createDto.toString());
        try {
            createDto.check();
            checkKafkaTopic(createDto);

            MqAutoCreateDto dto = createDto.deriveMqAutoCreateDto();
            checkRegionAndCreateMhaDbReplicationForMq(dto);
            MqMetaCreateResultView view = createMqConfigAndSwitchMessenger(dto);
            autoConfigLogger.info("[[tag=autoconfig]] autoCreateMq success: {}", view);
            return view;
        } catch (Exception e) {
            autoConfigLogger.error("[[tag=autoconfig]] autoCreateMq error", e);
            String duplicateMessage = e.getMessage();
            if (duplicateMessage.startsWith(AutoBuildErrorEnum.DUPLICATE_MQ_CONFIGURATION.getMessage())) {
                try {
                    String duplicateTable = duplicateMessage.split("\\|")[1].split(":")[0].split("\\.")[1];
                    String duplicateTopic = duplicateMessage.split("\\|")[1].split(":")[1];
                    if (createDto.getTable().equals(duplicateTable) && createDto.getTopic().equals(duplicateTopic)) {
                        MqMetaCreateResultView view = createDto.deriveMqAutoCreateDto().deriveMqMetaCreateResultView();
                        view.setContainTables(0);
                        autoConfigLogger.info("[[tag=autoconfig]] autoCreateMq success: {}", view);
                        return view;
                    }
                } catch (Exception e1) {
                    autoConfigLogger.error("[[tag=autoconfig]] extract message error", e);
                }
            }

            transaction.setStatus(e);
            throw e;
        } finally {
            transaction.complete();
        }
    }

    /**
     * should not have @DalTransactional here
     */
    @VisibleForTesting
    protected void checkRegionAndCreateMhaDbReplicationForMq(MqAutoCreateDto dto) throws Exception {
        MqType mqType = MqType.valueOf(dto.getMqConfig().getMqType());
        DrcAutoBuildReq queryRegionDto = dto.deriveDrcAutoBuildReq(DrcAutoBuildReq.BuildMode.DAL_CLUSTER_NAME);
        List<String> availableRegions = drcAutoBuildService.getRegionOptions(queryRegionDto);
        if (!availableRegions.contains(dto.getSrcRegionName())) {
            throw ConsoleExceptionUtils.message(String.format("wrong region (%s only in %s)", dto.getDbName(), availableRegions));
        }
        String correctDbName = queryRegionDto.getDalClusterName().replaceFirst("_dalcluster$", "");
        if (!correctDbName.equals(dto.getDbName())) {
            throw ConsoleExceptionUtils.message(String.format("need to be shard db like: %s", correctDbName));
        }
        dto.setDalclusterName(queryRegionDto.getDalClusterName());

        List<DbMqConfigInfoDto> dbMqConfigInfoDtos = this.getExistDbMqConfigDcOption(dto.getDbName(), mqType);
        Set<String> existRegion = dbMqConfigInfoDtos.stream().map(DbMqConfigInfoDto::getSrcRegionName).collect(Collectors.toSet());
        if (!existRegion.contains(dto.getSrcRegionName())) {
            MhaDbReplicationCreateDto createDto = dto.deriveMhaDbReplicationCreateDto();
            this.createMhaDbReplicationForMq(createDto);
        }
    }

    @VisibleForTesting
    @DalTransactional(logicDbName = "fxdrcmetadb_w", exceptionWrappedByDalException = false)
    protected MqMetaCreateResultView createMqConfigAndSwitchMessenger(MqAutoCreateDto dto) throws Exception {
        MqType mqType = MqType.valueOf(dto.getMqConfig().getMqType());

        DbMqConfigInfoDto dbMqConfigInfoDto = this.getDbMqConfig(dto.getDbName(), dto.getSrcRegionName(), mqType);
        dto.setDbNames(dbMqConfigInfoDto.getDbNames());
        MqMetaCreateResultView view = dto.deriveMqMetaCreateResultView();

        try {
            DrcAutoBuildReq queryTableDto = dto.deriveDrcAutoBuildReq(DrcAutoBuildReq.BuildMode.MULTI_DB_NAME);
            List<TableCheckVo> tables = drcAutoBuildService.preCheckMysqlTables(queryTableDto);
            view.setContainTables(tables.size());
        } catch (ConsoleException e) {
            throw ConsoleExceptionUtils.message("no tables found in db or other error in finding tables");
        }

        dto.setNotPermitSameTableMqConfig(true);

        List<String> mhaNames = dbMqConfigInfoDto.getMhaMqDtos().stream().map(MhaMqDto::getSrcMha).map(MhaDto::getName).toList();

        try {
            dLockService.tryLocks(mhaNames, DlockEnum.AUTOCONFIG);
            Thread.sleep(5000);
            this.createDbMqReplication(dto);
            List<MessengerSwitchReqDto> switchReqDtos = dbMqConfigInfoDto.getMhaMqDtos().stream()
                    .map(mhaMqDto -> {
                        MessengerSwitchReqDto switchReqDto = new MessengerSwitchReqDto();
                        switchReqDto.setSrcMhaName(mhaMqDto.getSrcMha().getName());
                        switchReqDto.setMqType(mqType.name());
                        return switchReqDto;
                    }).toList();
            switchMessengers(switchReqDtos);
            // push to cm
            List<String> relatedMhaName = dbMqConfigInfoDto.getMhaMqDtos().stream()
                    .map(MhaMqDto::getSrcMha).map(MhaDto::getName).toList();
            notifyCmService.pushConfigToCM(relatedMhaName, DlockEnum.AUTOCONFIG, HttpRequestEnum.PUT);

        } catch (Exception e) {
            logger.error("autoCreateMq failed", e);
            throw e;
        } finally {
            dLockService.unlockLocks(mhaNames, DlockEnum.AUTOCONFIG);
        }

        return view;
    }

    @VisibleForTesting
    protected void checkKafkaTopic(MqAutoCreateRequestDto dto) {
        if (MqType.kafka == MqType.valueOf(dto.getMqType())) {
            String accessToken = domainConfig.getOpsAccessToken();
            String ckafkaRegion = consoleConfig.getDrcCkafkaRegionMapping().get(dto.getRegion());
            if (StringUtils.isEmpty(ckafkaRegion)) {
                throw ConsoleExceptionUtils.message("empty ckafkaRegion");
            }
            String dbOwner = dbaApiService.getDbOwner(dto.getDbName());
            if (StringUtils.isEmpty(dbOwner)) {
                throw ConsoleExceptionUtils.message("error in check db owner");
            }
            try {
                boolean checkResult = kafkaApiService.prepareTopic(accessToken, new KafkaTopicCreateVo(dto.getTopic(), dto.getKafkaCluster(),
                        dbOwner, dto.getBu(), dto.getPartitions(), dto.getMaxMessageMB(), ckafkaRegion));
                if (!checkResult) {
                    throw ConsoleExceptionUtils.message("kafka topic check fail");
                }
            } catch (Exception e) {
                throw ConsoleExceptionUtils.message("error in check kafka topic");
            }
        }
    }

}
