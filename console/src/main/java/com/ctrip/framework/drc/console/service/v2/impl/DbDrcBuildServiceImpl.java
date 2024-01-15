package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.ConsoleConfig;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.ApplierTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerGroupTblV3;
import com.ctrip.framework.drc.console.dao.entity.v3.MessengerTblV3;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dao.v3.ApplierGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.ApplierTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerGroupTblV3Dao;
import com.ctrip.framework.drc.console.dao.v3.MessengerTblV3Dao;
import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.DrcBuildBaseParam;
import com.ctrip.framework.drc.console.param.v2.DrcBuildParam;
import com.ctrip.framework.drc.console.service.v2.DbDrcBuildService;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MultiKey;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.utils.StreamUtils.getKey;

@Service
public class DbDrcBuildServiceImpl implements DbDrcBuildService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaInfoServiceV2 metaInfoService;
    @Autowired
    private MhaTblV2Dao mhaTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
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
    private ApplierGroupTblV2Dao applierGroupTblV2Dao;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;

    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "drcMetaRefreshV2");

    @Override
    public List<DbApplierDto> getMhaDbAppliers(String srcMhaName, String dstMhaName) {
        try {
            // mha pair -> mha db replication
            List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha(srcMhaName, dstMhaName, null);

            // appliers
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
            return replicationDtos.stream().map(replicationTbl -> {
                ApplierGroupTblV3 applierGroupTblV3 = applierGroupTblV3Map.get(replicationTbl.getId());
                if (replicationTbl.getId() == null || applierGroupTblV3 == null) {
                    return new DbApplierDto(null, null, replicationTbl.getDst().getDbName(), null);
                }
                List<ApplierTblV3> appliers = applierMap.getOrDefault(applierGroupTblV3.getId(), Collections.emptyList());
                List<String> ips = appliers.stream().map(e -> resouceMap.get(e.getResourceId())).collect(Collectors.toList());
                if (CollectionUtils.isEmpty(ips) && CollectionUtils.isEmpty(replicationTbl.getLogicTable())) {
                    // skip
                    return null;
                }
                return new DbApplierDto(ips, applierGroupTblV3.getGtidInit(), replicationTbl.getDst().getDbName(), applierGroupTblV3.getConcurrency());
            }).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<DbApplierDto> getMhaDbMessengers(String mhaName) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblDao.queryByMhaName(mhaName, 0);
        if (mhaTblV2 == null) {
            return Collections.emptyList();
        }
        // mha -> mq replications
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryMqByMha(mhaName, null);
        List<Long> ids = replicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());

        // mq replications -> messenger
        List<MessengerGroupTblV3> messengerGroupTblV3s = messengerGroupTblV3Dao.queryByMhaDbReplicationIds(ids);
        Map<Long, MessengerGroupTblV3> groupMap = messengerGroupTblV3s.stream().collect(Collectors.toMap(MessengerGroupTblV3::getMhaDbReplicationId, e -> e));

        List<MessengerTblV3> messengerTblV3s = messengerTblV3Dao.queryByGroupIds(messengerGroupTblV3s.stream().map(MessengerGroupTblV3::getId).collect(Collectors.toList()));
        Map<Long, List<MessengerTblV3>> messengersByGroupMap = messengerTblV3s.stream().collect(Collectors.groupingBy(MessengerTblV3::getMessengerGroupId));

        List<Long> resourceIds = messengerTblV3s.stream().map(MessengerTblV3::getResourceId).collect(Collectors.toList());
        Map<Long, String> resouceMap = resourceTblDao.queryByIds(resourceIds).stream()
                .collect(Collectors.toMap(ResourceTbl::getId, ResourceTbl::getIp));


        // build data
        return replicationDtos.stream().map(replicationTbl -> {
            MessengerGroupTblV3 messengerGroupTblV3 = groupMap.get(replicationTbl.getId());
            if (replicationTbl.getId() == null || messengerGroupTblV3 == null) {
                return new DbApplierDto(null, null, replicationTbl.getSrc().getDbName());
            } else {
                List<MessengerTblV3> messengers = messengersByGroupMap.getOrDefault(messengerGroupTblV3.getId(), Collections.emptyList());
                List<String> ips = messengers.stream().map(e -> resouceMap.get(e.getResourceId())).collect(Collectors.toList());
                return new DbApplierDto(ips, messengerGroupTblV3.getGtidExecuted(), replicationTbl.getSrc().getDbName());
            }
        }).collect(Collectors.toList());
    }

    @Override
    public boolean isDbApplierConfigurable(String mhaName) {
        return consoleConfig.getDbApplierConfigureSwitch(mhaName);
    }


    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public String buildDbApplier(DrcBuildParam param) throws Exception {
        DrcBuildBaseParam srcBuildParam = param.getSrcBuildParam();
        DrcBuildBaseParam dstBuildParam = param.getDstBuildParam();
        String srcMhaName = srcBuildParam.getMhaName();
        String dstMhaName = dstBuildParam.getMhaName();
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());
        if (srcMha == null || dstMha == null) {
            throw ConsoleExceptionUtils.message("srcMha or dstMha not exist");
        }
        List<DbApplierDto> srcDbAppliers = srcBuildParam.getDbApplierDtos();
        List<DbApplierDto> dstDbAppliers = dstBuildParam.getDbApplierDtos();

        List<MhaDbMappingTbl> srcMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(srcMha.getId());
        List<MhaDbMappingTbl> dstMhaDbMappings = mhaDbMappingTblDao.queryByMhaId(dstMha.getId());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByType(ModuleEnum.APPLIER.getCode());

        // config db applier
        if (!CollectionUtils.isEmpty(dstDbAppliers)) {
            // src -> dst
            if (!this.isDbApplierConfigurable(dstMhaName)) {
                throw new ConsoleException("mha " + dstMhaName + " is not allowed to configure db appliers");
            }
            this.checkDbAppliers(dstDbAppliers);
            List<DbReplicationTbl> srcToDstDbReplications = this.getExistDbReplications(srcMhaDbMappings, dstMhaDbMappings);
            List<MhaDbReplicationDto> srcToDstMhaDbReplicationDtos = mhaDbReplicationService.queryByMha(srcMhaName, dstMhaName, dstDbAppliers.stream().map(DbApplierDto::getDbName).collect(Collectors.toList()));
            this.configureDbAppliers(srcToDstMhaDbReplicationDtos, dstDbAppliers, resourceTbls, srcToDstDbReplications);
        }

        if (!StringUtils.isBlank(dstBuildParam.getApplierInitGtid())) {
            ApplierGroupTblV2 applierGroupTblV2 = this.getApplierGroupTblV2(srcMha, dstMha);
            applierGroupTblV2.setGtidInit(dstBuildParam.getApplierInitGtid());
            applierGroupTblV2Dao.update(applierGroupTblV2);
        }

        // refresh
        Drc drc = metaInfoService.getDrcReplicationConfig(param.getSrcBuildParam().getMhaName(), param.getDstBuildParam().getMhaName());
        String drcString = XmlUtils.formatXML(drc.toString());
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProvider scheduledTask error", e);
        }
        return drcString;
    }

    @Override
    public String getDbDrcExecutedGtidTruncate(String srcMhaName, String dstMhaName) {
        try {
            Map<String, GtidSet> dbGtidMap = this.getDbDrcExecutedGtid(srcMhaName, dstMhaName);
            return GtidSet.getIntersection(Lists.newArrayList(dbGtidMap.values())).getGtidFirstInterval().toString();
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DAO_TBL_EXCEPTION);
        }
    }

    @Override
    public String getMhaDrcExecutedGtidTruncate(String srcMhaName, String dstMhaName) {
        try {
            GtidSet gtidSet = this.getMhaDrcExecutedGtid(srcMhaName, dstMhaName);
            return gtidSet.getGtidFirstInterval().toString();
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DAO_TBL_EXCEPTION);
        }
    }


    @Override
    public GtidSet getMhaDrcExecutedGtid(String srcMhaName, String dstMhaName) throws SQLException {
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());

        ApplierGroupTblV2 applierGroupTblV2 = this.getApplierGroupTblV2(srcMha, dstMha);

        String appliedGtid = mysqlServiceV2.getMhaAppliedGtid(dstMha.getMhaName());
        if (appliedGtid == null) {
            throw ConsoleExceptionUtils.message("query mha applied gtid fail");
        }
        GtidSet mhaAppliedGtid = new GtidSet(appliedGtid);
        return mhaAppliedGtid.union(new GtidSet(applierGroupTblV2.getGtidInit()));
    }

    @Override
    public Map<String, GtidSet> getDbDrcExecutedGtid(String srcMhaName, String dstMhaName) throws SQLException {
        MhaTblV2 srcMha = mhaTblDao.queryByMhaName(srcMhaName, BooleanEnum.FALSE.getCode());
        MhaTblV2 dstMha = mhaTblDao.queryByMhaName(dstMhaName, BooleanEnum.FALSE.getCode());

        // db applier gtid init
        Map<String, String> mhaDbAppliedGtid = mysqlServiceV2.getMhaDbAppliedGtid(dstMha.getMhaName());
        if (mhaDbAppliedGtid == null) {
            throw ConsoleExceptionUtils.message("query db applied gtid fail");
        }

        // db applier gtid apply
        List<DbApplierDto> mhaDbAppliers = this.getMhaDbAppliers(srcMha.getMhaName(), dstMha.getMhaName());
        Map<String, GtidSet> gtidInitMap = mhaDbAppliers.stream()
                .filter(e -> !StringUtils.isEmpty(e.getGtidInit()))
                .collect(Collectors.toMap(DbApplierDto::getDbName, e -> new GtidSet(e.getGtidInit())));

        // union all for each db
        Set<String> dbNames = Sets.newHashSet();
        dbNames.addAll(gtidInitMap.keySet());
        Map<String, GtidSet> map = Maps.newHashMap();
        for (String dbName : dbNames) {
            GtidSet appliedGtid = new GtidSet(mhaDbAppliedGtid.get(dbName));
            GtidSet initGtid = gtidInitMap.get(dbName);
            map.put(dbName, appliedGtid.union(initGtid));
        }
        return map;
    }

    private void configureDbAppliers(List<MhaDbReplicationDto> mhaDbReplicationDtos, List<DbApplierDto> dbApplierDtos, List<ResourceTbl> resourceTbls, List<DbReplicationTbl> srcDbReplications) throws SQLException {
        // 0. check dbReplication exist;
        checkExistDbReplication(mhaDbReplicationDtos, dbApplierDtos, srcDbReplications);

        // 1. applier group
        List<ApplierGroupTblV3> applierGroupTblV3s = insertOrUpdateApplierGroups(mhaDbReplicationDtos, dbApplierDtos);

        // 2. prepare data
        List<ApplierTblV3> allExistAppliers = applierTblV3Dao.queryByApplierGroupIds(applierGroupTblV3s.stream().map(ApplierGroupTblV3::getId).collect(Collectors.toList()), 0);
        Map<Long, List<ApplierTblV3>> appliersByGroupIdMap = allExistAppliers.stream().collect(Collectors.groupingBy(ApplierTblV3::getApplierGroupId));
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

        if (!this.isDbApplierConfigurable(mhaName)) {
            throw new ConsoleException("mha " + mhaName + " is not allowed to configure db messengers");
        }
        this.checkDbAppliers(dbApplierDtos);
        List<String> dbNames = dbApplierDtos.stream().map(DbApplierDto::getDbName).collect(Collectors.toList());
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryMqByMha(mhaName, dbNames);
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(mhaTblV2.getId());
        List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryBySrcMappingIds(mhaDbMappingIds, ReplicationTypeEnum.DB_TO_MQ.getType());

        this.checkExistDbReplication(replicationDtos, dbApplierDtos, dbReplicationTbls);


        // upsert messenger group
        List<MessengerGroupTblV3> groupTblV3List = this.configureMessengerGroups(dbApplierDtos, replicationDtos);
        // configure messengers
        this.configureMessengers(dbApplierDtos, replicationDtos, groupTblV3List);


        // refresh
        Drc drc = metaInfoService.getDrcMessengerConfig(mhaName);
        try {
            executorService.submit(() -> metaProviderV2.scheduledTask());
        } catch (Exception e) {
            logger.error("metaProvider scheduledTask error", e);
        }
        return XmlUtils.formatXML(drc.toString());
    }

    private void checkExistDbReplication(List<MhaDbReplicationDto> mhaDbReplicationDtos, List<DbApplierDto> dbApplierDtos, List<DbReplicationTbl> dbReplicationTbls) {
        Map<MultiKey, DbReplicationTbl> table = dbReplicationTbls.stream().filter(StreamUtils.distinctByKey(StreamUtils::getKey)).collect(Collectors.toMap(StreamUtils::getKey, e -> e));
        Map<String, MhaDbReplicationDto> map = mhaDbReplicationDtos.stream().collect(Collectors.toMap(e -> e.getSrc().getDbName(), e -> e));
        List<String> notConfiguredDbNames = Lists.newArrayList();
        for (DbApplierDto applierDto : dbApplierDtos) {
            MhaDbReplicationDto dto = map.get(applierDto.getDbName());
            DbReplicationTbl dbReplicationTbl = table.get(getKey(dto));
            if (dbReplicationTbl == null && !CollectionUtils.isEmpty(applierDto.getIps())) {
                notConfiguredDbNames.add(applierDto.getDbName());
            }
        }
        if (!notConfiguredDbNames.isEmpty()) {
            throw ConsoleExceptionUtils.message("dbReplication not config yet, cannot config applier: " + String.join(",", notConfiguredDbNames));
        }
    }

    private List<MessengerGroupTblV3> configureMessengerGroups(List<DbApplierDto> dbApplierDtos, List<MhaDbReplicationDto> replicationDtos) throws SQLException {
        Map<String, DbApplierDto> applierMap = dbApplierDtos.stream().collect(Collectors.toMap(DbApplierDto::getDbName, e -> e));
        List<MessengerGroupTblV3> groupTblV3List = replicationDtos.stream().map(e -> {
            MessengerGroupTblV3 messengerGroupTblV3 = new MessengerGroupTblV3();
            messengerGroupTblV3.setMhaDbReplicationId(e.getId());
            messengerGroupTblV3.setGtidExecuted(applierMap.get(e.getSrc().getDbName()).getGtidInit());
            messengerGroupTblV3.setDeleted(0);
            return messengerGroupTblV3;
        }).collect(Collectors.toList());
        messengerGroupTblV3Dao.upsert(groupTblV3List);
        return groupTblV3List;
    }

    private ApplierGroupTblV2 getApplierGroupTblV2(MhaTblV2 srcMha, MhaTblV2 dstMha) throws SQLException {
        MhaReplicationTbl mhaReplicationTbl = mhaReplicationTblDao.queryByMhaId(srcMha.getId(), dstMha.getId());
        if (mhaReplicationTbl == null) {
            throw ConsoleExceptionUtils.message("mha replication not exist");
        }
        ApplierGroupTblV2 applierGroupTblV2 = applierGroupTblV2Dao.queryByMhaReplicationId(mhaReplicationTbl.getId(), BooleanEnum.FALSE.getCode());
        if (applierGroupTblV2 == null) {
            throw ConsoleExceptionUtils.message("mha applier group not exist");
        }
        return applierGroupTblV2;
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

    private List<DbReplicationTbl> getExistDbReplications(List<MhaDbMappingTbl> srcMhaDbMappings, List<MhaDbMappingTbl> dstMhaDbMappings) throws Exception {
        List<Long> srcMhaDbMappingIds = srcMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        List<Long> dstMhaDbMappingIds = dstMhaDbMappings.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> existDbReplications = dbReplicationTblDao.queryByMappingIds(srcMhaDbMappingIds, dstMhaDbMappingIds, ReplicationTypeEnum.DB_TO_DB.getType());
        return existDbReplications;
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
}
