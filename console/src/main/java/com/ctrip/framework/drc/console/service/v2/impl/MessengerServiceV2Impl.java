package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.*;
import com.ctrip.framework.drc.console.dao.v2.*;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigService;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.utils.StreamUtils;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigConflictTable;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.console.vo.response.QmqApiResponse;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.TransactionMonitor;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallMessengerDelayEntity;
import com.ctrip.framework.drc.core.service.utils.Constants;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.service.utils.Constants.DRC;
import static com.ctrip.framework.drc.core.service.utils.Constants.ESCAPE_CHARACTER_DOT_REGEX;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
@Service
public class MessengerServiceV2Impl implements MessengerServiceV2 {
    private static final Logger logger = LoggerFactory.getLogger(MessengerServiceV2Impl.class);
    private final TransactionMonitor transactionMonitor = DefaultTransactionMonitorHolder.getInstance();
    private DbClusterApiService dbClusterService = ApiContainer.getDbClusterApiServiceImpl();
    private final ExecutorService executorService = ThreadUtils.newFixedThreadPool(5, "mhaReplicationService");
    private OPSApiService opsApiServiceImpl = ApiContainer.getOPSApiServiceImpl();

    @Autowired
    private RowsFilterServiceV2 rowsFilterService;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DbTblDao dbTblDao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DefaultConsoleConfig defaultConsoleConfig;
    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private QConfigService qConfigService;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;

    @Override
    public List<MhaMessengerDto> getRelatedMhaMessenger(List<String> mhas, List<String> dbs) {
        try {
            // only consider mha with messenger group and
            List<MhaTblV2> mhaTblV2 = mhaTblV2Dao.queryByMhaNames(mhas, BooleanEnum.FALSE.getCode());
            List<DbTbl> dbTbls = dbTblDao.queryByDbNames(dbs);
            if (CollectionUtils.isEmpty(mhaTblV2) || CollectionUtils.isEmpty(dbTbls)) {
                return Collections.emptyList();
            }

            List<Long> mhaIds = mhaTblV2.stream().map(MhaTblV2::getId).collect(Collectors.toList());
            List<MessengerGroupTbl> messengerGroupTbls = messengerGroupTblDao.queryByMhaIds(mhaIds, BooleanEnum.FALSE.getCode());
            mhaIds = messengerGroupTbls.stream().map(MessengerGroupTbl::getMhaId).collect(Collectors.toList());

            List<Long> dbIds = dbTbls.stream().map(DbTbl::getId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByDbIdsAndMhaIds(dbIds, mhaIds);
            if (CollectionUtils.isEmpty(mhaDbMappingTbls)) {
                return Collections.emptyList();
            }


            // messenger dbReplications
            List<Long> mappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            List<DbReplicationTbl> relatedDbReplications = dbReplicationTblDao.queryByRelatedMappingIds(mappingIds, ReplicationTypeEnum.DB_TO_MQ.getType())
                    .stream().filter(StreamUtils.distinctByKey(DbReplicationTbl::getSrcMhaDbMappingId)).collect(Collectors.toList());

            Map<Long, MhaDbMappingTbl> mappingTblMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, Function.identity()));
            Map<Long, MhaTblV2> mhaMap = mhaTblV2.stream().collect(Collectors.toMap(MhaTblV2::getId, e -> e));
            Map<Long, MessengerGroupTbl> messengerGroupTblMap = messengerGroupTbls.stream().collect(Collectors.toMap(MessengerGroupTbl::getMhaId, e -> e));
            Map<Long, DbTbl> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, Function.identity()));
            Map<Long, MhaMessengerDto> map = Maps.newHashMap();
            for (DbReplicationTbl dbReplicationTbl : relatedDbReplications) {
                MhaDbMappingTbl srcMapping = mappingTblMap.get(dbReplicationTbl.getSrcMhaDbMappingId());
                Long mhaId = srcMapping.getMhaId();
                MhaMessengerDto dto = map.get(mhaId);
                if (dto == null) {
                    MhaTblV2 mhaTbl = mhaMap.get(mhaId);
                    MessengerGroupTbl messengerGroupTbl = messengerGroupTblMap.get(mhaId);
                    dto = MhaMessengerDto.from(mhaTbl, messengerGroupTbl);
                    map.put(mhaId, dto);
                }
                DbTbl dbTbl = dbMap.get(srcMapping.getDbId());
                if (dbTbl != null) {
                    dto.getDbs().add(dbTbl.getDbName());
                }
            }

            // set drc status: active if it has messenger
            List<MhaMessengerDto> mhaMessengerDtos = Lists.newArrayList(map.values());
            List<Long> groupIds = mhaMessengerDtos.stream().map(MhaMessengerDto::getMessengerGroupId).collect(Collectors.toList());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupIds(groupIds);
            Set<Long> activeGroupIdSet = messengerTbls.stream().map(MessengerTbl::getMessengerGroupId).collect(Collectors.toSet());
            mhaMessengerDtos.forEach(e -> e.setStatus(activeGroupIdSet.contains(e.getMessengerGroupId()) ? BooleanEnum.TRUE.getCode() : BooleanEnum.FALSE.getCode()));

            return mhaMessengerDtos;
        } catch (SQLException e) {
            logger.error("getRelatedMhaMessenger error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaDelayInfoDto> getMhaMessengerDelays(List<MhaMessengerDto> messengerDtoList) {
        if (CollectionUtils.isEmpty(messengerDtoList)) {
            return Collections.emptyList();
        }
        List<Callable<MhaDelayInfoDto>> list = Lists.newArrayList();
        List<String> mhaNames = messengerDtoList.stream().map(e -> e.getSrcMha().getName()).collect(Collectors.toList());

        try {
            String trafficFromHickWall = domainConfig.getTrafficFromHickWall();
            String opsAccessToken = domainConfig.getOpsAccessToken();
            if (EnvUtils.fat()) {
                trafficFromHickWall = domainConfig.getTrafficFromHickWallFat();
                opsAccessToken = domainConfig.getOpsAccessTokenFat();
            }
            List<HickWallMessengerDelayEntity> messengerDelayFromHickWall = opsApiServiceImpl.getMessengerDelayFromHickWall(trafficFromHickWall, opsAccessToken, mhaNames);
            Map<String, HickWallMessengerDelayEntity> hickWallMap = messengerDelayFromHickWall.stream().collect(Collectors.toMap(HickWallMessengerDelayEntity::getMha, e -> e, (e1, e2) -> e1));
            for (MhaMessengerDto dto : messengerDtoList) {
                list.add(() -> {
                    String mhaName = dto.getSrcMha().getName();
                    HickWallMessengerDelayEntity delayEntity = hickWallMap.get(mhaName);
                    return this.getMhaMessengerDelay(mhaName, delayEntity);
                });
            }

            List<MhaDelayInfoDto> res = Lists.newArrayList();
            List<Future<MhaDelayInfoDto>> futures = executorService.invokeAll(list, 5, TimeUnit.SECONDS);
            for (Future<MhaDelayInfoDto> future : futures) {
                res.add(future.get());
            }
            return res;
        } catch (InterruptedException | ExecutionException | IOException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_MHA_DELAY_FAIL, e);
        }
    }

    private MhaDelayInfoDto getMhaMessengerDelay(String mha, HickWallMessengerDelayEntity delayEntity) {
        MhaDelayInfoDto delayInfoDto = new MhaDelayInfoDto();
        delayInfoDto.setSrcMha(mha);

        // query receive time
        Long updateTime = mysqlServiceV2.getDelayUpdateTime(mha, mha);
        Long currentTime = mysqlServiceV2.getCurrentTime(mha);
        if (updateTime != null) {
            if (currentTime != null) {
                updateTime = Math.max(updateTime, currentTime);
            }
            delayInfoDto.setSrcTime(updateTime);
            if (delayEntity != null) {
                Long delay = delayEntity.getDelay();
                delayInfoDto.setDstTime(updateTime - delay);
            }
        }
        return delayInfoDto;
    }

    @Override
    public List<MhaTblV2> getAllMessengerMhaTbls() {
        try {
            MessengerGroupTbl sample = new MessengerGroupTbl();
            sample.setDeleted(BooleanEnum.FALSE.getCode());
            List<MessengerGroupTbl> messengers = messengerGroupTblDao.queryBy(sample);
            List<Long> mhaIdList = messengers.stream().map(MessengerGroupTbl::getMhaId).collect(Collectors.toList());
            Map<Long, MhaTblV2> mhaTblV2Map = mhaTblV2Dao.queryByIds(mhaIdList).stream().collect(Collectors.toMap(e -> e.getId(), e -> e));

            return messengers.stream()
                    .sorted(Comparator.comparing(MessengerGroupTbl::getDatachangeLasttime).reversed()) // order by .. desc
                    .map(e -> mhaTblV2Map.get(e.getMhaId())).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("queryAllBu exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void removeMessengerGroup(String mhaName) throws SQLException {
        MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not exist: " + mhaName);
        }
        List<MqConfigVo> currentMqConfig = this.queryMhaMessengerConfigs(mhaName);
        if (!CollectionUtils.isEmpty(currentMqConfig)) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.FORBIDDEN_OPERATION, "remove inner mq configs first!");
        }

        Long mhaId = mhaTbl.getId();
        MessengerGroupTbl mGroup = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (mGroup == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha messenger group not exist: " + mhaName);
        }
        List<MessengerTbl> messengers = messengerTblDao.queryByGroupId(mGroup.getId());

        // delete messenger & messenger group
        messengers.forEach(m -> m.setDeleted(BooleanEnum.TRUE.getCode()));
        logger.info("messengerTblDao.batchUpdate, group id: {}, messenger size: {}", mGroup.getId(), messengers.size());
        messengerTblDao.batchUpdate(messengers);

        mGroup.setDeleted(BooleanEnum.TRUE.getCode());
        logger.info("messengerGroupTblDao.update, group id: {}", mGroup.getId());
        messengerGroupTblDao.update(mGroup);
    }

    @Override
    public List<MqConfigVo> queryMhaMessengerConfigs(String mhaName) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
            if (mhaTblV2 == null) {
                return Collections.emptyList();
            }
            // prepare data
            // 1.1 MhaDbMappingTbl
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(mhaTblV2.getId());
            List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

            // 1.2 DbReplicationTbl
            List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryBySrcMappingIds(mhaDbMappingIds, ReplicationTypeEnum.DB_TO_MQ.getType());

            // 1.3 DbReplicationFilterMappingTbl
            List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(dbReplicationIds)) {
                return Collections.emptyList();
            }
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingList = dbReplicationFilterMappingTblDao.queryMessengerDbReplicationByIds(dbReplicationIds);
            // db_replication_tbl: db_replication_filter_mapping_tbl = 1:1
            Map<Long, DbReplicationFilterMappingTbl> map = dbReplicationFilterMappingList.stream().collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, e -> e, (e1, e2) -> e1));

            // 1.4 Db
            List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
            List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
            Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));


            // 1.5 MessengerFilterTbl
            List<Long> messengerFilerIds = map.values().stream().map(DbReplicationFilterMappingTbl::getMessengerFilterId).collect(Collectors.toList());
            List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryByIds(messengerFilerIds);
            Map<Long, MessengerFilterTbl> messengerFilterTblMap = messengerFilterTbls.stream().collect(Collectors.toMap(MessengerFilterTbl::getId, e -> e));

            // build
            List<MqConfigVo> list = Lists.newArrayList();
            for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
                DbReplicationFilterMappingTbl dbReplicationFilterMappings = map.get(dbReplicationTbl.getId());
                if (dbReplicationFilterMappings == null) {
                    continue;
                }
                MessengerFilterTbl messengerFilterTbl = messengerFilterTblMap.get(dbReplicationFilterMappings.getMessengerFilterId());
                if (messengerFilterTbl == null) {
                    logger.warn("messengerFilterTbl not found, dbReplicationTbl: {}", dbReplicationTbl);
                    continue;
                }

                long dbId = mhaDbMappingMap.getOrDefault(dbReplicationTbl.getSrcMhaDbMappingId(), 0L);
                String dbName = dbTblMap.getOrDefault(dbId, "dbNotFound!");
                String tableName = dbName + "\\." + dbReplicationTbl.getSrcLogicTableName();

                //build
                MqConfig mqConfig = JsonUtils.fromJson(messengerFilterTbl.getProperties(), MqConfig.class);
                MqConfigVo mqConfigVo = new MqConfigVo();
                mqConfigVo.setDbReplicationId(dbReplicationTbl.getId());
                mqConfigVo.setTopic(dbReplicationTbl.getDstLogicTableName());
                mqConfigVo.setTable(tableName);
                mqConfigVo.setDatachangeLasttime(dbReplicationTbl.getDatachangeLasttime().getTime());

                mqConfigVo.setMqType(mqConfig.getMqType());
                mqConfigVo.setSerialization(mqConfig.getSerialization());
                mqConfigVo.setOrderKey(mqConfig.getOrderKey());
                mqConfigVo.setOrder(mqConfig.isOrder());
                mqConfigVo.setPersistent(mqConfig.isPersistent());
                mqConfigVo.setDelayTime(mqConfig.getDelayTime());
                list.add(mqConfigVo);
            }
            return list;
        } catch (SQLException e) {
            logger.error("queryMhaMessengerConfigs exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<String> getBusFromQmq() throws Exception {
        List<QmqBuEntity> buEntities = getBuEntitiesFromQmq();
        return buEntities.stream().map(buEntity -> buEntity.getEnName().toLowerCase()).collect(Collectors.toList());
    }

    private List<QmqBuEntity> getBuEntitiesFromQmq() throws Exception {
        String qmqBuListUrl = domainConfig.getQmqBuListUrl();
        QmqBuList response = HttpUtils.post(qmqBuListUrl, null, QmqBuList.class);
        return response.getData();
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteDbReplicationForMq(String mhaName, List<Long> dbReplicationIds) {
        List<DbReplicationTbl> dbReplicationTbls;
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls;
        try {
            if (defaultConsoleConfig.getVpcMhaNames().contains(mhaName)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION, "Vpc mha not supported");
            }
            // check input: dbReplicationIds
            if (CollectionUtils.isEmpty(dbReplicationIds)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "dbReplicationIds is empty");
            }
            dbReplicationTbls = dbReplicationTblDao.queryByIds(dbReplicationIds);
            if (CollectionUtils.isEmpty(dbReplicationTbls)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "invalid dbReplicationIds");
            }
            boolean dbReplicationNotMatch = dbReplicationTbls.stream().anyMatch(dbReplicationTbl -> !ReplicationTypeEnum.DB_TO_MQ.getType().equals(dbReplicationTbl.getReplicationType()));
            if (dbReplicationNotMatch) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION, "db replication type should be DB_TO_MQ!");
            }

            // check input: mhaNames
            if (StringUtils.isBlank(mhaName)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mhaName is blank");
            }
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
            if (mhaTblV2 == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "mhaName not found");
            }

            // check mha && dbReplication match
            List<Long> mhaDbMappingIds = dbReplicationTbls.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(mhaDbMappingIds);
            boolean mhaIdNotMatch = mhaDbMappingTbls.stream().anyMatch(mhaDbMappingTbl -> !mhaTblV2.getId().equals(mhaDbMappingTbl.getMhaId()));
            if (mhaIdNotMatch) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION, String.format("some dbReplication %s not belong to this mha: %s", dbReplicationIds, mhaName));
            }

            dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }

        try {
            // do delete: dbReplicationTbls
            dbReplicationTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("deleteMessengerDbReplications, size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);
            dbReplicationTblDao.batchUpdate(dbReplicationTbls);

            // do delete: dbReplicationFilterMappingTbl
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            logger.info("update dbReplicationFilterMappingTbls set deleted true, size: {}, dbReplicationFilterMappingTbls: {}", dbReplicationFilterMappingTbls.size(), dbReplicationFilterMappingTbls);
            dbReplicationFilterMappingTblDao.batchUpdate(dbReplicationFilterMappingTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<Messenger> generateMessengers(Long mhaId) throws SQLException {
        List<Messenger> messengers = Lists.newArrayList();
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (null == messengerGroupTbl) {
            return messengers;
        }

        MessengerProperties messengerProperties = getMessengerProperties(mhaId);
        String propertiesJson = JsonUtils.toJson(messengerProperties);
        if (CollectionUtils.isEmpty(messengerProperties.getMqConfigs())) {
            logger.info("no mqConfig, should not generate messenger");
            return messengers;
        }

        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        for (MessengerTbl messengerTbl : messengerTbls) {
            Messenger messenger = new Messenger();
            ResourceTbl resourceTbl = resourceTblDao.queryByPk(messengerTbl.getResourceId());
            messenger.setIp(resourceTbl.getIp());
            messenger.setPort(messengerTbl.getPort());
            messenger.setNameFilter(messengerProperties.getNameFilter());
            messenger.setGtidExecuted(messengerGroupTbl.getGtidExecuted());
            messenger.setProperties(propertiesJson);
            messengers.add(messenger);
        }
        return messengers;
    }

    @Override
    public MqConfigCheckVo checkMqConfig(MqConfigDto dto) {
        String mhaName = dto.getMhaName();
        String nameFilter = dto.getTable();

        // query requested requestedTables
        List<MySqlUtils.TableSchemaName> requestedTables = mysqlServiceV2.getMatchTable(mhaName, nameFilter);

        // query exist messenger replication
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
            if (mhaTblV2 == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_RESULT_EMPTY, "mha not found: " + mhaName);
            }
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(mhaTblV2.getId());
            List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());
            Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

            List<DbReplicationTbl> messengerDbReplications = dbReplicationTblDao.queryBySrcMappingIds(mhaDbMappingIds, ReplicationTypeEnum.DB_TO_MQ.getType());

            List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
            List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
            Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));


            // if update request, remove itself before check conflicts
            boolean updateRequest = dto.getDbReplicationId() != null;
            if (updateRequest) {
                boolean anyRemove = messengerDbReplications.removeIf(e -> e.getId().equals(dto.getDbReplicationId()));
                if (!anyRemove) {
                    throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE, String.format("update replicationId %d that not belong to mha :%s", dto.getDbReplicationId(), mhaName));
                }
            }

            // one nameFilter only send to one topic
            List<MqConfigConflictTable> conflictTables = Lists.newArrayList();
            for (DbReplicationTbl replicationTbl : messengerDbReplications) {
                Long dbId = mhaDbMappingMap.get(replicationTbl.getSrcMhaDbMappingId());
                boolean dbNotExist = dbId == null || !dbTblMap.containsKey(dbId);
                if (dbNotExist) {
                    continue;
                }
                String dbName = dbTblMap.get(dbId);
                AviatorRegexFilter filter = new AviatorRegexFilter(dbName + "\\." + replicationTbl.getSrcLogicTableName());
                for (MySqlUtils.TableSchemaName requestTable : requestedTables) {
                    String directSchemaTableName = requestTable.getDirectSchemaTableName();
                    boolean sameTable = filter.filter(directSchemaTableName);
                    if (sameTable) {
                        MqConfigConflictTable vo = new MqConfigConflictTable();
                        vo.setTable(directSchemaTableName);
                        vo.setTopic(replicationTbl.getDstLogicTableName());
                        conflictTables.add(vo);
                    }
                }
            }
            return MqConfigCheckVo.from(conflictTables);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }

    }

    private MessengerProperties getMessengerProperties(Long mhaId) throws SQLException {
        MessengerProperties messengerProperties = new MessengerProperties();
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(mhaId);
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<Long> mhaDbMappingIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getId).collect(Collectors.toList());

        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryBySrcMappingIds(mhaDbMappingIds, ReplicationTypeEnum.DB_TO_MQ.getType());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        Set<String> srcTables = new HashSet<>();
        List<MqConfig> mqConfigs = new ArrayList<>();
        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        List<RowsFilterConfig> rowFilters = new ArrayList<>();

        for (DbReplicationTbl dbReplicationTbl : dbReplicationTbls) {
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationFilterMappingTblDao.queryByDbReplicationId(dbReplicationTbl.getId());
            List<Long> messengerFilterIds = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getMessengerFilterId)
                    .filter(e -> e != null && e > 0L).collect(Collectors.toList());

            MessengerFilterTbl messengerFilterTbl = messengerFilterTblDao.queryById(messengerFilterIds.get(0));
            if (null == messengerFilterTbl) {
                logger.warn("Messenger Filter is Null, dbReplicationTbl: {}", dbReplicationTbl);
                continue;
            }
            MqConfig mqConfig = JsonUtils.fromJson(messengerFilterTbl.getProperties(), MqConfig.class);

            long dbId = mhaDbMappingMap.getOrDefault(dbReplicationTbl.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationTbl.getSrcLogicTableName();
            srcTables.add(tableName);
            mqConfig.setTable(tableName);
            mqConfig.setTopic(dbReplicationTbl.getDstLogicTableName());
            // processor is null
            mqConfigs.add(mqConfig);

            List<Long> rowsFilterIds = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getRowsFilterId)
                    .filter(e -> e != null && e > 0L).collect(Collectors.toList());
            rowFilters.addAll(rowsFilterService.generateRowsFiltersConfig(tableName, rowsFilterIds));
        }

        messengerProperties.setMqConfigs(mqConfigs);
        messengerProperties.setNameFilter(Joiner.on(",").join(srcTables));
        dataMediaConfig.setRowsFilters(rowFilters);
        messengerProperties.setDataMediaConfig(dataMediaConfig);
        return messengerProperties;
    }

    @Override
    public String getMessengerGtidExecuted(String mhaName) {
        try {
            if (StringUtils.isBlank(mhaName)) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "input is blank");
            }
            MhaTblV2 mhaTbl = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
            if (mhaTbl == null) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not exist:" + mhaName);
            }
            MessengerGroupTbl mGroup = messengerGroupTblDao.queryByMhaId(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
            return mGroup.getGtidExecuted();
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public void processAddMqConfig(MqConfigDto dto) throws Exception {
        MhaTblV2 mhaTblV2 = this.getAndCheckMessengerMha(dto.getMhaName());
        this.initMqConfig(dto, mhaTblV2);
        this.addDalClusterMqConfig(dto, mhaTblV2);
        this.addMqConfig(dto, mhaTblV2);
    }

    @Override
    public void processUpdateMqConfig(MqConfigDto dto) throws Exception {
        MhaTblV2 mhaTblV2 = this.getAndCheckMessengerMha(dto.getMhaName());
        this.initMqConfig(dto, mhaTblV2);
        this.updateDalClusterMqConfig(dto, mhaTblV2);
        this.updateMqConfig(dto, mhaTblV2);
    }

    private MhaTblV2 getAndCheckMessengerMha(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, 0);
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not exist: " + mhaName);
        }
        List<String> vpcMhaNames = defaultConsoleConfig.getVpcMhaNames();
        if (vpcMhaNames.contains(mhaTblV2.getMhaName())) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "vpc mha not supported!");
        }
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaTblV2.getId(), 0);
        if (messengerGroupTbl == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID,
                    "messengerGroupTbl not exist for mha: " + mhaTblV2.getMhaName());
        }
        return mhaTblV2;
    }


    @Override
    public void processDeleteMqConfig(MqConfigDeleteRequestDto dto) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(dto.getMhaName(), 0);
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not exist: " + dto.getMhaName());
        }
        if (defaultConsoleConfig.getVpcMhaNames().contains(mhaTblV2.getMhaName())) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "vpc mha not supported!");
        }

        this.disableDalClusterMqConfigIfNecessary(dto.getDbReplicationIdList().get(0), mhaTblV2);
        this.deleteDbReplicationForMq(dto.getMhaName(), dto.getDbReplicationIdList());
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void updateMqConfig(MqConfigDto dto, MhaTblV2 mhaTblV2) throws Exception {
        // delete old
        this.deleteDbReplicationForMq(mhaTblV2.getMhaName(), Lists.newArrayList(dto.getDbReplicationId()));
        // insert new
        this.addMqConfig(dto, mhaTblV2);
    }


    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void addMqConfig(MqConfigDto dto, MhaTblV2 mhaTblV2) throws Exception {
        insertMhaDbMappings(mhaTblV2, dto);
        insertDbReplicationAndFilterMapping(mhaTblV2, dto);
    }

    private void insertMhaDbMappings(MhaTblV2 mhaTblV2, MqConfigDto dto) throws Exception {
        List<String> dbList = queryDbs(mhaTblV2.getMhaName(), dto.getTable());
        insertDbs(dbList);
        insertMhaDbMappings(mhaTblV2.getId(), dbList);
    }

    private List<String> queryDbs(String mhaName, String nameFilter) {
        List<String> tableList = mysqlServiceV2.queryTablesWithNameFilter(mhaName, nameFilter);
        List<String> dbList = new ArrayList<>();
        if (CollectionUtils.isEmpty(tableList)) {
            logger.info("mha: {} query db empty, nameFilter: {}", mhaName, nameFilter);
            return dbList;
        }
        for (String table : tableList) {
            String[] tables = table.split("\\.");
            dbList.add(tables[0]);
        }
        return dbList.stream().distinct().collect(Collectors.toList());
    }

    private void insertDbs(List<String> dbList) throws Exception {
        if (CollectionUtils.isEmpty(dbList)) {
            logger.warn("dbList is empty");
            return;
        }
        List<String> existDbList = dbTblDao.queryByDbNames(dbList).stream().map(DbTbl::getDbName).collect(Collectors.toList());
        List<String> insertDbList = dbList.stream().filter(e -> !existDbList.contains(e)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(insertDbList)) {
            logger.info("dbList has already exist: {}", dbList);
            return;
        }
        List<DbTbl> insertTbls = insertDbList.stream().map(db -> {
            DbTbl dbTbl = new DbTbl();
            dbTbl.setDbName(db);
            dbTbl.setIsDrc(0);
            dbTbl.setBuName(DRC);
            dbTbl.setBuCode(DRC);
            dbTbl.setDbOwner(DRC);
            dbTbl.setDeleted(BooleanEnum.FALSE.getCode());
            return dbTbl;
        }).collect(Collectors.toList());

        logger.info("insert dbList: {}", insertDbList);
        dbTblDao.batchInsert(insertTbls);
    }

    private void insertMhaDbMappings(long mhaId, List<String> dbList) throws Exception {
        if (CollectionUtils.isEmpty(dbList)) {
            logger.warn("dbList is empty, mhaId: {}", mhaId);
            return;
        }
        List<DbTbl> dbTblList = dbTblDao.queryByDbNames(dbList);
        Map<String, Long> dbMap = dbTblList.stream().collect(Collectors.toMap(DbTbl::getDbName, DbTbl::getId));
        List<Long> existDbIds = mhaDbMappingTblDao.queryByMhaId(mhaId).stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());

        List<MhaDbMappingTbl> insertDbMappingTbls = new ArrayList<>();
        for (String dbName : dbList) {
            long dbId = dbMap.get(dbName);
            if (existDbIds.contains(dbId)) {
                continue;
            }
            MhaDbMappingTbl mhaDbMappingTbl = new MhaDbMappingTbl();
            mhaDbMappingTbl.setMhaId(mhaId);
            mhaDbMappingTbl.setDbId(dbId);
            mhaDbMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            insertDbMappingTbls.add(mhaDbMappingTbl);
        }

        if (CollectionUtils.isEmpty(insertDbMappingTbls)) {
            logger.info("mhaDbMappings has already exist, mhaId: {}, dbList: {}", mhaId, dbList);
            return;
        }

        logger.info("insertDbMappingTbls: {}", insertDbMappingTbls);
        mhaDbMappingTblDao.batchInsert(insertDbMappingTbls);
    }

    private void insertDbReplicationAndFilterMapping(MhaTblV2 mhaTblV2, MqConfigDto configDto) throws Exception {
        Long messengerFilterId = insertMessengerFilter(configDto);
        List<Long> dbReplicationIds = insertMessengerDbReplications(mhaTblV2, configDto);

        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationIds.stream()
                .map(dbReplicationId -> buildDbReplicationFilterMappingTbl(dbReplicationId, -1L, -1L, messengerFilterId))
                .collect(Collectors.toList());

        logger.info("batchInsert dbReplicationFilterMappings: {}", dbReplicationFilterMappings);
        dbReplicationFilterMappingTblDao.batchInsert(dbReplicationFilterMappings);
    }

    private DbReplicationFilterMappingTbl buildDbReplicationFilterMappingTbl(long dbReplicationId, long rowsFilterId, long columnsFilterId, long messengerFilterId) {
        DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
        dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationId);
        dbReplicationFilterMappingTbl.setRowsFilterId(rowsFilterId);
        dbReplicationFilterMappingTbl.setColumnsFilterId(columnsFilterId);
        dbReplicationFilterMappingTbl.setMessengerFilterId(messengerFilterId);
        dbReplicationFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return dbReplicationFilterMappingTbl;
    }

    private List<Long> insertMessengerDbReplications(MhaTblV2 mhaTblV2, MqConfigDto mqConfigDto) throws Exception {
        logger.info("insertMessengerDbReplications mhaTblV2: {}, mqConfigDto: {}", mhaTblV2, mqConfigDto);
        List<DbReplicationTbl> dbReplicationTbls = getDbReplications(mhaTblV2, mqConfigDto, true);

        dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        logger.info("insertMessengerDbReplications size: {}, dbReplicationTbls: {}", dbReplicationTbls.size(), dbReplicationTbls);
        List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
        return dbReplicationIds;
    }

    private Long insertMessengerFilter(MqConfigDto mqConfigDto) throws Exception {
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAll().stream().filter(e -> e.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Long messengerFilterId = null;
        String mqJsonString = JsonUtils.toJson(this.buildConfig(mqConfigDto));
        for (MessengerFilterTbl messengerFilterTbl : messengerFilterTbls) {
            if (messengerFilterTbl.getProperties().equals(mqJsonString)) {
                messengerFilterId = messengerFilterTbl.getId();
                break;
            }
        }
        if (messengerFilterId == null) {
            MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
            messengerFilterTbl.setDeleted(BooleanEnum.FALSE.getCode());
            messengerFilterTbl.setProperties(mqJsonString);

            logger.info("insertMessengerFilter: {}", messengerFilterTbl);
            messengerFilterId = messengerFilterTblDao.insertWithReturnId(messengerFilterTbl);
        }

        return messengerFilterId;
    }

    private List<DbReplicationTbl> getDbReplications(MhaTblV2 mhaTblV2, MqConfigDto mqConfigDto, boolean insert) throws Exception {
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByMhaId(mhaTblV2.getId());
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);

        String[] dbTables = mqConfigDto.getTable().split(Constants.ESCAPE_CHARACTER_DOT_REGEX);
        String srcLogicTable = dbTables[1];

        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(dbTables[0]);
        List<Long> srcDbIds = dbTbls.stream().filter(e -> aviatorRegexFilter.filter(e.getDbName())).map(DbTbl::getId).collect(Collectors.toList());
        List<Long> srcMhaDbMappingIds = mhaDbMappingTbls.stream().filter(e -> srcDbIds.contains(e.getDbId())).map(MhaDbMappingTbl::getId).collect(Collectors.toList());
        if (insert) {
            return srcMhaDbMappingIds.stream()
                    .map(srcMhaDbMappingId -> buildDbReplicationTbl(srcLogicTable, mqConfigDto.getTopic(), srcMhaDbMappingId, -1L, ReplicationTypeEnum.DB_TO_MQ.getType()))
                    .collect(Collectors.toList());
        } else {
            return dbReplicationTblDao.queryBy(srcMhaDbMappingIds, srcLogicTable, mqConfigDto.getTopic(), ReplicationTypeEnum.DB_TO_MQ.getType());
        }
    }

    private DbReplicationTbl buildDbReplicationTbl(String srcTableName, String dstTableName, long srcMhaDbMappingId, long dstMhaDbMappingId, int replicationType) {
        DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
        dbReplicationTbl.setSrcMhaDbMappingId(srcMhaDbMappingId);
        dbReplicationTbl.setDstMhaDbMappingId(dstMhaDbMappingId);
        dbReplicationTbl.setSrcLogicTableName(srcTableName);
        dbReplicationTbl.setDstLogicTableName(dstTableName);
        dbReplicationTbl.setReplicationType(replicationType);
        dbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return dbReplicationTbl;
    }


    private boolean initTopic(MqConfigDto dto, String dcNameForMha) {
        if (consoleConfig.getLocalConfigCloudDc().contains(dcNameForMha)) {
            logger.info("[[tag=qmqInit]] localConfigCloudDc init qmq topic:{}", dto.getTopic());
            return true;
        }
        String topicApplicationUrl = domainConfig.getQmqTopicApplicationUrl(dcNameForMha);
        LinkedHashMap<String, String> requestBody = Maps.newLinkedHashMap();
        requestBody.put("subject", dto.getTopic());
        requestBody.put("cluster", dto.isOrder() ? "ordered" : "default");
        requestBody.put("bu", dto.getBu());
        requestBody.put("creator", "drc");
        requestBody.put("emailGroup", "rdkjdrc@Ctrip.com");
        QmqApiResponse response = HttpUtils.post(topicApplicationUrl, requestBody, QmqApiResponse.class);

        if (response.getStatus() == 0) {
            logger.info("[[tag=qmqInit]] init qmq topic success,topic:{}", dto.getTopic());
        } else if (StringUtils.isNotBlank(response.getStatusMsg())
                && response.getStatusMsg().contains("already existed")) {
            logger.info("[[tag=qmqInit]] init qmq topic success,topic:{} already existed", dto.getTopic());
        } else {
            logger.error("[[tag=qmqInit]] init qmq topic fail,MqConfigDto:{}", dto);
            return false;
        }
        return true;
    }

    private boolean initProducer(MqConfigDto dto, String dcNameForMha) {
        if (consoleConfig.getLocalConfigCloudDc().contains(dcNameForMha)) {
            logger.info("[[tag=qmqInit]] localConfigCloudDc init qmq topic:{}", dto.getTopic());
            return true;
        }
        String producerApplicationUrl = domainConfig.getQmqProducerApplicationUrl(dcNameForMha);
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("appCode", "100023500");
        requestBody.put("subject", dto.getTopic());
        requestBody.put("durable", false);
        requestBody.put("tableStrategy", 0);
        requestBody.put("qpsAvg", 1000);
        requestBody.put("qpsMax", 5000);
        requestBody.put("msgLength", 1000);
        requestBody.put("platform", 1);
        requestBody.put("creator", "drc");
        requestBody.put("remark", "binlog_dataChange_message");

        QmqApiResponse response = HttpUtils.post(producerApplicationUrl, requestBody, QmqApiResponse.class);
        if (response.getStatus() == 0) {
            logger.info("[[tag=qmqInit]] init qmq producer success,topic:{}", dto.getTopic());
        } else if (StringUtils.isNotBlank(response.getStatusMsg())
                && response.getStatusMsg().contains("already existed")) {
            logger.info("[[tag=qmqInit]] init success,qmq producer already existed,topic:{}", dto.getTopic());
        } else {
            logger.error("[[tag=qmqInit]] init qmq producer fail,MqConfigDto:{}", dto);
            return false;
        }
        return true;
    }


    private void initMqConfig(MqConfigDto dto, MhaTblV2 mhaTblV2) throws Exception {
        String dcName = this.getDcName(mhaTblV2);
        // only support qmq
        if (!MqType.qmq.name().equalsIgnoreCase(dto.getMqType())) {
            throw new IllegalArgumentException("unsupported MqType");
        }

        if (!this.initTopic(dto, dcName)) {
            throw new IllegalArgumentException("init Topic error");
        }
        if (!this.initProducer(dto, dcName)) {
            throw new IllegalArgumentException("init producer error");
        }
    }

    private void addDalClusterMqConfig(MqConfigDto dto, MhaTblV2 mhaTblV2) throws Exception {
        String[] dbAndTable = dto.getTable().split(ESCAPE_CHARACTER_DOT_REGEX);
        if (dbAndTable.length != 2) {
            throw new IllegalArgumentException("illegal table name" + dto.getTable());
        }

        List<MySqlUtils.TableSchemaName> matchTables = mysqlServiceV2.getMatchTable(dto.getMhaName(), dto.getTable());
        String dcName = this.getDcName(mhaTblV2);
        transactionMonitor.logTransaction(
                "QConfig.OpenApi.MqConfig.Generate",
                dto.getTopic(),
                () -> qConfigService.addOrUpdateDalClusterMqConfig(
                        dcName, dto.getTopic(), dto.getTable(), null, matchTables
                )
        );
    }

    private void updateDalClusterMqConfig(MqConfigDto dto, MhaTblV2 mhaTblV2) throws Exception {
        // delete + add
        disableDalClusterMqConfigIfNecessary(dto.getDbReplicationId(), mhaTblV2);
        addDalClusterMqConfig(dto, mhaTblV2);
    }

    private void disableDalClusterMqConfigIfNecessary(Long dbReplicationId, MhaTblV2 mhaTblV2) throws Exception {
        DbReplicationTbl dbReplicationTbl = dbReplicationTblDao.queryById(dbReplicationId);
        MhaDbMappingTbl mhaDbMappingTbl = mhaDbMappingTblDao.queryById(dbReplicationTbl.getSrcMhaDbMappingId());
        DbTbl dbTbl = dbTblDao.queryById(mhaDbMappingTbl.getDbId());

        String dcName = this.getDcName(mhaTblV2);
        String dbName = dbTbl.getDbName();
        String tableName = dbReplicationTbl.getSrcLogicTableName();
        String topic = dbReplicationTbl.getDstLogicTableName();

        // to delete tables
        String nameFilter = dbName + "\\." + tableName;
        List<MySqlUtils.TableSchemaName> tablesToDelete = mysqlServiceV2.getMatchTable(mhaTblV2.getMhaName(), nameFilter);
        if (CollectionUtils.isEmpty(tablesToDelete)) {
            logger.info("no table to delete: "+ nameFilter);
            return;
        }

        // all topic tables
        List<String> sameTopicTableFilters = this.getTableNameFiltersWithSameTopic(topic);
        List<MySqlUtils.TableSchemaName> allTables = mysqlServiceV2.getAnyMatchTable(mhaTblV2.getMhaName(), String.join(",", sameTopicTableFilters));


        // final remain = all - delete
        Set<MySqlUtils.TableSchemaName> tablesToDeletSet = Sets.newHashSet(tablesToDelete);
        Set<MySqlUtils.TableSchemaName> allTablesSet = Sets.newHashSet(allTables);
        allTablesSet.removeAll(tablesToDeletSet);
        List<MySqlUtils.TableSchemaName> remainTables = Lists.newArrayList(allTablesSet);

        // do update dal qmq config
        String dalClusterName = dbClusterService.getDalClusterName(domainConfig.getDalClusterUrl(), tablesToDelete.get(0).getSchema());
        transactionMonitor.logTransaction(
                "QConfig.OpenApi.MqConfig.Delete",
                topic,
                () -> qConfigService.updateDalClusterMqConfig(
                        dcName,
                        topic,
                        dalClusterName,
                        remainTables
                )
        );
    }

    private List<String> getTableNameFiltersWithSameTopic(String topic) throws SQLException {
        // query other table with same topic
        List<DbReplicationTbl> dbReplicationTbls = dbReplicationTblDao.queryByDstLogicTableName(topic, ReplicationTypeEnum.DB_TO_MQ.getType());
        List<Long> srcMhaDbMapping = dbReplicationTbls.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).collect(Collectors.toList());
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(srcMhaDbMapping);
        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
        List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
        Map<Long, String> dbMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        return dbReplicationTbls.stream().map(e -> {
            Long dbId = mhaDbMappingMap.get(e.getSrcMhaDbMappingId());
            String db = dbMap.get(dbId);
            return db + "\\." + e.getSrcLogicTableName();
        }).collect(Collectors.toList());
    }


    private String getDcName(MhaTblV2 mhaTblV2) {
        List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
        Optional<String> dcNameOptional = dcDos.stream().filter(e -> e.getDcId().equals(mhaTblV2.getDcId())).map(DcDo::getDcName).findFirst();
        if (dcNameOptional.isEmpty()) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_DATA_INCOMPLETE, "dc not found for mha: " + mhaTblV2.getMhaName());
        }

        return dcNameOptional.get();
    }

    private MqConfig buildConfig(MqConfigDto dto) {
        MqConfig mqConfig = new MqConfig();
        mqConfig.setMqType(dto.getMqType());
        mqConfig.setSerialization(dto.getSerialization());
        mqConfig.setOrder(dto.isOrder());
        mqConfig.setOrderKey(dto.getOrderKey());
        mqConfig.setPersistent(dto.isPersistent());
        mqConfig.setPersistentDb(dto.getPersistentDb());
        mqConfig.setDelayTime(dto.getDelayTime());
        return mqConfig;
    }
}
