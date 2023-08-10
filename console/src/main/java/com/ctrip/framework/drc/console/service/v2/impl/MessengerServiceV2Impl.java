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
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.impl.MessengerServiceImpl;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
@Service
public class MessengerServiceV2Impl implements MessengerServiceV2 {
    private static final Logger logger = LoggerFactory.getLogger(MessengerServiceImpl.class);

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

    /**
     * mha -> mha_db_mapping_tbl -> db_replication_tbl
     * -> db_replication_filter_mapping_tbl -> messenger_filter_tbl
     *
     * @param mhaName
     * @return
     */
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

            // 1.3 Db
            List<Long> dbIds = mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList());
            List<DbTbl> dbTbls = dbTblDao.queryByIds(dbIds);
            Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

            // 1.4 DbReplicationFilterMappingTbl
            List<Long> dbReplicationIds = dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingList = dbReplicationFilterMappingTblDao.queryMessengerDbReplicationByIds(dbReplicationIds);
            // db_replication_tbl: db_replication_filter_mapping_tbl = 1:1
            Map<Long, DbReplicationFilterMappingTbl> map = dbReplicationFilterMappingList.stream().collect(Collectors.toMap(DbReplicationFilterMappingTbl::getDbReplicationId, e -> e, (e1, e2) -> e1));

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

                mqConfigVo.setMqType(mqConfig.getMqType());
                mqConfigVo.setSerialization(mqConfig.getSerialization());
                mqConfigVo.setOrderKey(mqConfig.getOrderKey());
                mqConfigVo.setOrder(mqConfig.isOrder());
                mqConfigVo.setPersistent(mqConfig.isPersistent());
                mqConfigVo.setDelayTime(mqConfig.getDelayTime());
                list.add(mqConfigVo);
                // todo by yongnian: 2023/8/10 rwos filter
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
            if (defaultConsoleConfig.getVpcMhaNames().contains(mhaTblV2.getMhaName())) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION, "Vpc mha not supported");
            }

            // check mha && dbReplication match
            List<Long> mhaDbMappingId = dbReplicationTbls.stream().map(DbReplicationTbl::getSrcMhaDbMappingId).collect(Collectors.toList());
            List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(mhaDbMappingId);
            boolean mhaIdNotMatch = mhaDbMappingTbls.stream().anyMatch(mhaDbMappingTbl -> !mhaTblV2.getId().equals(mhaDbMappingTbl.getMhaId()));
            if (mhaIdNotMatch) {
                throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION, "dbReplication not belong to this mha: " + mhaName);
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
}
