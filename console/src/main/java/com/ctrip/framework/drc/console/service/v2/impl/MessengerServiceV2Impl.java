package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MessengerFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MessengerFilterTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.impl.MessengerServiceImpl;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
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
