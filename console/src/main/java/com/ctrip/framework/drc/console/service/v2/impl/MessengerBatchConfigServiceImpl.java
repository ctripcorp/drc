package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MessengerFilterTbl;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MessengerFilterTblDao;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.dto.v3.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.error.AutoBuildErrorEnum;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildReq;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigService;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildService;
import com.ctrip.framework.drc.console.service.v2.MessengerBatchConfigService;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
@Service
@Lazy
public class MessengerBatchConfigServiceImpl implements MessengerBatchConfigService {
    private static final Logger logger = LoggerFactory.getLogger(MessengerBatchConfigServiceImpl.class);

    @Autowired
    private DrcAutoBuildService drcAutoBuildService;
    @Autowired
    private MessengerServiceV2 messengerServiceV2;
    @Autowired
    private DbReplicationTblDao dbReplicationTblDao;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MessengerFilterTblDao messengerFilterTblDao;
    @Autowired
    private QConfigService qConfigService;


    @Override
    public void processDeleteMqConfig(DbMqEditDto editDto, DbMqConfigInfoDto currentConfig) {
        // 1. delete from db
        this.deleteDbReplicationAndFilterMapping(editDto, currentConfig.getMhaMqDtos());
    }

    @Override
    public void processCreateMqConfig(DbMqCreateDto createDto, DbMqConfigInfoDto currentConfig) {
        // 1. check duplicate configuration
        this.preCheck(createDto, currentConfig);
        // 2. rpc init topic/producer
        this.initTopicIfNeeded(createDto, currentConfig);
        // 3. update db
        this.insertDbReplicationAndFilterMapping(createDto, currentConfig.getMhaMqDtos());
    }

    @Override
    public void processUpdateMqConfig(DbMqEditDto editDto, DbMqConfigInfoDto currentConfig) {
        // 1. check duplicate configuration
        this.preCheck(editDto, currentConfig);
        // 2. rpc init topic/producer
        this.initTopicIfNeeded(editDto, currentConfig);
        // 3. update db
        this.updateMetaDb(editDto, currentConfig.getMhaMqDtos());
    }

    private void updateMetaDb(DbMqEditDto editDto, List<MhaMqDto> mhaMqDtos) {
        this.deleteDbReplicationAndFilterMapping(editDto, mhaMqDtos);
        this.insertDbReplicationAndFilterMapping(editDto, mhaMqDtos);
    }

    private void insertDbReplicationAndFilterMapping(DbMqCreateDto createDto, List<MhaMqDto> mhaMqDtos) {
        try {
            Long messengerFilterId = insertMessengerFilter(createDto.getMqConfig());
            List<Long> dbReplicationIds = insertMessengerDbReplications(createDto, mhaMqDtos);
            insertFilterMappingTbl(dbReplicationIds, messengerFilterId);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.INSERT_TBL_EXCEPTION, e);
        }
    }

    private void insertFilterMappingTbl(List<Long> dbReplicationIds, Long messengerFilterId) throws SQLException {
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationIds.stream()
                .map(dbReplicationId -> buildDbReplicationFilterMappingTbl(dbReplicationId, messengerFilterId))
                .collect(Collectors.toList());

        logger.info("batchInsert dbReplicationFilterMappings: {}", dbReplicationFilterMappings);
        dbReplicationFilterMappingTblDao.batchInsert(dbReplicationFilterMappings);
    }

    private List<Long> insertMessengerDbReplications(DbMqCreateDto createDto, List<MhaMqDto> mhaMqDtos) throws SQLException {
        List<DbReplicationTbl> dbReplicationTbls = new ArrayList<>();
        for (MhaMqDto mhaMqDto : mhaMqDtos) {
            dbReplicationTbls.addAll(mhaMqDto.getMhaDbReplications().stream().map(e -> {
                DbReplicationTbl dbReplicationTbl = new DbReplicationTbl();
                dbReplicationTbl.setSrcMhaDbMappingId(e.getSrc().getMhaDbMappingId());
                dbReplicationTbl.setDstMhaDbMappingId(-1L);
                dbReplicationTbl.setSrcLogicTableName(createDto.getLogicTableConfig().getLogicTable());
                dbReplicationTbl.setDstLogicTableName(createDto.getLogicTableConfig().getDstLogicTable());
                dbReplicationTbl.setReplicationType(createDto.getMqConfig().getMqTypeEnum().getReplicationType().getType());
                dbReplicationTbl.setDeleted(BooleanEnum.FALSE.getCode());
                return dbReplicationTbl;
            }).collect(Collectors.toList()));
        }
        dbReplicationTblDao.batchInsertWithReturnId(dbReplicationTbls);
        return dbReplicationTbls.stream().map(DbReplicationTbl::getId).collect(Collectors.toList());

    }

    private Long insertMessengerFilter(MqConfigDto mqConfigDto) throws SQLException {
        List<MessengerFilterTbl> messengerFilterTbls = messengerFilterTblDao.queryAllExist();
        String mqJsonString = mqConfigDto.build().toJson();
        Long messengerFilterId = messengerFilterTbls.stream().filter(messengerFilterTbl -> messengerFilterTbl.getProperties().equals(mqJsonString)).findFirst().map(MessengerFilterTbl::getId).orElse(null);
        if (messengerFilterId != null) {
            return messengerFilterId;
        }
        MessengerFilterTbl messengerFilterTbl = new MessengerFilterTbl();
        messengerFilterTbl.setDeleted(BooleanEnum.FALSE.getCode());
        messengerFilterTbl.setProperties(mqJsonString);

        logger.info("insertMessengerFilter: {}", messengerFilterTbl);
        messengerFilterId = messengerFilterTblDao.insertWithReturnId(messengerFilterTbl);
        return messengerFilterId;
    }

    private DbReplicationFilterMappingTbl buildDbReplicationFilterMappingTbl(long dbReplicationId, long messengerFilterId) {
        DbReplicationFilterMappingTbl dbReplicationFilterMappingTbl = new DbReplicationFilterMappingTbl();
        dbReplicationFilterMappingTbl.setDbReplicationId(dbReplicationId);
        dbReplicationFilterMappingTbl.setRowsFilterId(-1L);
        dbReplicationFilterMappingTbl.setColumnsFilterId(-1L);
        dbReplicationFilterMappingTbl.setMessengerFilterId(messengerFilterId);
        dbReplicationFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return dbReplicationFilterMappingTbl;
    }

    private void deleteDbReplicationAndFilterMapping(DbMqEditDto editDto, List<MhaMqDto> mhaMqDtos) {
        try {
            List<Long> originDbReplicationIds = new ArrayList<>();
            for (MhaMqDto mhaMqDto : mhaMqDtos) {
                originDbReplicationIds.addAll(getOriginDbReplicationIds(mhaMqDto, editDto.getOriginLogicTableConfig()));
            }
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(originDbReplicationIds);
            List<DbReplicationTbl> dbReplicationTblList = dbReplicationTblDao.queryByIds(originDbReplicationIds);

            dbReplicationTblList.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            dbReplicationTblDao.batchUpdate(dbReplicationTblList);
            dbReplicationFilterMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            dbReplicationFilterMappingTblDao.batchUpdate(dbReplicationFilterMappingTbls);
        } catch (SQLException e) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.DELETE_TBL_EXCEPTION, e);
        }
    }


    /**
     * {topic}.status=on
     * {topic}.dbName=htlordermshard01db,htlordermshard02db
     * {topic}.tableName=ord_roomorder
     *
     * @param currentConfig
     */
    @Override
    public void refreshRegistryConfig(DbMqConfigInfoDto currentConfig) {
        Map<String, String> config = generateRegistryConfig(currentConfig);

        List<String> dcNames = currentConfig.getMhaMqDtos().stream().map(e -> e.getSrcMha().getDcName()).distinct().collect(Collectors.toList());
        for (String dcName : dcNames) {
            boolean result = qConfigService.reWriteDalClusterMqConfig(dcName, currentConfig.getDalclusterName(), config);
            if (!result) {
                throw ConsoleExceptionUtils.message("reWriteDalClusterMqConfig dalcluster binlog registry in qconfig fail!");
            }
        }
    }

    private Map<String, String> generateRegistryConfig(DbMqConfigInfoDto currentConfig) {
        List<MqLogicTableSummaryDto> logicTableSummaryDtos = filterAndSortRegistryConfig(currentConfig.getLogicTableSummaryDtos());
        List<LogicTableConfig> logicTableConfigs = logicTableSummaryDtos.stream().map(LogicTableSummaryDto::getConfig).collect(Collectors.toList());

        Map<String, List<MySqlUtils.TableSchemaName>> matchTableMap = getLogicTableToMatchedTableMap(currentConfig.getMhaMqDtos(), logicTableConfigs);

        ArrayList<String> dbNames = Lists.newArrayList(currentConfig.getDbNames());
        dbNames.sort(String::compareTo);

        return generateRegistryConfig(dbNames, logicTableConfigs, matchTableMap);
    }

    @VisibleForTesting
    public static List<MqLogicTableSummaryDto> filterAndSortRegistryConfig(List<MqLogicTableSummaryDto> logicTableSummaryDtos1) {
        return logicTableSummaryDtos1.stream()
                .filter(e -> CollectionUtils.isEmpty(e.getExcludeFilterTypes()) && CollectionUtils.isEmpty(e.getFilterFields()) && e.getDelayTime() == 0)
                .sorted(((o1, o2) -> {
                    long firstMaxId = o1.getDbReplicationIds().stream().mapToLong(e -> e).max().orElse(0L);
                    long secondMaxId = o2.getDbReplicationIds().stream().mapToLong(e -> e).max().orElse(0L);
                    return Long.compare(firstMaxId, secondMaxId);
                })).collect(Collectors.toList());
    }

    @VisibleForTesting
    public static Map<String, String> generateRegistryConfig(List<String> dbNames, List<LogicTableConfig> logicTableConfigs, Map<String, List<MySqlUtils.TableSchemaName>> matchTableMap) {
        String dbName = String.join(",", dbNames);

        Map<String, String> config = Maps.newLinkedHashMap();
        Set<MySqlUtils.TableSchemaName> tableSchemaNames = Sets.newHashSet();
        for (LogicTableConfig logicTableConfig : logicTableConfigs) {
            List<MySqlUtils.TableSchemaName> requestedTables = matchTableMap.get(logicTableConfig.getLogicTable());
            String tables = requestedTables.stream().filter(e -> !tableSchemaNames.contains(e))
                    .map(e -> e.getName().toLowerCase()).distinct().collect(Collectors.joining(","));
            tableSchemaNames.addAll(requestedTables);

            String topic = logicTableConfig.getDstLogicTable();
            appendConfig(config, topic, dbName, tables);
        }
        return config;
    }

    @VisibleForTesting
    protected Map<String, List<MySqlUtils.TableSchemaName>> getLogicTableToMatchedTableMap(List<MhaMqDto> mhaMqDtos, List<LogicTableConfig> logicTableConfigs) {
        List<DrcAutoBuildParam> drcBuildParam = new ArrayList<>();
        for (MhaMqDto mhaMqDto : mhaMqDtos) {
            String mhaName = mhaMqDto.getSrcMha().getName();
            Set<String> dbNamesInMha = mhaMqDto.getMhaDbReplications().stream().map(e -> e.getSrc().getDbName()).collect(Collectors.toSet());
            DrcAutoBuildParam drcAutoBuildParam = new DrcAutoBuildParam();
            drcAutoBuildParam.setSrcMhaName(mhaName);
            drcAutoBuildParam.setDbName(dbNamesInMha);
            drcBuildParam.add(drcAutoBuildParam);
        }
        Map<String, List<MySqlUtils.TableSchemaName>> matchTableMap = new HashMap<>();
        for (LogicTableConfig logicTableConfig : logicTableConfigs) {
            String logicTable = logicTableConfig.getLogicTable();
            if (matchTableMap.containsKey(logicTable)) {
                continue;
            }
            drcBuildParam.forEach(e -> e.setTableFilter(logicTable));
            List<MySqlUtils.TableSchemaName> requestedTables = drcAutoBuildService.getMatchTable(drcBuildParam);
            matchTableMap.put(logicTable, requestedTables);
        }
        return matchTableMap;
    }

    private static void appendConfig(Map<String, String> config, String topic, String dbs, String tables) {
        if (StringUtils.isEmpty(tables)) {
            return;
        }
        config.put(topic + "." + "status", "on");
        config.put(topic + "." + "dbName", dbs);

        String tableKey = topic + "." + "tableName";
        if (config.containsKey(tableKey)) {
            tables = config.get(tableKey) + "," + tables;
        }
        config.put(tableKey, tables);
    }


    private void initTopicIfNeeded(DbMqCreateDto editDto, DbMqConfigInfoDto currentConfig) {
        List<String> dcNames = currentConfig.getMhaMqDtos().stream().map(e -> e.getSrcMha().getDcName()).collect(Collectors.toList());
        messengerServiceV2.initMqConfigIfNeeded(editDto.getMqConfig(), dcNames);
    }

    // check duplicate configuration
    private void preCheck(DbMqCreateDto dbMqDto, DbMqConfigInfoDto currentConfig) {
        LogicTableConfig logicTableConfig = dbMqDto.getLogicTableConfig();
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setMode(DrcAutoBuildReq.BuildMode.MULTI_DB_NAME.getValue());
        req.setDbName(String.join(",", dbMqDto.getDbNames()));
        req.setSrcRegionName(dbMqDto.getSrcRegionName());
        req.setReplicationType(dbMqDto.getMqConfig().getMqTypeEnum().getReplicationType().getType());
        req.setTblsFilterDetail(new DrcAutoBuildReq.TblsFilterDetail(dbMqDto.getLogicTableConfig().getLogicTable()));
        List<MySqlUtils.TableSchemaName> requestedTables = drcAutoBuildService.getMatchTable(req);


        List<LogicTableConfig> currentTableConfig = currentConfig.getLogicTableSummaryDtos().stream().map(LogicTableSummaryDto::getConfig).collect(Collectors.toList());
        if (dbMqDto instanceof DbMqEditDto) {
            LogicTableConfig originLogicTableConfig = ((DbMqEditDto) dbMqDto).getOriginLogicTableConfig();
            currentTableConfig.removeIf(e -> e.getLogicTable().equalsIgnoreCase(originLogicTableConfig.getLogicTable())
                    && e.getDstLogicTable().equalsIgnoreCase(originLogicTableConfig.getDstLogicTable()));
        }
        for (LogicTableConfig otherConfig : currentTableConfig) {
            if (!otherConfig.getDstLogicTable().equalsIgnoreCase(logicTableConfig.getDstLogicTable())) {
                continue;
            }
            AviatorRegexFilter filter = new AviatorRegexFilter("(" + String.join("|", dbMqDto.getDbNames()) + ")" + "\\." + otherConfig.getLogicTable());
            for (MySqlUtils.TableSchemaName requestTable : requestedTables) {
                String directSchemaTableName = requestTable.getDirectSchemaTableName();
                boolean sameTable = filter.filter(directSchemaTableName);
                if (sameTable) {
                    throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.DUPLICATE_MQ_CONFIGURATION, directSchemaTableName + ":" + otherConfig.getDstLogicTable());
                }
            }
        }
    }

    private static List<Long> getOriginDbReplicationIds(MhaMqDto mhaMqDto, LogicTableConfig originLogicTableConfig) {
        List<LogicTableSummaryDto> oldSummaryDtos = LogicTableSummaryDto.from(mhaMqDto.getMhaDbReplications());
        List<LogicTableSummaryDto> originTarget = oldSummaryDtos.stream().filter(e -> e.getConfig().equals(originLogicTableConfig)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(originTarget) || originTarget.size() != 1) {
            throw ConsoleExceptionUtils.message(AutoBuildErrorEnum.MQ_CONFIG_CHECK_FAIL);
        }
        return originTarget.get(0).getDbReplicationIds();
    }


}
