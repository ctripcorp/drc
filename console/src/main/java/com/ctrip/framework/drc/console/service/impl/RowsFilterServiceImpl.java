package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.aop.PossibleRemote;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @ClassName ConfigGeneratorServiceImpl
 * @Author haodongPan
 * @Date 2022/4/29 14:38
 * @Version: $
 */
@Service
public class RowsFilterServiceImpl implements RowsFilterService {
    public static final Logger logger = LoggerFactory.getLogger(RowsFilterServiceImpl.class);
    public static final String DB_NAME = "fxdrcmetadb_w";

    @Autowired
    private DataMediaTblDao dataMediaTblDao;
    
    @Autowired
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;
    
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    
    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    @Override
    public List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException {
        ArrayList<RowsFilterConfig> rowsFilterConfigs = Lists.newArrayList();
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = 
                rowsFilterMappingTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
        for (RowsFilterMappingTbl mapping :  rowsFilterMappingTbls) {
            RowsFilterConfig rowsFilterConfig = new RowsFilterConfig();
            RowsFilterTbl rowsFilterTbl = rowsFilterTblDao.queryById(mapping.getRowsFilterId(), BooleanEnum.FALSE.getCode());

            DataMediaTbl dataMediaTbl = dataMediaTblDao.queryByIdsAndType(
                    Lists.newArrayList(mapping.getDataMediaId()), DataMediaTypeEnum.ROWS_FILTER.getType(), BooleanEnum.FALSE.getCode()
            ).get(0);
            rowsFilterConfig.setTables(dataMediaTbl.getFullName());
            
            // old rowsFilterConfig in parameters
            if (rowsFilterTbl.getMode().equals(RowsFilterType.TripUid.getName())) {
                // generate RowsFilterConfig.Configs by parameters
                RowsFilterConfig.Parameters parameters = JsonUtils.fromJson(rowsFilterTbl.getParameters(), RowsFilterConfig.Parameters.class);
                parameters.setUserFilterMode(UserFilterMode.Uid.getName());
                RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
                configs.setParameterList(Lists.newArrayList(parameters));
                
                String configsJson = JsonUtils.toJson(configs);
                logger.info("[[tag=rowsFilter]] old rowsFilterTbl in trip_uid mode with id:{},generate config:{}",
                        rowsFilterTbl.getId(),configsJson);
                
                // copy parameters to configs
                if ("on".equalsIgnoreCase(consoleConfig.getRowsFilterMigrateSwitch())) {
                    RowsFilterTbl sampleWithConfigs = new RowsFilterTbl();
                    sampleWithConfigs.setId(rowsFilterTbl.getId());
                    sampleWithConfigs.setMode(RowsFilterType.TripUdl.getName());
                    sampleWithConfigs.setConfigs(configsJson);
                    int updateRows = rowsFilterTblDao.update(sampleWithConfigs);
                    logger.info("[[tag=rowsFilter]] effect rows:{},correct old rowsFilterTbl to udl_mode with id:{},config:{}",
                            updateRows, rowsFilterTbl.getId(), configsJson);
                }
                
                //set RowsFilterConfig
                rowsFilterConfig.setMode(RowsFilterType.TripUdl.getName());
                rowsFilterConfig.setConfigs(configs);
            } else {
                rowsFilterConfig.setMode(rowsFilterTbl.getMode());
                rowsFilterConfig.setConfigs(JsonUtils.fromJson(rowsFilterTbl.getConfigs(), RowsFilterConfig.Configs.class));
            }
            rowsFilterConfigs.add(rowsFilterConfig);
        }
        return rowsFilterConfigs;
        
    }

    @Override
    public List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException {
        List<RowsFilterMappingVo> mappingVos = Lists.newArrayList();
        if (applierGroupId == null) {
            return mappingVos;
        }
        List<RowsFilterMappingTbl> rowsFilterMappingTbls =
                rowsFilterMappingTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
        for (RowsFilterMappingTbl mapping : rowsFilterMappingTbls) {
            List<DataMediaTbl> dataMediaTbls =
                    dataMediaTblDao.queryByIdsAndType(
                            Lists.newArrayList(mapping.getDataMediaId()),
                            DataMediaTypeEnum.ROWS_FILTER.getType(),
                            BooleanEnum.FALSE.getCode());
            RowsFilterTbl rowsFilterTbl =
                    rowsFilterTblDao.queryById(mapping.getRowsFilterId(), BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(dataMediaTbls) && rowsFilterTbl != null) {
                mappingVos.add(new RowsFilterMappingVo(mapping, dataMediaTbls.get(0), rowsFilterTbl));
            }
        }
        return mappingVos;
    }
    
    @Override
    @DalTransactional(logicDbName = DB_NAME)
    public String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException {
        DataMediaTbl dataMediaTbl = rowsFilterConfigDto.extractDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = rowsFilterConfigDto.extractRowsFilterTbl();
        Long dataMediaId = dataMediaTblDao.insertReturnPk(dataMediaTbl);
        Long rowsFilterId = rowsFilterTblDao.insertReturnPk(rowsFilterTbl);
        RowsFilterMappingTbl mappingTbl = new RowsFilterMappingTbl();
        mappingTbl.setApplierGroupId(rowsFilterConfigDto.getApplierGroupId());
        mappingTbl.setDataMediaId(dataMediaId);
        mappingTbl.setRowsFilterId(rowsFilterId);
        int insert = rowsFilterMappingTblDao.insert(mappingTbl);
        return insert == 1 ?  "insert rowsFilterConfig success" : "insert rowsFilterConfig fail";
    }

    @Override
    @DalTransactional(logicDbName = DB_NAME)
    public String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException {
        DataMediaTbl dataMediaTbl = rowsFilterConfigDto.extractDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = rowsFilterConfigDto.extractRowsFilterTbl();
        int update0 = dataMediaTblDao.update(dataMediaTbl);
        int update1 = rowsFilterTblDao.update(rowsFilterTbl);
        if (update0 + update1 == 2) {
            return "update rowsFilterConfig success";
        } else if (update0 == 1) {
            return "update dateMedia success";
        } else if (update1 == 1) {
            return "update rowsFilter success";
        } else {
            return "update rowsFilterConfig fail";
        }
    }

    @Override
    @DalTransactional(logicDbName = DB_NAME)
    public String deleteRowsFilterConfig(Long id) throws SQLException {
        RowsFilterMappingTbl mappingTbl = rowsFilterMappingTblDao.queryByPk(id);
        mappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(mappingTbl.getRowsFilterId());
        rowsFilterTbl.setDeleted(BooleanEnum.TRUE.getCode());
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(mappingTbl.getDataMediaId());
        dataMediaTbl.setDeleted(BooleanEnum.TRUE.getCode());
        int update0 = rowsFilterMappingTblDao.update(mappingTbl);
        int update1 = dataMediaTblDao.update(dataMediaTbl);
        int update2 = rowsFilterTblDao.update(rowsFilterTbl);
        
        return update0+update1+update2 == 3 ?  "delete rowsFilterConfig success" : "update rowsFilterConfig fail";
    }
    
    @Override
    @PossibleRemote(path = "/api/drc/v1/build/dataMedia/columnCheck")
    public List<String> getTablesWithoutColumn(String column,String namespace,String name,String mhaName) {
        List<String> tables = Lists.newArrayList();
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mhaName);
        List<MySqlUtils.TableSchemaName> tablesAfterRegexFilter = 
                MySqlUtils.getTablesAfterRegexFilter(endpoint, new AviatorRegexFilter(namespace + "\\." + name));
        Map<String, Set<String>> allColumnsByTable = MySqlUtils.getAllColumnsByTable(endpoint, tablesAfterRegexFilter, true);
        for (Map.Entry<String, Set<String>> entry : allColumnsByTable.entrySet()) {
            String tableName = entry.getKey();
            if (!entry.getValue().contains(column)) {
                tables.add(tableName);
            }
        }
        return tables;
    }

    
    @Override
    public List<String> getLogicalTables(
            Long applierGroupId, 
            Long dataMediaId, 
            String namespace, 
            String name,
            String mhaName) throws SQLException {
        List<RowsFilterMappingVo> rowsFilterMappingVos = getRowsFilterMappingVos(applierGroupId);
        List<String> logicalTables = Lists.newArrayList();
        if (dataMediaId == 0) { // add
            logicalTables = rowsFilterMappingVos.stream().
                    map(mappingVo -> mappingVo.getNamespace() + "\\." + mappingVo.getName()).collect(Collectors.toList());
        } else { // update
            logicalTables = rowsFilterMappingVos.stream().
                    filter(p -> !p.getDataMediaId().equals(dataMediaId)).
                    map(mappingVo -> mappingVo.getNamespace() + "\\." + mappingVo.getName()).collect(Collectors.toList());
        }
        logicalTables.add(namespace + "\\." + name);
        return logicalTables;
    }
    
    @Override
    @PossibleRemote(path = "/api/drc/v1/build/dataMedia/conflictCheck")
    public List<String> getConflictTables(String mhaName, String logicalTables)  {
        String[] tables = logicalTables.split(",");
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mhaName);
        HashSet<String> allTable = Sets.newHashSet();
        ArrayList<String> conflictTables = Lists.newArrayList();
        for (String logicalTable : tables) {
            AviatorRegexFilter filter = new AviatorRegexFilter(logicalTable);
            List<String> tablesAfterFilter = MySqlUtils.getTablesAfterRegexFilter(endpoint, filter).stream().
                    map(MySqlUtils.TableSchemaName ::getDirectSchemaTableName).collect(Collectors.toList());
            for (String table : tablesAfterFilter) {
                if (allTable.contains(table)) {
                    logger.warn("[[tag=checkTable]] contain common table: {}",table);
                    conflictTables.add(table);
                } else {
                    allTable.add(table);
                }
            }
        }
        return conflictTables;
    }

    
}
