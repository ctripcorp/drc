package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.vo.display.RowsFilterMappingVo;
import com.ctrip.framework.drc.console.vo.response.migrate.MigrateResult;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {

    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId,int applierType) throws SQLException;
    
    String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws Exception;
    
    String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws Exception;
    
    String deleteRowsFilterConfig(Long id) throws Exception;
  
    List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId,int applierType) throws SQLException;

    // forward by mhaName
    List<String> getTablesWithoutColumn(String column,String namespace,String name,String mhaName);

    
    List<String> getLogicalTables(
            Long applierGroupId,
            int applierType,
            Long dataMediaId,
            String namespace,
            String name,
            String mhaName) throws SQLException;

    // forward by mhaName
    List<String> getConflictTables(String mhaName, String logicalTables);
    
    List<Long> getMigrateRowsFilterIds() throws SQLException;

    MigrateResult migrateUdlStrategyId(List<Long> rowsFilterIds) throws SQLException; 
}
