package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.vo.display.RowsFilterMappingVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {

    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId,int applierType) throws SQLException;
    
    String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    
    String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    
    String deleteRowsFilterConfig(Long id) throws SQLException;
  
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
    
    List<Integer> migrateUdlStrategyId(List<Long> rowsFilterIds) throws SQLException; 
}
