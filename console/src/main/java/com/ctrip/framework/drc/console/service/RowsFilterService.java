package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {
    
    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException;
    
    String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;

    String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    
    String deleteRowsFilterConfig(Long id) throws SQLException;
    
    List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException;

    // forward by mhaName
    List<String> getTablesWithoutColumn(String column,String namespace,String name,String mhaName);

    List<String> getLogicalTables(
            Long applierGroupId,
            Long dataMediaId,
            String namespace,
            String name,
            String mhaName) throws SQLException;

    // forward by mhaName
    List<String> getConflictTables(String mhaName, List<String> logicalTables);
}
