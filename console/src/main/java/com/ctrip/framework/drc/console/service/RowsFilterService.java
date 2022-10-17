package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {
    
    // todo
    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId,int applierType) throws SQLException;
    // todo
    String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    // todo
    String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    
    String deleteRowsFilterConfig(Long id) throws SQLException;
    // todo
    List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId,int applierType) throws SQLException;

    // forward by mhaName
    List<String> getTablesWithoutColumn(String column,String namespace,String name,String mhaName);

    // todo
    List<String> getLogicalTables(
            Long applierGroupId,
            int applierType,
            Long dataMediaId,
            String namespace,
            String name,
            String mhaName) throws SQLException;

    // forward by mhaName
    List<String> getConflictTables(String mhaName, String logicalTables);
}
