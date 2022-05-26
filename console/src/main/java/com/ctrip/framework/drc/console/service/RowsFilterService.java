package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.RowsFilterDto;
import com.ctrip.framework.drc.console.dto.RowsFilterMappingDto;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.ctrip.framework.drc.console.vo.RowsFilterVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {
    
    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException;
    
    String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;

    String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException;
    
    String deleteRowsFilterConfig(Long id) throws SQLException;
    
    List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException;
    

}
