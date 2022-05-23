package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterDto;
import com.ctrip.framework.drc.console.dto.RowsFilterMappingDto;
import com.ctrip.framework.drc.console.vo.RowsFilterVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {
    
    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException;

    String addRowsFilter(RowsFilterDto rowsFilterDto) throws SQLException;

    String addRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException;

    List<RowsFilterVo> getAllRowsFilterVos() throws SQLException;

    String updateRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException;

    String deleteRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException;
}
