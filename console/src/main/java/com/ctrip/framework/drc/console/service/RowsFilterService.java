package com.ctrip.framework.drc.console.service;


import com.ctrip.framework.drc.console.dto.RowsFilterDto;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;

public interface RowsFilterService {
    
    List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException;

    String addRowsFilter(RowsFilterDto rowsFilterDto) throws SQLException;

    String addRowsFilterMapping(Long applierGroupId, Long dataMediaId, Long rowsFilterId) throws SQLException;
}
