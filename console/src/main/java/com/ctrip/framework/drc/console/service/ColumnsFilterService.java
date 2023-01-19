package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import java.sql.SQLException;
import java.util.List;

public interface ColumnsFilterService {

    ColumnsFilterConfig generateColumnsFilterConfig(DataMediaTbl dataMediaTbl) throws SQLException;
    
    String addColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;

    String updateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;

    String deleteColumnsFilterConfig(Long columnsFilterId) throws SQLException;

    ColumnsFilterTbl getColumnsFilterTbl(Long dataMediaId) throws SQLException;

    void deleteColumnsFilter(Long dataMediaId) throws SQLException;
}
