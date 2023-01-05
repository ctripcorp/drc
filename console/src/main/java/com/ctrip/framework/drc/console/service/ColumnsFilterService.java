package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import java.sql.SQLException;

public interface ColumnsFilterService {

    ColumnsFilterConfig generateColumnsFilterConfig(DataMediaTbl dataMediaTbl) throws SQLException;

    String addColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;

    String updateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;

    String deleteColumnsFilterConfig(Long columnsFilterId) throws SQLException;


}
