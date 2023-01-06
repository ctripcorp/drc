package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import java.sql.SQLException;

public interface DataMediaService {
    
    DataMediaConfig generateConfig(Long applierGroupId) throws SQLException;
    
    Long processAddDataMedia(DataMediaDto dataMediaDto) throws SQLException;
    Long processUpdateDataMedia(DataMediaDto dataMediaDto) throws SQLException;
    Long processDeleteDataMedia(Long dataMediaId) throws SQLException;
    
    
    String processAddColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;
    String processUpdateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException;
    String processDeleteColumnsFilterConfig(Long columnsFilterId) throws SQLException;
    
}
