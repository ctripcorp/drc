package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.vo.DataMediaVo;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;

import java.sql.SQLException;
import java.util.List;

public interface DataMediaService {
    
    String addDataMedia(DataMediaDto dataMediaDto) throws SQLException;
    
    String addDataMediaMapping(Long applierGroupId,  Long dataMediaId) throws SQLException;
    
    List<DataMediaVo> getDataMediaVos(Long applierGroupId,String srcMha) throws SQLException;
    
    List<DataMediaVo> getAllDataMediaVos() throws SQLException;

    List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException;

    String updateDataMedia(DataMediaDto dataMediaDto) throws SQLException;

    String deleteDataMedia(Long id) throws SQLException;
}
