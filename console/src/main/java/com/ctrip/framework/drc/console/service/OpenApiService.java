package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.console.vo.api.MhaGroupFilterVo;

import java.sql.SQLException;
import java.util.List;

public interface OpenApiService {

    @Deprecated
    List<MhaGroupFilterVo> getAllDrcMhaDbFilters() throws SQLException;

    List<MessengerInfo> getAllMessengersInfo() throws SQLException;
    
    // return all infos if dbName is empty
    List<DrcDbInfo> getDrcDbInfos(String dbName);
}
