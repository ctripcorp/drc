package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;

import java.sql.SQLException;
import java.util.List;

public interface OpenApiService {

    List<MessengerInfo> getAllMessengersInfo() throws SQLException;
    
    // return all infos if dbName is empty
    List<DrcDbInfo> getDrcDbInfos(String dbName);
}
