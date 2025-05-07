package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.DbTableDrcRegionInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;

import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.List;

public interface OpenApiService {

    List<MessengerInfo> getAllMessengersInfo() throws SQLException;
    
    // return all infos if dbName is empty
    List<DrcDbInfo> getDrcDbInfos(String dbName);

    /**
     *
     * @param db dbName, must be physical existed db name (e.g. htlorderreceiptshard64db)
     * @param table tableName
     */
    DbTableDrcRegionInfo getDbTableDrcRegionInfos(@NotNull String db, @NotNull String table);
}
