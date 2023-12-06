package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.param.mysql.DdlHistoryEntity;

import java.sql.SQLException;
import java.util.List;

public interface CentralService {
    
    List<MhaTblV2> getMhaTblV2s(String dcName) throws SQLException;

    Integer insertDdlHistory(DdlHistoryEntity requestBody) throws SQLException;

}
