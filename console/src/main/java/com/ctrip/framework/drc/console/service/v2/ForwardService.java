package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;

import java.sql.SQLException;
import java.util.List;

public interface ForwardService {
    
    List<MhaTblV2> getMhaTblV2s(String dcName) throws SQLException;

    List<MhaDbReplicationDto> getMhaDbReplications(String dcName) throws SQLException;
}
