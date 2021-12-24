package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dao.entity.UnitRouteVerificationHistoryTbl;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;

import java.sql.SQLException;
import java.util.List;

public interface UnitService {
    boolean addUnitRouteVerificationResult(ValidationResultDto dto) throws SQLException;
    List<UnitRouteVerificationHistoryTbl> getUnitRouteVerificationResult(String mha, String schemaName, String tableName);
}
