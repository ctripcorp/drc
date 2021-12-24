package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.entity.UnitRouteVerificationHistoryTbl;
import com.ctrip.framework.drc.console.service.UnitService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UnitServiceImpl implements UnitService {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    private DalUtils dalUtils = DalUtils.getInstance();

    @Override
    public boolean addUnitRouteVerificationResult(ValidationResultDto dto) throws SQLException {
        int affected = dalUtils.insertUnitRouteVerificationHistory(
                dto.getSchemaName(),
                dto.getTableName(),
                dto.getGtid(),
                dto.getSql(),
                dto.getExpectedDc(),
                dto.getActualDc(),
                StringUtils.join(dto.getColumns(), ','),
                StringUtils.join(dto.getBeforeValues(), ','),
                StringUtils.join(dto.getAfterValues(), ','),
                dto.getUidName(),
                dto.getUcsStrategyId(),
                metaInfoService.getMhaGroupId(dto.getMhaName()),
                new Timestamp(dto.getExecuteTime() * 1000));
        return affected != 0;
    }

    @Override
    public List<UnitRouteVerificationHistoryTbl> getUnitRouteVerificationResult(String mha, String schemaName, String tableName) {
        try {
            Long mhaGroupId = metaInfoService.getMhaGroupId(mha);
            return dalUtils.getUnitRouteVerificationHistoryTblDao()
                    .queryAll(false)
                    .stream()
                    .filter(p -> mhaGroupId.equals(p.getMhaGroupId()) &&
                            ("".equalsIgnoreCase(schemaName) || schemaName.equalsIgnoreCase(p.getSchemaName())) &&
                            ("".equalsIgnoreCase(tableName) || tableName.equalsIgnoreCase(p.getTableName()))
                    )
                    .collect(Collectors.toList());
        } catch(SQLException e) {
            logger.error("[Unit] cannot get unit route verification result for {}, ", mha, e);
            return new ArrayList<>();
        }
    }
}
