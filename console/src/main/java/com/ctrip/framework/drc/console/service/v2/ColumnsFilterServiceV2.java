package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/26 16:20
 */
public interface ColumnsFilterServiceV2 {
    List<ColumnsFilterTblV2> queryByIds(List<Long> columnsFilterIds) throws SQLException;

    List<ColumnsFilterConfig> generateColumnsFilterConfig(String tableName, List<Long> columnsFilterIds) throws SQLException;

    List<ColumnsFilterConfig> generateColumnsFilterConfigFromTbl(String tableName, List<ColumnsFilterTblV2> colsFilterTblList);
}
