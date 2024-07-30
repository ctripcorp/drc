package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;

import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by dengquanliang
 * 2023/5/30 10:39
 */
public interface RowsFilterServiceV2 {

    List<RowsFilterTblV2> queryByIds(List<Long> rowsFilterIds) throws SQLException;

    List<RowsFilterConfig> generateRowsFiltersConfig(String tableName, List<Long> rowsFilterIds) throws SQLException;

    List<RowsFilterConfig> generateRowsFiltersConfigFromTbl(String tableName, List<RowsFilterTblV2> rowsFilterTbls) throws SQLException;
    
    List<Long> queryRowsFilterIdsShouldMigrate(String srcRegion) throws SQLException;
    
    // key: success or not, value: the number of rows filter migrated to SGP
    Pair<Boolean,Integer> migrateRowsFilterUDLRegion(List<Long> rowsFilterIds,String srcRegion,String dstRegion) throws SQLException;
    
}
