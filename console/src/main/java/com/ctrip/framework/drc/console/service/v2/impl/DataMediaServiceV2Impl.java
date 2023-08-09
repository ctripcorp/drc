package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.service.v2.ColumnsFilterServiceV2;
import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.NumberUtils;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/26 15:49
 */
@Service
public class DataMediaServiceV2Impl implements DataMediaServiceV2 {

    @Autowired
    private ColumnsFilterServiceV2 columnsFilterService;
    @Autowired
    private RowsFilterServiceV2 rowsFilterService;
    @Autowired
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;

    @Override
    public DataMediaConfig generateConfig(List<DbReplicationDto> dbReplicationDtos) throws SQLException {
        List<MhaDbMappingTbl> mhaDbMappingTbls =
                mhaDbMappingTblDao.queryByIds(dbReplicationDtos.stream().map(DbReplicationDto::getSrcMhaDbMappingId).collect(Collectors.toList()));
        List<DbTbl> dbTbls = dbTblDao.queryByIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()));

        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        List<ColumnsFilterConfig> columnsFilters = new ArrayList<>();
        List<RowsFilterConfig> rowsFilters = new ArrayList<>();
        for (DbReplicationDto dbReplicationDto : dbReplicationDtos) {
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationFilterMappingTblDao.queryByDbReplicationId(dbReplicationDto.getDbReplicationId());
            if (CollectionUtils.isEmpty(dbReplicationFilterMappings)) {
                continue;
            }

            List<Long> rowsFilterIds = dbReplicationFilterMappings.stream().map(DbReplicationFilterMappingTbl::getRowsFilterId).filter(e -> e != null && e > 0L).collect(Collectors.toList());
            List<Long> columnsFilterIds = dbReplicationFilterMappings.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).filter(e -> e != null && e > 0L).collect(Collectors.toList());
            Long dbId = mhaDbMappingMap.getOrDefault(dbReplicationDto.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationDto.getSrcLogicTableName();

            List<ColumnsFilterConfig> columnsFilterConfigs = columnsFilterService.generateColumnsFilterConfig(tableName, columnsFilterIds);
            columnsFilters.addAll(columnsFilterConfigs);
            List<RowsFilterConfig> rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfig(tableName, rowsFilterIds);
            rowsFilters.addAll(rowsFilterConfigs);
        }

        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        dataMediaConfig.setColumnsFilters(columnsFilters);
        dataMediaConfig.setRowsFilters(rowsFilters);
        return dataMediaConfig;
    }


    /**
     * Fast version, only query once
     */
    @Override
    public DataMediaConfig generateConfigFast(List<DbReplicationDto> dbReplicationDtos) throws SQLException {
        // 1. prepare all data

        // 1.1 mha
        List<MhaDbMappingTbl> mhaDbMappingTbls = mhaDbMappingTblDao.queryByIds(dbReplicationDtos.stream().map(DbReplicationDto::getSrcMhaDbMappingId).collect(Collectors.toList()));
        Map<Long, Long> mhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));

        // 1.2 db
        List<DbTbl> dbTbls = dbTblDao.queryByIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));

        // 1.3 db replication
        List<Long> dbReplicationIds = dbReplicationDtos.stream().map(DbReplicationDto::getDbReplicationId).collect(Collectors.toList());
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls = dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationIds);
        Map<Long, List<DbReplicationFilterMappingTbl>> dbReplicationMap = dbReplicationFilterMappingTbls
                .stream().collect(Collectors.groupingBy(DbReplicationFilterMappingTbl::getDbReplicationId));

        // 1.4 rows filter
        List<Long> allRowsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getRowsFilterId).filter(NumberUtils::isPositive).collect(Collectors.toList());
        Map<Long, RowsFilterTblV2> rowsFilterMap = rowsFilterService.queryByIds(allRowsFilterIds)
                .stream().collect(Collectors.toMap(RowsFilterTblV2::getId, e -> e));

        // 1.5 cols filter
        List<Long> allColsFilterIds = dbReplicationFilterMappingTbls.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).filter(NumberUtils::isPositive).collect(Collectors.toList());
        Map<Long, ColumnsFilterTblV2> colsFilterMap = columnsFilterService.queryByIds(allColsFilterIds)
                .stream().collect(Collectors.toMap(ColumnsFilterTblV2::getId, e -> e));

        // 2. build result
        List<ColumnsFilterConfig> columnsFilters = new ArrayList<>();
        List<RowsFilterConfig> rowsFilters = new ArrayList<>();
        for (DbReplicationDto dbReplicationDto : dbReplicationDtos) {
            // table info
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationMap.get(dbReplicationDto.getDbReplicationId());
            if (CollectionUtils.isEmpty(dbReplicationFilterMappings)) {
                continue;
            }
            Long dbId = mhaDbMappingMap.getOrDefault(dbReplicationDto.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationDto.getSrcLogicTableName();

            // rows filter
            List<RowsFilterTblV2> rowsFilterTblList = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getRowsFilterId)
                    .filter(NumberUtils::isPositive)
                    .map(rowsFilterMap::get).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(rowsFilterTblList)) {
                List<RowsFilterConfig> rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfigFromTbl(tableName, rowsFilterTblList);
                rowsFilters.addAll(rowsFilterConfigs);
            }

            // cols filter
            List<ColumnsFilterTblV2> colsFilterTblList = dbReplicationFilterMappings.stream()
                    .map(DbReplicationFilterMappingTbl::getColumnsFilterId)
                    .filter(NumberUtils::isPositive)
                    .map(colsFilterMap::get).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(colsFilterTblList)) {
                List<ColumnsFilterConfig> columnsFilterConfigs = columnsFilterService.generateColumnsFilterConfigFromTbl(tableName, colsFilterTblList);
                columnsFilters.addAll(columnsFilterConfigs);
            }
        }

        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        dataMediaConfig.setColumnsFilters(columnsFilters);
        dataMediaConfig.setRowsFilters(rowsFilters);
        return dataMediaConfig;
    }
}
