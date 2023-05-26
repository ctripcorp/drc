package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.DbTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaDbMappingTbl;
import com.ctrip.framework.drc.console.dao.v2.ColumnsFilterTblV2Dao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaDbMappingTblDao;
import com.ctrip.framework.drc.console.dto.v2.DbReplicationDto;
import com.ctrip.framework.drc.console.service.v2.ColumnsFilterServiceV2;
import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
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
    private DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Autowired
    private MhaDbMappingTblDao mhaDbMappingTblDao;
    @Autowired
    private DbTblDao dbTblDao;

    @Override
    public DataMediaConfig generateConfig(List<DbReplicationDto> dbReplicationDtos) throws SQLException {
        List<MhaDbMappingTbl> mhaDbMappingTbls =
                mhaDbMappingTblDao.queryByIds(dbReplicationDtos.stream().map(DbReplicationDto::getSrcMhaDbMappingId).collect(Collectors.toList()));
        List<DbReplicationFilterMappingTbl> dbReplicationFilterMappingTbls =
                dbReplicationFilterMappingTblDao.queryByDbReplicationIds(dbReplicationDtos.stream().map(DbReplicationDto::getDbReplicationId).collect(Collectors.toList()));
        List<DbTbl> dbTbls = dbTblDao.queryByIds(mhaDbMappingTbls.stream().map(MhaDbMappingTbl::getDbId).collect(Collectors.toList()));

        Map<Long, Long> MhaDbMappingMap = mhaDbMappingTbls.stream().collect(Collectors.toMap(MhaDbMappingTbl::getId, MhaDbMappingTbl::getDbId));
        Map<Long, String> dbTblMap = dbTbls.stream().collect(Collectors.toMap(DbTbl::getId, DbTbl::getDbName));
        Map<Long, List<DbReplicationFilterMappingTbl>> dbReplicationFilterMappingMap =
                dbReplicationFilterMappingTbls.stream().collect(Collectors.groupingBy(DbReplicationFilterMappingTbl::getDbReplicationId));

        List<ColumnsFilterConfig> columnsFilters = new ArrayList<>();
        List<RowsFilterConfig> rowsFilters = new ArrayList<>();
        for (DbReplicationDto dbReplicationDto : dbReplicationDtos) {
            List<DbReplicationFilterMappingTbl> dbReplicationFilterMappings = dbReplicationFilterMappingMap.get(dbReplicationDto.getDbReplicationId());
            if (CollectionUtils.isEmpty(dbReplicationFilterMappings)) {
                continue;
            }

            //todo
            List<Long> rowsFilterIds = dbReplicationFilterMappings.stream().map(DbReplicationFilterMappingTbl::getRowsFilterId).filter(e -> e != null && e > 0L).collect(Collectors.toList());
            List<Long> columnsFilterIds = dbReplicationFilterMappings.stream().map(DbReplicationFilterMappingTbl::getColumnsFilterId).filter(e -> e != null && e > 0L).collect(Collectors.toList());
            Long dbId = MhaDbMappingMap.getOrDefault(dbReplicationDto.getSrcMhaDbMappingId(), 0L);
            String dbName = dbTblMap.getOrDefault(dbId, "");
            String tableName = dbName + "\\." + dbReplicationDto.getSrcLogicTableName();

            List<ColumnsFilterConfig> columnsFilterConfigs = columnsFilterService.generateColumnsFilterConfig(tableName, columnsFilterIds);
            columnsFilters.addAll(columnsFilterConfigs);
        }

        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        dataMediaConfig.setColumnsFilters(columnsFilters);
        dataMediaConfig.setRowsFilters(rowsFilters);
        return dataMediaConfig;
    }
}
