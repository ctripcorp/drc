package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.RowsFilterTblV2;
import com.ctrip.framework.drc.console.dao.v2.RowsFilterTblV2Dao;
import com.ctrip.framework.drc.console.enums.RowsFilterModeEnum;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig.Configs;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig.Parameters;
import com.ctrip.framework.drc.core.server.common.filter.row.Region;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/30 10:39
 */
@Service
public class RowsFilterServiceV2Impl implements RowsFilterServiceV2 {
    public static final Logger logger = LoggerFactory.getLogger(RowsFilterServiceV2Impl.class);

    @Autowired
    private RowsFilterTblV2Dao rowsFilterTblDao;


    @Override
    public List<RowsFilterTblV2> queryByIds(List<Long> idList) throws SQLException {
        if(CollectionUtils.isEmpty(idList)){
            return Collections.emptyList();
        }
        return rowsFilterTblDao.queryByIds(idList);
    }

    @Override
    public List<RowsFilterConfig> generateRowsFiltersConfig(String tableName, List<Long> rowsFilterIds) throws SQLException {
        List<RowsFilterTblV2> rowsFilterTbls = rowsFilterTblDao.queryByIds(rowsFilterIds);
        return this.generateRowsFiltersConfigFromTbl(tableName, rowsFilterTbls);
    }

    @Override
    public List<RowsFilterConfig> generateRowsFiltersConfigFromTbl(String tableName, List<RowsFilterTblV2> rowsFilterTbls) throws SQLException {
        List<RowsFilterConfig> rowsFilterConfigs = rowsFilterTbls.stream().map(source -> {
            RowsFilterConfig target = new RowsFilterConfig();
            target.setTables(tableName);
            target.setMode(RowsFilterModeEnum.getNameByCode(source.getMode()));

            String configsJson = source.getConfigs();
            target.setConfigs(JsonUtils.fromJson(configsJson, RowsFilterConfig.Configs.class));
            return target;
        }).collect(Collectors.toList());
        return rowsFilterConfigs;
    }

    @Override
    public List<Long> queryRowsFilterIdsShouldMigrate(String srcRegion) throws SQLException {
        if (!Region.SGP.isLegal(srcRegion)) {
            throw ConsoleExceptionUtils.message("illegal srcRegion: " + srcRegion);
        }
        List<Integer> udlRelatedModes = Lists.newArrayList(
                RowsFilterModeEnum.TRIP_UDL.getCode(),
                RowsFilterModeEnum.TRIP_UID.getCode(),
                RowsFilterModeEnum.TRIP_UDL_UID.getCode()
        );
        List<RowsFilterTblV2> rowsFilterTblV2s = rowsFilterTblDao.queryByModes(udlRelatedModes);
        List<Long> res = Lists.newArrayList();
        for (RowsFilterTblV2 rowsFilterTblV2 : rowsFilterTblV2s) {
            String configContent = rowsFilterTblV2.getConfigs();
            Configs filterConfigs = JsonUtils.fromJson(configContent, Configs.class);
            List<Parameters> parameterList = filterConfigs.getParameterList();
            boolean match = parameterList.stream().anyMatch(parameter -> srcRegion.equalsIgnoreCase(parameter.getContext()));
            if (match) {
                res.add(rowsFilterTblV2.getId());
            }
        }
        return res;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Pair<Boolean, Integer> migrateRowsFilterUDLRegion(List<Long> idsToUpdate,String srcRegion,String dstRegion) throws SQLException {
        if (!Region.SGP.isLegal(srcRegion) || !Region.SGP.isLegal(dstRegion)) {
            throw ConsoleExceptionUtils.message("illegal region: " + srcRegion + " or " + dstRegion);
        }
        if (CollectionUtils.isEmpty(idsToUpdate)) {
            return Pair.of(false, 0);
        }
        List<Long> idsAllowToUpdate = this.queryRowsFilterIdsShouldMigrate(srcRegion);
        if (!idsAllowToUpdate.containsAll(idsToUpdate)) {
            return Pair.of(false, 0);
        }
        List<RowsFilterTblV2> rowsFiltersToUpdate = rowsFilterTblDao.queryByIds(idsToUpdate);
        for (RowsFilterTblV2 rowsFilterTblV2 : rowsFiltersToUpdate) {
            String configBeforeChange = rowsFilterTblV2.getConfigs();
            Configs configs = JsonUtils.fromJson(configBeforeChange, Configs.class);
            List<Parameters> parameterList = configs.getParameterList();
            for (Parameters parameters : parameterList) {
                if (srcRegion.equalsIgnoreCase(parameters.getContext())) {
                    parameters.setContext(dstRegion);
                }
            }
            String configAfterChange = JsonUtils.toJson(configs);
            rowsFilterTblV2.setConfigs(configAfterChange);
            logger.info("migrateRowsFilterToSGP, id: {}, before:{},after: {}", rowsFilterTblV2.getId(),configBeforeChange, configAfterChange);
        }
        rowsFilterTblDao.batchUpdate(rowsFiltersToUpdate);
        return Pair.of(true, idsToUpdate.size());
    }

}
