package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.service.impl.RowsFilterServiceImpl;
import com.ctrip.framework.drc.console.service.v2.RowsFilterServiceV2;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/30 10:39
 */
@Service
public class RowsFilterServiceV2Impl implements RowsFilterServiceV2 {
    public static final Logger logger = LoggerFactory.getLogger(RowsFilterServiceImpl.class);

    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;

    private final String TRIP_UID = RowsFilterType.TripUid.getName();
    private final String TRIP_UDL = RowsFilterType.TripUdl.getName();

    @Override
    public List<RowsFilterConfig> generateRowsFiltersConfig(String tableName, List<Long> rowsFilterIds) throws SQLException {
        List<RowsFilterTbl> rowsFilterTbls = rowsFilterTblDao.queryByIds(rowsFilterIds);
        List<RowsFilterConfig> rowsFilterConfigs = rowsFilterTbls.stream().map(source -> {
            RowsFilterConfig target = new RowsFilterConfig();
            target.setTables(tableName);
            target.setMode(TRIP_UID.equalsIgnoreCase(source.getMode()) ? TRIP_UDL : source.getMode());

            String configsJson = source.getConfigs();
            if (StringUtils.isBlank(configsJson)) {
                RowsFilterConfig.Parameters parameters = JsonUtils.fromJson(source.getParameters(), RowsFilterConfig.Parameters.class);
                RowsFilterConfig.Configs configs = new RowsFilterConfig.Configs();
                configs.setParameterList(Lists.newArrayList(parameters));
                target.setConfigs(configs);

                migrateUdlConfigs(target, source);
            } else {
                target.setConfigs(JsonUtils.fromJson(configsJson, RowsFilterConfig.Configs.class));
            }
            return target;
        }).collect(Collectors.toList());
        return rowsFilterConfigs;
    }

    private void migrateUdlConfigs(RowsFilterConfig rowsFilterConfig, RowsFilterTbl rowsFilterTbl) {
        try {
            DefaultTransactionMonitorHolder.getInstance().logTransaction("console.meta", "udl.migrate.updateDb", () -> {
                String configsJson = JsonUtils.toJson(rowsFilterConfig.getConfigs());
                RowsFilterTbl sampleWithConfigs = new RowsFilterTbl();
                sampleWithConfigs.setId(rowsFilterTbl.getId());
                sampleWithConfigs.setMode(rowsFilterConfig.getMode());
                sampleWithConfigs.setConfigs(configsJson);
                int updateRows = rowsFilterTblDao.update(sampleWithConfigs);
                logger.info("[[tag=rowsFilter]] effect rows: {}, correct old rowsFilterTbl to udl_mode with id: {},config: {}",
                        updateRows, rowsFilterTbl.getId(), configsJson);
            });
        } catch (Exception e) {
            logger.error("[[tag=rowsFilter]] udl.migrate.updateDb fail,id: {}", rowsFilterTbl.getId(), e);
        }
    }
}
