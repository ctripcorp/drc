package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dto.RowsFilterDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName ConfigGeneratorServiceImpl
 * @Author haodongPan
 * @Date 2022/4/29 14:38
 * @Version: $
 */
@Service
public class RowsFilterServiceImpl implements RowsFilterService {
    public static final Logger logger = LoggerFactory.getLogger(RowsFilterServiceImpl.class);
    
    @Autowired
    DataMediaTblDao dataMediaTblDao;
    
    @Autowired
    RowsFilterMappingTblDao rowsFilterMappingTblDao;
    
    @Autowired
    RowsFilterTblDao rowsFilterTblDao;
    
    @Override
    public List<RowsFilterConfig> generateRowsFiltersConfig (Long applierGroupId) throws SQLException {
        ArrayList<RowsFilterConfig> configs = Lists.newArrayList();
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = 
                rowsFilterMappingTblDao.queryByApplierGroupIds(List.of(applierGroupId), BooleanEnum.FALSE.getCode());
        Map<Long, List<RowsFilterMappingTbl>> mapGroupByRowsFilterId = 
                rowsFilterMappingTbls.stream().collect(Collectors.groupingBy(RowsFilterMappingTbl::getRowsFilterId));
        for (Map.Entry<Long, List<RowsFilterMappingTbl>> entry :mapGroupByRowsFilterId.entrySet()) {
            RowsFilterConfig config = new RowsFilterConfig();
            RowsFilterTbl rowsFilterTbl = rowsFilterTblDao.queryById(entry.getKey(), BooleanEnum.FALSE.getCode());
            config.setMode(String.valueOf(rowsFilterTbl.getMode()));
            List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByIdsAndType(
                    Lists.transform(entry.getValue(), RowsFilterMappingTbl::getDataMediaId),
                    DataMediaTypeEnum.REGEX_LOGIC.getType(),
                    BooleanEnum.FALSE.getCode());
            config.setTables(
                    dataMediaTbls.stream().map(DataMediaTbl::getFullName).collect(Collectors.joining(",")));
            config.setParameters(
                    JsonUtils.fromJson(rowsFilterTbl.getParameters(), RowsFilterConfig.Parameters.class));
            configs.add(config);
        }
        return configs;
    }

    @Override
    public String addRowsFilter(RowsFilterDto rowsFilterDto) throws SQLException {
        try {
            RowsFilterTbl rowsFilterTbl = rowsFilterDto.toRowsFilterTbl();
            rowsFilterTblDao.insert(rowsFilterTbl);
            return "add RowsFilter success";
        } catch (IllegalArgumentException e) {
            logger.error("[[meta=rowsFilter]] addRowsFilter error, rowsFilterDto:{}",rowsFilterDto,e);
            return "illegal argument for rowFilter";
        }
    }

    @Override
    public String addRowsFilterMapping(Long applierGroupId, Long dataMediaId, Long rowsFilterId) throws SQLException {
        RowsFilterMappingTbl rowsFilterMappingTbl = new RowsFilterMappingTbl();
        rowsFilterMappingTbl.setApplierGroupId(applierGroupId);
        rowsFilterMappingTbl.setDataMediaId(dataMediaId);
        rowsFilterMappingTbl.setRowsFilterId(rowsFilterId);
        int insert = rowsFilterMappingTblDao.insert(rowsFilterMappingTbl);
        return "add RowsFilterMapping success";
    }
}
