package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dto.RowsFilterDto;
import com.ctrip.framework.drc.console.dto.RowsFilterMappingDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.RowsFilterVo;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.google.common.collect.Lists;
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
                rowsFilterMappingTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
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
    public String addRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException {
        RowsFilterMappingTbl rowsFilterMappingTbl = mappingDto.transferToTbl();
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = rowsFilterMappingTblDao.queryBy(rowsFilterMappingTbl);
        if (!CollectionUtils.isEmpty(rowsFilterMappingTbls)) {
            rowsFilterMappingTbl = rowsFilterMappingTbls.get(0);
            rowsFilterMappingTbl.setDeleted(BooleanEnum.FALSE.getCode());
            int update = rowsFilterMappingTblDao.update(rowsFilterMappingTbl);
            return update == 1 ? "update RowsFilterMapping success" : "update RowsFilterMapping fail";
        }
        int insert = rowsFilterMappingTblDao.insert(rowsFilterMappingTbl);
        return insert == 1 ? "add RowsFilterMapping success" : "add RowsFilterMapping fail";
    }

    @Override
    public List<RowsFilterVo> getAllRowsFilterVos() throws SQLException {
        List<RowsFilterVo> vos = Lists.newArrayList();
        List<RowsFilterTbl> rowsFilterTbls = rowsFilterTblDao.queryAll();
        if (CollectionUtils.isEmpty(rowsFilterTbls)) {
            return vos;
        } else {
            return rowsFilterTbls.stream().
                    filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode())).
                    map(RowsFilterVo::new).collect(Collectors.toList());
        }
    }

    @Override
    public String updateRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException {
        RowsFilterMappingTbl rowsFilterMappingTbl = mappingDto.transferToTbl();
        int update = rowsFilterMappingTblDao.update(rowsFilterMappingTbl);
        return update == 1 ? "update RowsFilterMapping success" : "update RowsFilterMapping fail";
    }

    @Override
    public String deleteRowsFilterMapping(RowsFilterMappingDto mappingDto) throws SQLException {
        RowsFilterMappingTbl rowsFilterMappingTbl = mappingDto.transferToTbl();
        rowsFilterMappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        int update = rowsFilterMappingTblDao.update(rowsFilterMappingTbl);
        return update == 1 ? "delete RowsFilterMapping success" : "delete RowsFilterMapping fail";
    }
}
