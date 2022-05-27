package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
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
                    DataMediaTypeEnum.ROWS_FILTER.getType(),
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
    public List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException {
        List<RowsFilterMappingVo> mappingVos = Lists.newArrayList();
        if (applierGroupId == null) {
            return mappingVos;
        }
        List<RowsFilterMappingTbl> rowsFilterMappingTbls =
                rowsFilterMappingTblDao.queryByApplierGroupIds(Lists.newArrayList(applierGroupId), BooleanEnum.FALSE.getCode());
        for (RowsFilterMappingTbl mapping : rowsFilterMappingTbls) {
            List<DataMediaTbl> dataMediaTbls =
                    dataMediaTblDao.queryByIdsAndType(
                            Lists.newArrayList(mapping.getDataMediaId()),
                            DataMediaTypeEnum.ROWS_FILTER.getType(),
                            BooleanEnum.FALSE.getCode());
            RowsFilterTbl rowsFilterTbl =
                    rowsFilterTblDao.queryById(mapping.getRowsFilterId(), BooleanEnum.FALSE.getCode());
            if (!CollectionUtils.isEmpty(dataMediaTbls) && rowsFilterTbl != null) {
                mappingVos.add(new RowsFilterMappingVo(mapping, dataMediaTbls.get(0), rowsFilterTbl));
            }
        }
        return mappingVos;
    }
    
    @Override
    public String addRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException {
        DataMediaTbl dataMediaTbl = rowsFilterConfigDto.getDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = rowsFilterConfigDto.getRowsFilterTbl();
        Long dataMediaId = dataMediaTblDao.insertReturnPk(dataMediaTbl);
        Long rowsFilterId = rowsFilterTblDao.insertReturnPk(rowsFilterTbl);
        RowsFilterMappingTbl mappingTbl = new RowsFilterMappingTbl();
        mappingTbl.setApplierGroupId(rowsFilterConfigDto.getApplierGroupId());
        mappingTbl.setDataMediaId(dataMediaId);
        mappingTbl.setRowsFilterId(rowsFilterId);
        int insert = rowsFilterMappingTblDao.insert(mappingTbl);
        return insert == 1 ?  "insert rowsFilterConfig success" : "insert rowsFilterConfig fail";
    }

    @Override
    public String updateRowsFilterConfig(RowsFilterConfigDto rowsFilterConfigDto) throws SQLException {
        DataMediaTbl dataMediaTbl = rowsFilterConfigDto.getDataMediaTbl();
        RowsFilterTbl rowsFilterTbl = rowsFilterConfigDto.getRowsFilterTbl();
        int update0 = dataMediaTblDao.update(dataMediaTbl);
        int update1 = rowsFilterTblDao.update(rowsFilterTbl);
        return update0+update1 >= 1 ?  "update rowsFilterConfig success" : "update rowsFilterConfig fail";
    }

    @Override
    public String deleteRowsFilterConfig(Long id) throws SQLException {
        RowsFilterMappingTbl mappingTbl = rowsFilterMappingTblDao.queryByPk(id);
        mappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        RowsFilterTbl rowsFilterTbl = new RowsFilterTbl();
        rowsFilterTbl.setId(mappingTbl.getRowsFilterId());
        rowsFilterTbl.setDeleted(BooleanEnum.TRUE.getCode());
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setId(mappingTbl.getDataMediaId());
        dataMediaTbl.setDeleted(BooleanEnum.TRUE.getCode());

        int update0 = rowsFilterMappingTblDao.update(mappingTbl);
        int update1 = dataMediaTblDao.update(dataMediaTbl);
        int update2 = rowsFilterTblDao.update(rowsFilterTbl);
        
        return update0+update1+update2 == 3 ?  "delete rowsFilterConfig success" : "update rowsFilterConfig fail";
    }

    
}
