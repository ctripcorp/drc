package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.display.ColumnsFilterVo;
import com.ctrip.framework.drc.console.vo.display.DataMediaVo;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName DataMediaServiceImpl
 * @Author haodongPan
 * @Date 2023/1/4 10:33
 * @Version: $
 */
@Service
public class DataMediaServiceImpl implements DataMediaService {

    @Autowired
    private DataMediaTblDao dataMediaTblDao;

    @Autowired
    private ColumnsFilterService columnsFilterService;

    @Autowired
    private RowsFilterService rowsFilterService;

    @Autowired
    private ApplierGroupTblDao applierGroupTblDao;

    @Autowired
    private DrcDoubleWriteService drcDoubleWriteService;


    @Override
    public DataMediaConfig generateConfig(Long applierGroupId) throws SQLException {
        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByAGroupId(applierGroupId, BooleanEnum.FALSE.getCode());

        // columnsFilterConfigs
        List<ColumnsFilterConfig> columnsFilterConfigs = Lists.newArrayList();
        for (DataMediaTbl dataMedia : dataMediaTbls) {
            ColumnsFilterConfig columnsFilterConfig = columnsFilterService.generateColumnsFilterConfig(dataMedia);
            if (null != columnsFilterConfig) {
                columnsFilterConfigs.add(columnsFilterConfig);
            }
        }
        if (!CollectionUtils.isEmpty(columnsFilterConfigs)) {
            dataMediaConfig.setColumnsFilters(columnsFilterConfigs);
        }

        // rowsFilters todo generate should change, generate by dataMediaId
        List<RowsFilterConfig> rowsFilterConfigs = rowsFilterService.generateRowsFiltersConfig(
                applierGroupId, ConsumeType.Applier.getCode());
        if (!CollectionUtils.isEmpty(rowsFilterConfigs)) {
            dataMediaConfig.setRowsFilters(rowsFilterConfigs);
        }

        // todo nameFilter, tableMapping , columnMapping
        return dataMediaConfig;
    }

    @Override
    public List<DataMediaVo> getAllDataMediaVos(Long applierGroupId) throws SQLException {
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByAGroupId(applierGroupId, BooleanEnum.FALSE.getCode());
        List<DataMediaVo> vos = Lists.newArrayList();
        for (DataMediaTbl dataMediaTbl : dataMediaTbls) {
            vos.add(DataMediaVo.toVo(dataMediaTbl));
        }
        return vos;
    }

    @Override
    public Long processAddDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        DataMediaTbl dataMediaTbl = dataMediaDto.transferTo();
        if (!checkFilter(dataMediaDto.getApplierGroupId(), dataMediaTbl)) {
            throw ConsoleExceptionUtils.message("nameFilter and columnsFilter not match!");
        }
        return dataMediaTblDao.insertReturnPk(dataMediaTbl);
    }

    @Override
    public Long processUpdateDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        DataMediaTbl dataMediaTbl = dataMediaDto.transferTo();
        if (!checkFilter(dataMediaDto.getApplierGroupId(), dataMediaTbl)) {
            throw ConsoleExceptionUtils.message("nameFilter and columnsFilter not match!");
        }
        int update = dataMediaTblDao.update(dataMediaTbl);
        return update == 1 ? dataMediaTbl.getId() : 0L;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Long processDeleteDataMedia(Long dataMediaId) throws Exception {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setDeleted(BooleanEnum.TRUE.getCode());
        dataMediaTbl.setId(dataMediaId);
        int update = dataMediaTblDao.update(dataMediaTbl);
        columnsFilterService.deleteColumnsFilter(dataMediaId);
        // todo migrate other config , eg: rowsFilter

        drcDoubleWriteService.deleteColumnsFilter(dataMediaId);
        return update == 1 ? dataMediaTbl.getId() : 0L;
    }


    @Override
    public ColumnsFilterVo getColumnsFilterConfig(Long dataMediaId) throws SQLException {
        ColumnsFilterTbl columnsFilter = columnsFilterService.getColumnsFilterTbl(dataMediaId);
        ColumnsFilterVo vo = new ColumnsFilterVo();
        vo.setId(columnsFilter.getId());
        vo.setMode(columnsFilter.getMode());
        vo.setColumns(JsonUtils.fromJsonToList(columnsFilter.getColumns(), String.class));
        return vo;
    }


    @Override
    public String processAddColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws Exception {
        DataMediaTbl dataMediaTbl = dataMediaTblDao.queryById(columnsFilterConfigDto.getDataMediaId());
        String result;
        if (dataMediaTbl != null && dataMediaTbl.getApplierGroupId() > 0L) {
            result =  columnsFilterService.updateColumnsFilterConfig(columnsFilterConfigDto);
        } else {
            result = columnsFilterService.addColumnsFilterConfig(columnsFilterConfigDto);
        }

        drcDoubleWriteService.insertOrUpdateColumnsFilter(columnsFilterConfigDto.getDataMediaId());
        return result;
    }

    @Override
    public String processUpdateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws Exception {
        String result =  columnsFilterService.updateColumnsFilterConfig(columnsFilterConfigDto);
        drcDoubleWriteService.insertOrUpdateColumnsFilter(columnsFilterConfigDto.getDataMediaId());
        return result;
    }

    @Override
    public String processDeleteColumnsFilterConfig(Long columnsFilterId) throws SQLException {
        //该接口不支持双写,暂时下线
        return "接口已下线";
//        return columnsFilterService.deleteColumnsFilterConfig(columnsFilterId);
    }

    private boolean checkFilter(Long applierGroupId, DataMediaTbl dataMediaTbl) throws SQLException {
        ApplierGroupTbl applierGroupTbl = applierGroupTblDao.queryByPk(applierGroupId);
        if (StringUtils.isBlank(applierGroupTbl.getNameFilter())) {
            return false;
        }
        List<String> nameFilters = Lists.newArrayList(applierGroupTbl.getNameFilter().split(","));
        return nameFilters.contains(dataMediaTbl.getFullName());
    }
}
