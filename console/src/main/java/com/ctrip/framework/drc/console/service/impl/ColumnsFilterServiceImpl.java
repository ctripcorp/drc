package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.console.vo.ColumnsFilterMappingVo;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jixinwang on 2022/12/30
 */
@Service
public class ColumnsFilterServiceImpl implements ColumnsFilterService {

    public static final Logger logger = LoggerFactory.getLogger(RowsFilterServiceImpl.class);
    public static final String DB_NAME = "fxdrcmetadb_w";

    @Autowired
    private DataMediaTblDao dataMediaTblDao;

    @Autowired
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;

    @Autowired
    private ColumnsFilterMappingTblDao columnsFilterMappingTblDao;

    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;

    @Autowired
    private ColumnsFilterTblDao columnsFilterTblDao;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Override
    @DalTransactional(logicDbName = DB_NAME)
    public String addColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException {
        DataMediaTbl dataMediaTbl = columnsFilterConfigDto.extractDataMediaTbl();
        ColumnsFilterTbl columnsFilterTbl = columnsFilterConfigDto.extractRowsFilterTbl();
        Long dataMediaId = dataMediaTblDao.insertReturnPk(dataMediaTbl);
        Long rowsFilterId = columnsFilterTblDao.insertReturnPk(columnsFilterTbl);
        ColumnsFilterMappingTbl mappingTbl = new ColumnsFilterMappingTbl();
        mappingTbl.setApplierGroupId(columnsFilterConfigDto.getApplierGroupId());
        mappingTbl.setDataMediaId(dataMediaId);
        mappingTbl.setColumnsFilterId(rowsFilterId);
        int insert = columnsFilterMappingTblDao.insert(mappingTbl);
        return insert == 1 ?  "insert columnsFilterConfig success" : "insert columnsFilterConfig fail";
    }

    @Override
    public String updateColumnsFilterConfig(ColumnsFilterConfigDto rowsFilterConfigDto) throws SQLException {
        return null;
    }

    @Override
    public String deleteColumnsFilterConfig(Long id) throws SQLException {
        return null;
    }

    @Override
    public List<ColumnsFilterMappingVo> getColumnsFilterMappingVos(Long applierGroupId, int applierType) throws SQLException {
        return null;
    }
}
