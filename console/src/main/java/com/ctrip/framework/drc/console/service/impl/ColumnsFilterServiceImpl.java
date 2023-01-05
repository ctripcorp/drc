package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.ColumnsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.ColumnsFilterTbl;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.sql.SQLException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName ColumnsFilterServiceImpl
 * @Author haodongPan
 * @Date 2023/1/4 10:33
 * @Version: $
 */
@Service
public class ColumnsFilterServiceImpl implements ColumnsFilterService {
    
    @Autowired private ColumnsFilterTblDao columnsFilterTblDao;

    @Override
    public ColumnsFilterConfig generateColumnsFilterConfig(DataMediaTbl dataMediaTbl) throws SQLException {
        List<ColumnsFilterTbl> columnFilterTbls = columnsFilterTblDao.queryByDataMediaId(dataMediaTbl.getId(),
                BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(columnFilterTbls)) {
            return null;
        } else {
            ColumnsFilterTbl columnFilterTbl = columnFilterTbls.get(0);
            ColumnsFilterConfig columnsFilterConfig = new ColumnsFilterConfig();
            columnsFilterConfig.setTables(dataMediaTbl.getFullName());
            columnsFilterConfig.setMode(columnFilterTbl.getMode());
            columnsFilterConfig.setColumns(JsonUtils.fromJsonToList(columnFilterTbl.getColumns(),String.class));
            return columnsFilterConfig;
        }
    }

    @Override         
    public String addColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException {
        ColumnsFilterTbl columnsFilterTbl = columnsFilterConfigDto.transferTo();
        int insert = columnsFilterTblDao.insert(columnsFilterTbl);
        return insert == 1 ? "add ColumnsFilterConfig success" : "add ColumnsFilterConfig fail";
    }
 
    @Override
    public String updateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException {
        ColumnsFilterTbl columnsFilterTbl = columnsFilterConfigDto.transferTo();
        int update = columnsFilterTblDao.update(columnsFilterTbl);
        return update == 1 ? "update ColumnsFilterConfig success" : "update ColumnsFilterConfig fail";
    }

    @Override
    public String deleteColumnsFilterConfig(Long columnsFilterId) throws SQLException {
        ColumnsFilterTbl columnsFilterTbl = new ColumnsFilterTbl();
        columnsFilterTbl.setId(columnsFilterId);
        columnsFilterTbl.setDeleted(BooleanEnum.TRUE.getCode());
        int update = columnsFilterTblDao.update(columnsFilterTbl);
        return update == 1 ? "delete ColumnsFilterConfig success" : "delete ColumnsFilterConfig fail";
    } 
   
}
