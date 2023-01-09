package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.ColumnsFilterService;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.google.common.collect.Lists;
import java.sql.SQLException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName DataMediaServiceImpl
 * @Author haodongPan
 * @Date 2023/1/4 10:33
 * @Version: $
 */
@Service
public class DataMediaServiceImpl implements DataMediaService {
    
    @Autowired private DataMediaTblDao dataMediaTblDao;
    
    @Autowired private ColumnsFilterService columnsFilterService;
    

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
        
        // todo rowsFilters config generate function should migrate here....
        return dataMediaConfig;
    }

    @Override
    public Long processAddDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        DataMediaTbl dataMediaTbl = dataMediaDto.transferTo();
        return dataMediaTblDao.insertReturnPk(dataMediaTbl);
    }

    @Override
    public Long processUpdateDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        DataMediaTbl dataMediaTbl = dataMediaDto.transferTo();
        int update = dataMediaTblDao.update(dataMediaTbl);
        return update == 1 ? dataMediaTbl.getId() : 0L;
    }

    @Override
    public Long processDeleteDataMedia(Long dataMediaId) throws SQLException {
        DataMediaTbl dataMediaTbl = new DataMediaTbl();
        dataMediaTbl.setDeleted(BooleanEnum.TRUE.getCode());
        dataMediaTbl.setId(dataMediaId);
        int update = dataMediaTblDao.update(dataMediaTbl);
        return update == 1 ? dataMediaTbl.getId() : 0L;
    }

    
    @Override
    public String processAddColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException {
        return columnsFilterService.addColumnsFilterConfig(columnsFilterConfigDto);
    }

    @Override
    public String processUpdateColumnsFilterConfig(ColumnsFilterConfigDto columnsFilterConfigDto) throws SQLException {
        return columnsFilterService.updateColumnsFilterConfig(columnsFilterConfigDto);
    }

    @Override
    public String processDeleteColumnsFilterConfig(Long columnsFilterId) throws SQLException {
        return columnsFilterService.deleteColumnsFilterConfig(columnsFilterId);
    }
}
