package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.service.DataMediaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

/**
 * @ClassName DataMediaServiceImpl
 * @Author haodongPan
 * @Date 2022/5/6 21:13
 * @Version: $
 */
@Service
public class DataMediaServiceImpl implements DataMediaService {
    
    public static final Logger logger = LoggerFactory.getLogger(DataMediaServiceImpl.class);
    @Autowired
    private DataMediaTblDao dataMediaTblDao;
    
    @Autowired
    private RowsFilterMappingTblDao rowsFilterMappingTblDao;
    
    @Override
    public String addDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        try {
            DataMediaTbl dataMediaTbl = dataMediaDto.toDataMediaTbl();
            dataMediaTblDao.insert(dataMediaTbl);
            return "add DataMedia success";
        } catch (IllegalArgumentException e) {
            logger.error("[[meta=dataMedia]] add DataMedia error, DataMediaDto:{}",dataMediaDto,e);
            return "illegal argument for DataMedia";
        }

    }

    @Override
    public String addDataMediaMapping(Long applierGroupId, Long dataMediaId) throws SQLException {
        RowsFilterMappingTbl rowsFilterMappingTbl = new RowsFilterMappingTbl();
        rowsFilterMappingTbl.setApplierGroupId(applierGroupId);
        rowsFilterMappingTbl.setDataMediaId(dataMediaId);
        int insert = rowsFilterMappingTblDao.insert(rowsFilterMappingTbl);
        return "add DataMediaMapping success";
    }
    
    
}
