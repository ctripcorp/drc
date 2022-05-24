package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.console.dto.DataMediaDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.DataMediaVo;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
    
    @Autowired
    private RowsFilterTblDao rowsFilterTblDao;
    
    private DalUtils dalUtils = DalUtils.getInstance();
    
    @Override
    public String addDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        try {
            Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, dataMediaDto.getDataMediaSourceName());
            dataMediaDto.setDataMediaSourceId(mhaId);
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

    @Override
    public List<DataMediaVo> getDataMediaVos(Long applierGroupId,String srcMha) throws SQLException {
        List<DataMediaVo> dataMediaVos = Lists.newArrayList();
        if (applierGroupId == null) {
            return dataMediaVos;
        }
        Long srcMhaId = dalUtils.getId(TableEnum.MHA_TABLE, srcMha);
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByDataSourceId(srcMhaId,
                DataMediaTypeEnum.REGEX_LOGIC.getType(),
                BooleanEnum.FALSE.getCode());
        for (DataMediaTbl dataMediaTbl : dataMediaTbls) {
            dataMediaVos.add(new DataMediaVo(dataMediaTbl));
        }
        return dataMediaVos;
    }

    @Override
    public List<DataMediaVo> getAllDataMediaVos() throws SQLException {
        List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryAllByDeleted(BooleanEnum.FALSE.getCode());
        List<DataMediaVo> vos = Lists.newArrayList();
        for (DataMediaTbl dataMediaTbl : dataMediaTbls) {
            DataMediaVo dataMediaVo = new DataMediaVo(dataMediaTbl);
            MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryByPk(dataMediaTbl.getDataMediaSourceId());
            dataMediaVo.setDataMediaSourceName(mhaTbl.getMhaName());
            vos.add(dataMediaVo);
        }
        return vos;
    }

    @Override
    public List<RowsFilterMappingVo> getRowsFilterMappingVos(Long applierGroupId) throws SQLException {
        List<RowsFilterMappingVo> mappingVos = Lists.newArrayList();
        if (applierGroupId == null) {
            return mappingVos;
        }
        List<RowsFilterMappingTbl> rowsFilterMappingTbls = rowsFilterMappingTblDao.queryByApplierGroupIds(
                Lists.newArrayList(applierGroupId),
                BooleanEnum.FALSE.getCode());
       for (RowsFilterMappingTbl mapping : rowsFilterMappingTbls) {
           RowsFilterMappingVo rowsFilterMappingVo = new RowsFilterMappingVo();
           rowsFilterMappingVo.setId(mapping.getId());
           List<DataMediaTbl> dataMediaTbls = dataMediaTblDao.queryByIdsAndType(
                   Lists.newArrayList(mapping.getDataMediaId()),
                   DataMediaTypeEnum.REGEX_LOGIC.getType(),
                   BooleanEnum.FALSE.getCode());
           if (!CollectionUtils.isEmpty(dataMediaTbls)) {
               rowsFilterMappingVo.setDataMediaVo(new DataMediaVo(dataMediaTbls.get(0)));
           }
           RowsFilterTbl rowsFilterTbl = rowsFilterTblDao.queryById(mapping.getRowsFilterId(), BooleanEnum.FALSE.getCode());
           if (rowsFilterTbl != null) {
               rowsFilterMappingVo.setRowsFilterId(rowsFilterTbl.getId());
               rowsFilterMappingVo.setRowsFilterName(rowsFilterTbl.getName());
           }
           mappingVos.add(rowsFilterMappingVo);
       }
        return mappingVos;
    }

    @Override
    public String updateDataMedia(DataMediaDto dataMediaDto) throws SQLException {
        try {
            Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, dataMediaDto.getDataMediaSourceName());
            dataMediaDto.setDataMediaSourceId(mhaId);
            DataMediaTbl dataMediaTbl = dataMediaDto.toDataMediaTbl();
            int update = dataMediaTblDao.update(dataMediaTbl);
            return update == 1 ?  "update DataMedia success" : "update DataMedia fail";
        } catch (IllegalArgumentException e) {
            logger.error("[[meta=dataMedia]] update DataMedia error, DataMediaDto:{}",dataMediaDto,e);
            return "illegal argument for DataMedia";
        }
    }

    @Override
    public String deleteDataMedia(Long id) throws SQLException {
        try {
            if (id == null) {
                return "pk is null, cannot delete";
            }
            DataMediaTbl dataMediaTbl = new DataMediaTbl();
            dataMediaTbl.setId(id);
            dataMediaTbl.setDeleted(BooleanEnum.TRUE.getCode());
            int update = dataMediaTblDao.update(dataMediaTbl);
            return update == 1 ?  "delete DataMedia success" : "delete DataMedia fail";
        } catch (IllegalArgumentException e) {
            logger.error("[[meta=dataMedia]] delete DataMedia error, id is :{}",id,e);
            return "illegal argument for DataMedia";
        }
    }

}
