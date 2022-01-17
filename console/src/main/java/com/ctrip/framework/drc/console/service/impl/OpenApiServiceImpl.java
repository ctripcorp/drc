package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.service.OpenApiService;
import com.ctrip.framework.drc.console.vo.MhaGroupFilterVo;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName OpenApiServiceImpl
 * @Author haodongPan
 * @Date 2022/1/6 20:26
 * @Version: $
 */
@Service
public class OpenApiServiceImpl implements OpenApiService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private static final String ALLMATCH = "*";
    private static final int SET_GTID_MODE = 0;
    private static final int TRANSACTION_TABLE_MODE = 1;
    
    @Autowired
    private MetaGenerator metaGenerator;
    
    @Autowired
    private MetaInfoServiceImpl metaInfoService;
    
    @Override
    public List<MhaGroupFilterVo> getAllDrcMhaDbFilters() throws SQLException {
        
        ArrayList<MhaGroupFilterVo> allDrcMhaDbFilters = Lists.newArrayList();
        List<MhaGroupTbl> mhaGroupTbls = metaGenerator.getMhaGroupTbls().stream().filter(p -> p.getDrcEstablishStatus().equals(EstablishStatusEnum.ESTABLISHED.getCode())).collect(Collectors.toList());
        List<GroupMappingTbl> groupMappingTbls = metaGenerator.getGroupMappingTbls();
        List<MhaTbl> mhaTbls = metaGenerator.getMhaTbls();
        List<MachineTbl> machineTbls = metaGenerator.getMachineTbls();
        List<DcTbl> dcTbls = metaGenerator.getDcTbls();

        for (MhaGroupTbl mhaGroupTbl : mhaGroupTbls) {
            MhaGroupFilterVo mhaGroupFilterVo = new MhaGroupFilterVo();
            
            Long mhaGroupTblId = mhaGroupTbl.getId();
            List<MhaTbl> twoMha = Lists.newArrayList();
            groupMappingTbls.stream().filter(p -> p.getMhaGroupId().equals(mhaGroupTblId)).forEach(groupMappingTbl -> {
                MhaTbl mhaTbl = mhaTbls.stream().filter(p -> p.getId().equals(groupMappingTbl.getMhaId())).findFirst().get();
                twoMha.add(mhaTbl);
            });
            if (twoMha.size() != 2) {
                logger.warn("get twoMhaListSize error group is {}",mhaGroupTbl.getId() );
            }
            
            int i = 0;
            for (MhaTbl mhaTbl : twoMha) {
                String anotherMhaName = twoMha.stream().filter(p -> !p.getId().equals(mhaTbl.getId())).findFirst().get().getMhaName();
                String mhaName = mhaTbl.getMhaName();
                Long mhaTblId = mhaTbl.getId();
                String dcName = dcTbls.stream().filter(p -> p.getId().equals(mhaTbl.getDcId())).findFirst().get().getDcName();
                MachineTbl machineTbl = machineTbls.stream().filter(p -> p.getMhaId().equals(mhaTblId) && p.getMaster().equals(BooleanEnum.TRUE.getCode())).findFirst().get();

                String includedDbs = metaInfoService.getIncludedDbs(mhaName, anotherMhaName);
                String nameFilter = metaInfoService.getNameFilter(mhaName, anotherMhaName);
                if (i++ == 0) {
                    mhaGroupFilterVo.setSrcMhaName(mhaName);
                    mhaGroupFilterVo.setSrcDc(dcName);
                    mhaGroupFilterVo.setSrcIpPort(machineTbl.getIp()+":"+machineTbl.getPort());
                    String srcApplierFilter = ALLMATCH;
                    if (StringUtils.isNotBlank(nameFilter)) {
                        srcApplierFilter = nameFilter;
                    } else if (StringUtils.isNotBlank(includedDbs)) {
                        srcApplierFilter = includedDbs;
                    } else {
                        logger.info("srcApplierFilter find no filter,use allMatch,mhaId is {}",mhaTblId);
                    }
                    mhaGroupFilterVo.setSrcApplierFilter(srcApplierFilter);
                } else {
                    mhaGroupFilterVo.setDestMhaName(mhaName);
                    mhaGroupFilterVo.setDestDc(dcName);
                    mhaGroupFilterVo.setDestIpPort(machineTbl.getIp()+":"+machineTbl.getPort());
                    String destApplierFilter = ALLMATCH;
                    if (StringUtils.isNotBlank(nameFilter)) {
                        destApplierFilter = nameFilter;
                    } else if (StringUtils.isNotBlank(includedDbs)) {
                        destApplierFilter = includedDbs;
                    } else {
                        logger.info("destApplierFilter find no filter,use allMatch,mhaId is {}",mhaTblId);
                    }
                    mhaGroupFilterVo.setDestApplierFilter(destApplierFilter);
                }
            }
            allDrcMhaDbFilters.add(mhaGroupFilterVo);
        }
        
        return allDrcMhaDbFilters;
    }
}
