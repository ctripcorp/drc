package com.ctrip.framework.drc.console.service.impl;


import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.OpenApiService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.vo.MhaDbFiltersVo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


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
    
    private static final String ALLMATCH = "*";
    
    @Autowired
    private MetaGenerator metaGenerator;
    
    @Override
    public List<MhaDbFiltersVo> getAllDrcMhaDbFilters() {

        ArrayList<MhaDbFiltersVo> allDrcMhaDbFilters = Lists.newArrayList();
        List<MhaTbl> mhaTbls = metaGenerator.getMhaTbls();
        List<ApplierGroupTbl> applierGroupTbls = metaGenerator.getApplierGroupTbls();
        List<MachineTbl> machineTbls = metaGenerator.getMachineTbls();
        List<DcTbl> dcTbls = metaGenerator.getDcTbls();

        for (MhaTbl mhaTbl : mhaTbls) {
            String mhaName = mhaTbl.getMhaName();
            Long mhaTblId = mhaTbl.getId();
            List<ApplierGroupTbl> mhaAllApplierGroups = applierGroupTbls.stream().filter(p -> p.getMhaId().equals(mhaTblId)).collect(Collectors.toList());
            List<String> filters = Lists.newArrayList();
            for (ApplierGroupTbl applierGroupTbl : mhaAllApplierGroups) {
                String filter;
                String nameFilter = applierGroupTbl.getNameFilter();
                String includedDbs = applierGroupTbl.getIncludedDbs();
                filter = nameFilter != null ? nameFilter : (includedDbs != null ? includedDbs : ALLMATCH);
                filters.add(filter);
                if (filter.equals(ALLMATCH)) {
                    break;
                }
            }
            String dcName = dcTbls.stream().filter(p -> p.getId().equals(mhaTbl.getDcId())).findFirst().get().getDcName();
            MachineTbl machineTbl = machineTbls.stream().filter(p -> p.getMhaId().equals(mhaTblId) && p.getMaster().equals(BooleanEnum.TRUE.getCode())).findFirst().get();
            allDrcMhaDbFilters.add(new MhaDbFiltersVo(mhaName,dcName, machineTbl.getIp()+":"+machineTbl.getPort(),filters));
        }
        
        return allDrcMhaDbFilters;
    }
}
