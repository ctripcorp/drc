package com.ctrip.framework.drc.console.service.impl;


import static com.ctrip.framework.drc.core.service.utils.Constants.ESCAPE_CHARACTER_DOT_REGEX;

import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.console.service.OpenApiService;
import com.ctrip.framework.drc.console.vo.api.DrcDbInfo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.console.vo.api.MhaGroupFilterVo;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.ColumnsFilterConfig;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName OpenApiServiceImpl
 * @Author haodongPan
 * @Date 2022/1/6 20:26
 * @Version: $
 */
@Service
public class OpenApiServiceImpl implements OpenApiService {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());

    
    @Autowired private MetaGenerator metaGenerator;
    
    @Autowired private MetaInfoServiceImpl metaInfoService;
    
    @Autowired private MessengerService messengerService;
    
    @Autowired private DbClusterSourceProvider  dbClusterSourceProvider;
    
    
    
    
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
                String unionApplierFilter = metaInfoService.getUnionApplierFilter(mhaName, anotherMhaName);
                if (i++ == 0) {
                    mhaGroupFilterVo.setSrcMhaName(mhaName);
                    mhaGroupFilterVo.setSrcDc(dcName);
                    mhaGroupFilterVo.setSrcIpPort(machineTbl.getIp()+":"+machineTbl.getPort());
                    mhaGroupFilterVo.setSrcApplierFilter(unionApplierFilter);
                } else {
                    mhaGroupFilterVo.setDestMhaName(mhaName);
                    mhaGroupFilterVo.setDestDc(dcName);
                    mhaGroupFilterVo.setDestIpPort(machineTbl.getIp()+":"+machineTbl.getPort());
                    mhaGroupFilterVo.setDestApplierFilter(unionApplierFilter);
                }
            }
            allDrcMhaDbFilters.add(mhaGroupFilterVo);
        }

        return allDrcMhaDbFilters;
    }

    @Override
    public List<MessengerInfo> getAllMessengersInfo() throws SQLException {
        return messengerService.getAllMessengersInfo();
    }

    
    
    @Override
    public List<DrcDbInfo> getDrcDbInfos(String dbName) {
        List<DrcDbInfo> res = Lists.newArrayList();
        Drc drc = dbClusterSourceProvider.getDrc();
        for (Entry<String, Dc> dcInfo : drc.getDcs().entrySet()) {
            Dc dcMeta = dcInfo.getValue();
            String destRegion = dcMeta.getRegion();
            
            for (Entry<String, DbCluster> dbClusterInfo : dcMeta.getDbClusters().entrySet()) {
                DbCluster dbClusterMeta = dbClusterInfo.getValue();
                String destMha = dbClusterMeta.getMhaName();

                Set<String> srcMhaNames = Sets.newHashSet();
                List<Applier> appliers = dbClusterMeta.getAppliers();
                for (Applier applier : appliers) {
                    try {
                        String srcRegion = applier.getTargetRegion();
                        String srcMha = applier.getTargetMhaName();
                        if (srcMhaNames.contains(srcMha)) {
                            continue;
                        } else {
                            srcMhaNames.add(srcMha);
                        }
                        
                        String nameFilter = applier.getNameFilter();
                        if (StringUtils.isNotBlank(nameFilter)) {
                            Map<String, DrcDbInfo> dbInfoMap = Maps.newHashMap();
                            for (String fullTableName : nameFilter.split(",")) {
                                String[] split = fullTableName.split(ESCAPE_CHARACTER_DOT_REGEX);
                                String dbRegex = split[0];
                                String tableRegex = split[1];
                                if ("drcmonitordb".equalsIgnoreCase(dbRegex)) {
                                    continue;
                                }
                                if (StringUtils.isNotBlank(dbName) && !new AviatorRegexFilter(dbRegex).filter(dbName)) {
                                    continue;
                                }
                                if (dbInfoMap.containsKey(dbRegex)) {
                                    DrcDbInfo drcDbInfo = dbInfoMap.get(dbRegex);
                                    drcDbInfo.addRegexTable(tableRegex);
                                } else {
                                    DrcDbInfo drcDbInfo = new DrcDbInfo(dbRegex,tableRegex,srcMha,destMha,srcRegion,destRegion);
                                    dbInfoMap.put(dbRegex,drcDbInfo);
                                    res.add(drcDbInfo);
                                }
                            }
                            
                            processProperties(applier,dbInfoMap,destMha);
                        } else {
                            // all db
                            DrcDbInfo drcDbInfo = new DrcDbInfo(".*", ".*", srcMha, destMha, srcRegion, destRegion);
                            res.add(drcDbInfo);
                            
                            processProperties(applier,drcDbInfo,destMha);
                        }
                    } catch (Exception e) {
                        logger.warn("getDrcDbInfos fail in applier which destMha is :{}",destMha,e);
                    }
                }
            }
        }
        return res;
    }
    
    private void processProperties(Applier applier, Map<String, DrcDbInfo> dbInfoMap, String destMha) {
        if (StringUtils.isNotBlank(applier.getProperties())) {
            String properties = applier.getProperties();
            DataMediaConfig dataMediaConfig = JsonCodec.INSTANCE.decode(properties, DataMediaConfig.class);
            
            List<RowsFilterConfig> rowsFilters = dataMediaConfig.getRowsFilters();
            if (!CollectionUtils.isEmpty(rowsFilters)) {
                for (RowsFilterConfig rowsFilter : rowsFilters) {
                    String fullTableName = rowsFilter.getTables();
                    String[] split = fullTableName.split(ESCAPE_CHARACTER_DOT_REGEX);
                    String db = split[0];
                    if (dbInfoMap.containsKey(db)) {
                        DrcDbInfo drcDbInfo = dbInfoMap.get(db);
                        drcDbInfo.addRowsFilterConfig(rowsFilter);
                    } else {
                        logger.warn("no db found in nameFilter,destMha:{},rowsFilter:{}",destMha,rowsFilter);
                    }
                }
            }
            
            List<ColumnsFilterConfig> columnsFilters = dataMediaConfig.getColumnsFilters();
            if (!CollectionUtils.isEmpty(columnsFilters)) {
                for (ColumnsFilterConfig columnsFilter : columnsFilters) {
                    String fullTableName = columnsFilter.getTables();
                    String[] split = fullTableName.split(ESCAPE_CHARACTER_DOT_REGEX);
                    String db = split[0];
                    if (dbInfoMap.containsKey(db)) {
                        DrcDbInfo drcDbInfo = dbInfoMap.get(db);
                        drcDbInfo.addColumnFilterConfig(columnsFilter);
                    } else {
                        logger.warn("no db found in nameFilter,destMha:{},columnsFilter:{}",destMha,columnsFilter);
                    }
                }
            }
        }
        
    }

    private void processProperties(Applier applier, DrcDbInfo drcDbInfo, String destMha) {
        if (StringUtils.isNotBlank(applier.getProperties())) { 
            String properties = applier.getProperties();
            DataMediaConfig dataMediaConfig = JsonCodec.INSTANCE.decode(properties, DataMediaConfig.class);

            List<RowsFilterConfig> rowsFilters = dataMediaConfig.getRowsFilters();
            if (!CollectionUtils.isEmpty(rowsFilters)) {
                for (RowsFilterConfig rowsFilter : rowsFilters) {
                    drcDbInfo.addRowsFilterConfig(rowsFilter);
                }
            }

            List<ColumnsFilterConfig> columnsFilters = dataMediaConfig.getColumnsFilters();
            if (!CollectionUtils.isEmpty(columnsFilters)) {
                for (ColumnsFilterConfig columnsFilter : columnsFilters) {
                    drcDbInfo.addColumnFilterConfig(columnsFilter);
                }
            }
        }

    }
    
   

}
