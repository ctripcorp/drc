package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DataMediaTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.impl.*;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.*;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.filter.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName BuildController
 * @Author haodongPan
 * @Date 2022/5/10 17:26
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/build/")
public class BuildController {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private TransferServiceImpl transferService;

    @Autowired
    private DrcBuildServiceImpl drcBuildService;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Autowired
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Autowired
    private DbClusterSourceProvider sourceProvider;

    @Autowired
    private RowsFilterService rowsFilterService;
    
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    @Autowired
    private DataMediaTblDao dataMediaTblDao;
    
    private DalUtils dalUtils = DalUtils.getInstance();
    
    
    @GetMapping("applierGroups/{srcMha}/{destMha}")
    public ApiResult getApplierGroupVos(@PathVariable String srcMha,
                                      @PathVariable String destMha) {
        try {
            String srcDc = metaInfoService.getDc(srcMha);
            String destDc = metaInfoService.getDc(destMha);
            ArrayList<ApplierGroupVo>  applierGroupVos = Lists.newArrayList();
            ApplierGroupTbl applierGroupTbl0 = metaInfoService.getApplierGroupTbl(destMha, srcMha);
            if (applierGroupTbl0 != null) {
                applierGroupVos.add(new ApplierGroupVo(srcMha,destMha,srcDc,destDc,applierGroupTbl0.getId()));
            }
            ApplierGroupTbl applierGroupTbl1 = metaInfoService.getApplierGroupTbl(srcMha, destMha);
            if (applierGroupTbl1 != null ) {
                applierGroupVos.add(new ApplierGroupVo(destMha,srcMha, destDc, srcDc,applierGroupTbl1.getId()));
            }
            return ApiResult.getSuccessInstance(applierGroupVos);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }
    
    @PostMapping("simplexDrc/{srcMha}/{destMha}") 
    public ApiResult getOrBuildSimplexDrc(@PathVariable String srcMha, @PathVariable String destMha) {
        // if not exist add replicatorGroup[srcMha] and applierGroup[srcMha->destMha]
        try {
            Long srcMhaId = dalUtils.getId(TableEnum.MHA_TABLE, srcMha);
            Long destMhaId = dalUtils.getId(TableEnum.MHA_TABLE, destMha);
            String srcDc = metaInfoService.getDc(srcMha);
            String destDc = metaInfoService.getDc(destMha);
            Long srcReplicatorGroupId = dalUtils.getReplicatorGroupTblDao().upsertIfNotExist(srcMhaId);
            Long destApplierGroupId = dalUtils.getApplierGroupTblDao().upsertIfNotExist(srcReplicatorGroupId, destMhaId);
            SimplexDrcBuildVo simplexDrcBuildVo = new SimplexDrcBuildVo(srcMha,
                    destMha,
                    srcDc,
                    destDc,
                    destApplierGroupId,
                    srcReplicatorGroupId);
            return ApiResult.getSuccessInstance(simplexDrcBuildVo);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }

    @GetMapping("RowsFilterMappings/{applierGroupId}")
    public ApiResult getRowsFilterMappingVos (@PathVariable String applierGroupId) {
        Long id = Long.valueOf(applierGroupId);
        try {
            List<RowsFilterMappingVo> dataMediaVos = rowsFilterService.getRowsFilterMappingVos(id);
            return ApiResult.getSuccessInstance(dataMediaVos);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }
    
    @PostMapping("rowsFilterConfig")
    public ApiResult inputRowsFilter(@RequestBody RowsFilterConfigDto rowsFilterConfigDto) {
        logger.info("[[meta=rowsFilterConfig]] load rowsFilterConfigDto: {}", rowsFilterConfigDto);
        try {
            if (rowsFilterConfigDto.getId() != null) {
                // todo 校验是否存在一张表被多个表达式匹配
                return ApiResult.getSuccessInstance(rowsFilterService.updateRowsFilterConfig(rowsFilterConfigDto));
            } else {
                return ApiResult.getSuccessInstance(rowsFilterService.addRowsFilterConfig(rowsFilterConfigDto));
            }
        } catch (SQLException e) {
            logger.error("[[meta=rowsFilterConfig]] load rowsFilterConfig fail with {} ", rowsFilterConfigDto, e);
            return ApiResult.getFailInstance("sql error in add or update rowsFilterConfig");
        }
    }

    @DeleteMapping("rowsFilterConfig/{rowsFilterMappingId}")
    public ApiResult deleteRowsFilterConfig(@PathVariable Long rowsFilterMappingId) {
        logger.info("[[meta=rowsFilterConfig]] delete rowsFilterConfig id: {}", rowsFilterMappingId);
        try {
            return ApiResult.getSuccessInstance(rowsFilterService.deleteRowsFilterConfig(rowsFilterMappingId));
        } catch (SQLException e) {
            logger.error("[[meta=rowsFilterConfig]] delete rowsFilterConfig fail with {} ", rowsFilterMappingId, e);
            return ApiResult.getFailInstance("sql error in delete rowsFilterConfig");
        }
    }
    
    
    @GetMapping("dataMedia/check/{namespace}/{name}/{srcDc}/{dataMediaSourceName}/{type}")
    public ApiResult getMatchTable (@PathVariable String namespace,
                                    @PathVariable String name,
                                    @PathVariable String srcDc,
                                    @PathVariable String dataMediaSourceName,
                                    @PathVariable Integer type) {
        try {
            if (StringUtils.isEmpty(srcDc)) {
                MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryByMhaName(dataMediaSourceName, BooleanEnum.FALSE.getCode());
                srcDc = dalUtils.getDcNameByDcId(mhaTbl.getDcId());
            }
        } catch (SQLException e) {
            logger.warn("[[tag=matchTable]] error when get {}.{} from {}",namespace,name,dataMediaSourceName,e);
            return ApiResult.getFailInstance("sql error");
        }
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if (publicCloudDc.contains(srcDc)) {
            String dcDomain = consoleDcInfos.get(srcDc);
            String url = dcDomain + "/api/drc/v1/mha/dataMedia/check/" +
                    namespace + "/" +
                    name + "/"+
                    srcDc + "/" +
                    dataMediaSourceName + "/"  + 
                    type;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=matchTable]] get {}.{} from {} ",namespace,name,dataMediaSourceName);
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroupForMha(dataMediaSourceName);
                MachineTbl machineTbl = metaInfoService.getMachineTbls(dataMediaSourceName).stream()
                        .filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
                if (machineTbl != null) {
                    MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(
                            machineTbl.getIp(),
                            machineTbl.getPort(),
                            mhaGroup.getMonitorUser(),
                            mhaGroup.getMonitorPassword(),
                            true);
                    AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." +  name);
                    List<MySqlUtils.TableSchemaName> tables = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
                    return ApiResult.getSuccessInstance(tables);
                }
                return ApiResult.getFailInstance("no machine find for " + dataMediaSourceName);
            } catch (Exception e) {
                logger.warn("[[tag=matchTable]] error when get {}.{} from {}",namespace,name,dataMediaSourceName,e);
                if (e instanceof SQLException) {
                    return ApiResult.getFailInstance("sql error");
                } else if (e  instanceof CompileExpressionErrorException) {
                    return ApiResult.getFailInstance("expression error");
                } else {
                    return ApiResult.getFailInstance("other error");
                }
            }
        }
    }

    @GetMapping("rowsFilter/commonColumns/{srcDc}/{srcMha}/{namespace}/{name}")
    public ApiResult getCommonColumnInDataMedias (
                                    @PathVariable String srcDc,
                                    @PathVariable String srcMha,
                                    @PathVariable String namespace,
                                    @PathVariable String name) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if (publicCloudDc.contains(srcDc)) {
            String dcDomain = consoleDcInfos.get(srcDc);
            String url = dcDomain + "/api/drc/v1/mha/rowsFilter/commonColumns/" +
                    srcDc + "/" +
                    srcMha + "/" +
                    namespace + "/" +
                    name;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=commonColumns]] get columns {}\\.{} from {}",namespace,name,srcMha);
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroupForMha(srcMha);
                MachineTbl machineTbl = metaInfoService.getMachineTbls(srcMha).stream()
                        .filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
                if (machineTbl != null) {
                    MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(
                            machineTbl.getIp(),
                            machineTbl.getPort(), 
                            mhaGroup.getMonitorUser(), 
                            mhaGroup.getMonitorPassword(), 
                            true);
                    AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." +  name);
                    Set<String> allCommonColumns = MySqlUtils.getAllCommonColumns(mySqlEndpoint, aviatorRegexFilter);
                    return ApiResult.getSuccessInstance(allCommonColumns);
                }
                return ApiResult.getFailInstance("no machine find for " + srcMha);
            } catch (Exception e) {
                logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error",namespace,name,srcMha,e);
                if (e instanceof SQLException) {
                    return ApiResult.getFailInstance("sql error");
                } else if (e  instanceof CompileExpressionErrorException) {
                    return ApiResult.getFailInstance("expression error");
                } else {
                    return ApiResult.getFailInstance("other error");
                }
            }
        }
    }
    
    
}
