package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.impl.*;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.ApplierGroupVo;
import com.ctrip.framework.drc.console.vo.DataMediaVo;
import com.ctrip.framework.drc.console.vo.RowsFilterMappingVo;
import com.ctrip.framework.drc.console.vo.RowsFilterVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.filter.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    private DataMediaService dataMediaService;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
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
    public ApiResult buildSimplexDrc(@PathVariable String srcMha, @PathVariable String destMha) {
        // if not add replicatorGroup[srcMha] and applierGroup[srcMha->destMha]
        try {
            Long srcMhaId = dalUtils.getId(TableEnum.MHA_TABLE, srcMha);
            Long destMhaId = dalUtils.getId(TableEnum.MHA_TABLE, destMha);
            Long srcReplicatorGroupId = dalUtils.updateOrCreateRGroup(srcMhaId);
            Long destApplierGroupId = dalUtils.updateOrCreateAGroup(srcReplicatorGroupId, destMhaId,
                    null, 1, null, null, null);
            return ApiResult.getSuccessInstance(destApplierGroupId);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
        
        
    }

    @GetMapping("RowsFilterMappings/{applierGroupId}")
    public ApiResult getRowsFilterMappingVos (@PathVariable String applierGroupId) {
        Long id = Long.valueOf(applierGroupId);
        try {
            List<RowsFilterMappingVo> dataMediaVos = dataMediaService.getRowsFilterMappingVos(id);
            return ApiResult.getSuccessInstance(dataMediaVos);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }
    
    @GetMapping("dataMedias/{applierGroupId}/{srcMha}")
    public ApiResult getDataMediaVos (@PathVariable String applierGroupId,@PathVariable String srcMha) {
        Long id = Long.valueOf(applierGroupId);
        try {
            List<DataMediaVo> dataMediaVos = dataMediaService.getDataMediaVos(id,srcMha);
            dataMediaVos.forEach(vo -> vo.setDataMediaSourceName(srcMha));
            return ApiResult.getSuccessInstance(dataMediaVos);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }


    @GetMapping("rowsFilters")
    public ApiResult getAllRowsFilterVos () {
        try {
            List<RowsFilterVo> allRowsFilterVos = rowsFilterService.getAllRowsFilterVos();
            return ApiResult.getSuccessInstance(allRowsFilterVos);
        } catch (SQLException e) {
            logger.error("sql error",e);
            return ApiResult.getFailInstance("sql error");
        }
    }
    
    
    @GetMapping("dataMedia/check/{namespace}/{name}/{srcDc}/{dataMediaSourceName}/{type}")
    public ApiResult getMatchTable (@PathVariable String namespace,
                                    @PathVariable String name,
                                    @PathVariable String srcDc,
                                    @PathVariable String dataMediaSourceName,
                                    @PathVariable Integer type) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if (publicCloudDc.contains(srcDc)) {
            String dcDomain = consoleDcInfos.get(srcDc);
            String url = dcDomain + "/api/drc/v1/mha/tables/" +
                    namespace + "/" +
                    name + "/"+
                    srcDc + "/" +
                    dataMediaSourceName + "/"  + type;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=matchTable]] get {}.{} from {} ",namespace,name,dataMediaSourceName);
                MhaGroupTbl mhaGroup = metaInfoService.getMhaGroupForMha(dataMediaSourceName);
                MachineTbl machineTbl = metaInfoService.getMachineTbls(dataMediaSourceName).stream()
                        .filter(p -> BooleanEnum.TRUE.getCode().equals(p.getMaster())).findFirst().orElse(null);
                if (machineTbl != null) {
                    MySqlEndpoint mySqlEndpoint = new MySqlEndpoint(machineTbl.getIp(), machineTbl.getPort(), mhaGroup.getReadUser(), mhaGroup.getReadPassword(), true);
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
    
    
}
