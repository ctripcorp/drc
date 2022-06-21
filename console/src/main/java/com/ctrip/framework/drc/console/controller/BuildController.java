package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.service.impl.*;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.*;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
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
    private MetaInfoServiceImpl metaInfoService;
    
    @Autowired
    private RowsFilterService rowsFilterService;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;
    
    private DalUtils dalUtils = DalUtils.getInstance();
    
    private ReplicatorGroupTblDao replicatorGroupTblDao = dalUtils.getReplicatorGroupTblDao();
    
    private ApplierGroupTblDao applierGroupTblDao = dalUtils.getApplierGroupTblDao();
    
    
    @PostMapping("simplexDrc/{srcMha}/{destMha}") 
    public ApiResult getOrBuildSimplexDrc(@PathVariable String srcMha, @PathVariable String destMha) {
        // if not exist add replicatorGroup[srcMha] and applierGroup[srcMha->destMha]
        try {
            Long srcMhaId = dalUtils.getId(TableEnum.MHA_TABLE, srcMha);
            Long destMhaId = dalUtils.getId(TableEnum.MHA_TABLE, destMha);
            String srcDc = metaInfoService.getDc(srcMha);
            String destDc = metaInfoService.getDc(destMha);
            Long srcReplicatorGroupId = replicatorGroupTblDao.upsertIfNotExist(srcMhaId);
            Long destApplierGroupId = applierGroupTblDao.upsertIfNotExist(srcReplicatorGroupId, destMhaId);
            SimplexDrcBuildVo simplexDrcBuildVo = new SimplexDrcBuildVo(srcMha,
                    destMha,
                    srcDc,
                    destDc,
                    destApplierGroupId,
                    srcReplicatorGroupId,
                    srcMhaId);
            return ApiResult.getSuccessInstance(simplexDrcBuildVo);
        } catch (SQLException e) {
            logger.error("sql error", e);
            return ApiResult.getFailInstance("sql error");
        }
    }


    @GetMapping("rowsFilterMappings/{applierGroupId}")
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
    public ApiResult inputRowsFilterConfig(@RequestBody RowsFilterConfigDto rowsFilterConfigDto) {
        logger.info("[[meta=rowsFilterConfig]] load rowsFilterConfigDto: {}", rowsFilterConfigDto);
        try {
            if (rowsFilterConfigDto.getId() != null) {
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


    @GetMapping("dataMedia/check")
    public ApiResult getMatchTable (@RequestParam String namespace,
                                    @RequestParam String name,
                                    @RequestParam String srcDc,
                                    @RequestParam String dataMediaSourceName,
                                    @RequestParam Integer type) {
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
            String url = dcDomain + "/api/drc/v1/local/dataMedia/check?" +
                    "namespace=" + namespace +
                    "&name=" + name +
                    "&srcDc=" + srcDc +
                    "&dataMediaSourceName=" + dataMediaSourceName +
                    "&type=" + type;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=matchTable]] get {}.{} from {} ",namespace,name,dataMediaSourceName);
                Endpoint mySqlEndpoint = dbClusterSourceProvider.getMasterEndpoint(dataMediaSourceName);
                if (mySqlEndpoint != null) {
                    AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." +  name);
                    List<MySqlUtils.TableSchemaName> tables = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
                    return ApiResult.getSuccessInstance(tables);
                }
                return ApiResult.getFailInstance("no machine find for " + dataMediaSourceName);
            } catch (Exception e) {
                logger.warn("[[tag=matchTable]] error when get {}.{} from {}",namespace,name,dataMediaSourceName,e);
                if (e  instanceof CompileExpressionErrorException) {
                    return ApiResult.getFailInstance("expression error");
                } else {
                    return ApiResult.getFailInstance("other error");
                }
            }
        }
    }

    @GetMapping("rowsFilter/commonColumns")
    public ApiResult getCommonColumnInDataMedias (
                                    @RequestParam String srcDc,
                                    @RequestParam String srcMha,
                                    @RequestParam String namespace,
                                    @RequestParam String name) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        if (publicCloudDc.contains(srcDc)) {
            String dcDomain = consoleDcInfos.get(srcDc);
            String url = dcDomain + "/api/drc/v1/local/rowsFilter/commonColumns?" +
                    "srcDc=" + srcDc +
                    "&srcMha=" + srcMha +
                    "&namespace=" + namespace +
                    "&name=" + name;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=commonColumns]] get columns {}\\.{} from {}",namespace,name,srcMha);
                Endpoint mySqlEndpoint = dbClusterSourceProvider.getMasterEndpoint(srcMha);
                if (mySqlEndpoint != null) {
                    AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." +  name);
                    Set<String> allCommonColumns = MySqlUtils.getAllCommonColumns(mySqlEndpoint, aviatorRegexFilter);
                    return ApiResult.getSuccessInstance(allCommonColumns);
                }
                return ApiResult.getFailInstance("no machine find for " + srcMha);
            } catch (Exception e) {
                logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error",namespace,name,srcMha,e);
                if (e  instanceof CompileExpressionErrorException) {
                    return ApiResult.getFailInstance("expression error");
                } else {
                    return ApiResult.getFailInstance("other error");
                }
            }
        }
    }


    @GetMapping("dataMedia/conflictCheck")
    public ApiResult getConflictTables(
            @RequestParam Long applierGroupId,
            @RequestParam Long dataMediaId,
            @RequestParam String srcDc,
            @RequestParam String mhaName,
            @RequestParam String namespace,
            @RequestParam String name) {

        try {
            List<String> logicalTables =
                    rowsFilterService.getLogicalTables(applierGroupId, dataMediaId, namespace, name, mhaName);
            Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
            Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
            if (publicCloudDc.contains(srcDc)) {
                String dcDomain = consoleDcInfos.get(srcDc);
                String url = dcDomain + "/api/drc/v1/local/dataMedia/conflictCheck?" +
                        "mhaName=" + applierGroupId +
                        "&logicalTables=" + String.join(",", logicalTables);
                return HttpUtils.get(url, ApiResult.class);
            } else {
                logger.info("[[tag=conflictTables]] get conflictTables {}\\.{} from {}", namespace, name, mhaName);
                List<String> conflictTables = rowsFilterService.getConflictTables(mhaName,logicalTables);
                return ApiResult.getSuccessInstance(conflictTables);
            }
        } catch (Exception e) {
            logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error", namespace, name, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }

    @GetMapping("dataMedia/columnCheck")
    public ApiResult getTablesWithoutColumn(
            @RequestParam String srcDc,
            @RequestParam String mhaName,
            @RequestParam String namespace,
            @RequestParam String name,
            @RequestParam String column) {
        Map<String, String> consoleDcInfos = consoleConfig.getConsoleDcInfos();
        Set<String> publicCloudDc = consoleConfig.getPublicCloudDc();
        column = column.toLowerCase();
        if (publicCloudDc.contains(srcDc)) {
            String dcDomain = consoleDcInfos.get(srcDc);
            String url = dcDomain + "/api/drc/v1/local/dataMedia/columnCheck?" +
                    "srcDc=" + srcDc +
                    "&mhaName=" + mhaName +
                    "&namespace=" + namespace +
                    "&name=" + name +
                    "&column=" + column;
            return HttpUtils.get(url, ApiResult.class);
        } else {
            try {
                logger.info("[[tag=columnsCheck]]  columnsCheck column:{} in {}\\.{} from {}", column, namespace, name, mhaName);
                return ApiResult.getSuccessInstance(rowsFilterService.getTablesWithoutColumn(column, namespace, name, mhaName));
            } catch (Exception e) {
                logger.error("[[tag=columnsCheck]]  columnsCheck column:{} in {}\\.{} from {}", column, namespace, name, mhaName, e);
                if (e instanceof CompileExpressionErrorException) {
                    return ApiResult.getFailInstance("expression error");
                } else {
                    return ApiResult.getFailInstance("other error");
                }
            }
        }
    }
    
    
}
