package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.ApplierGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.DrcBuildService;
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
    private DrcBuildService drcBuildService;
    
    @Autowired
    private DefaultConsoleConfig consoleConfig;

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;
    
    private  DalUtils dalUtils = DalUtils.getInstance();
    
    private  ReplicatorGroupTblDao replicatorGroupTblDao = dalUtils.getReplicatorGroupTblDao();
    
    private  ApplierGroupTblDao applierGroupTblDao = dalUtils.getApplierGroupTblDao();
    
    
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
            SimplexDrcBuildVo simplexDrcBuildVo = new SimplexDrcBuildVo(
                    srcMha,
                    destMha,
                    srcDc,
                    destDc,
                    destApplierGroupId,
                    srcReplicatorGroupId,
                    srcMhaId
            );
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
                                    @RequestParam String mhaName,
                                    @RequestParam Integer type) {
        try {
            List<MySqlUtils.TableSchemaName> matchTables = drcBuildService.getMatchTable(namespace, name, mhaName, type);
            return ApiResult.getSuccessInstance(matchTables);
        } catch (Exception e) {
            logger.warn("[[tag=matchTable]] error when get {}.{} from {}",namespace,name, mhaName,e);
            if (e  instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else if (e instanceof IllegalArgumentException) {
                return ApiResult.getFailInstance("no machine find for " + mhaName);
            } else {
                return ApiResult.getFailInstance("other error see log");
            }
        }
    }

    @GetMapping("rowsFilter/commonColumns")
    public ApiResult getCommonColumnInDataMedias (
                                    @RequestParam String mhaName,
                                    @RequestParam String namespace,
                                    @RequestParam String name) {
        try {
            Set<String> commonColumns = drcBuildService.getCommonColumnInDataMedias(mhaName, namespace, name);
            return ApiResult.getSuccessInstance(commonColumns);
        } catch (Exception e) {
            logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error",namespace,name, mhaName,e);
            if (e  instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else if (e instanceof IllegalArgumentException) {
                return ApiResult.getFailInstance("no machine find for " + mhaName);
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }


    @GetMapping("dataMedia/conflictCheck")
    public ApiResult getConflictTables(
            @RequestParam Long applierGroupId,
            @RequestParam Long dataMediaId,
            @RequestParam String mhaName,
            @RequestParam String namespace,
            @RequestParam String name) {

        try {
            List<String> logicalTables =
                    rowsFilterService.getLogicalTables(applierGroupId, dataMediaId, namespace, name, mhaName);
            logger.info("[[tag=conflictTables]] get conflictTables {}\\.{} from {}", namespace, name, mhaName);
            List<String> conflictTables = rowsFilterService.getConflictTables(mhaName,String.join(",",logicalTables));
            return ApiResult.getSuccessInstance(conflictTables);
        } catch (Exception e) {
            logger.warn("[[tag=conflictTables]] get columns {}\\.{} from {} error", namespace, name, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
                
            }
        }
    }

    @GetMapping("dataMedia/columnCheck")
    public ApiResult getTablesWithoutColumn(
            @RequestParam String mhaName,
            @RequestParam String namespace,
            @RequestParam String name,
            @RequestParam String column) {
        try {
            logger.info("[[tag=columnsCheck]]  columnsCheck column:{} in {}\\.{} from {}", column, namespace, name, mhaName);
            return ApiResult.getSuccessInstance(rowsFilterService.getTablesWithoutColumn(column, namespace, name, mhaName));
        } catch (Exception e) {
            logger.warn("[[tag=columnsCheck]]  columnsCheck column:{} in {}\\.{} from {}", column, namespace, name, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }
    
    @GetMapping("preCheckMySqlConfig") 
    public ApiResult preCheckConfig(@RequestParam String mha) {
        try {
            logger.info("[[tag=preCheck,mha={}]] preCheckConfig ",mha);
            Map<String, Object> resMap = drcBuildService.preCheckMySqlConfig(mha);
            return ApiResult.getSuccessInstance(resMap);
        } catch (Exception e) {
            logger.warn("[[tag=preCheck,mha={}]] error in preCheckMySqlConfig",mha,e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("preCheckMySqlTables")
    public ApiResult preCheckTables(@RequestParam String mha,@RequestParam String nameFilter) {
        try {
            logger.info("[[tag=preCheck,mha={}]] preCheckTables with nameFilter:{}",mha,nameFilter);
            List<TableCheckVo> checkVos = drcBuildService.preCheckMySqlTables(mha, nameFilter);
            return ApiResult.getSuccessInstance(checkVos);
        } catch (Exception e) {
            logger.warn("[[tag=preCheck,mha={}]]  in preCheckMySqlTables",mha,e);
            return ApiResult.getFailInstance(null);
        }
    }
    
}
