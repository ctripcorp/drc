package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.RowsFilterConfigDto;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.*;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.google.common.collect.Lists;
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


    @Autowired private RowsFilterService rowsFilterService;

    @Autowired private DrcBuildService drcBuildService;

    @PostMapping("simplexDrc")
    public ApiResult getOrBuildSimplexDrc(@RequestParam(value = "srcMha", defaultValue = "") String srcMha,
                                          @RequestParam(value = "destMha", defaultValue = "") String destMha) {
        try {
            return drcBuildService.getOrBuildSimplexDrc(srcMha,destMha);
        } catch (SQLException e) {
            logger.error("sql error", e);
            return ApiResult.getFailInstance("sql error");
        }
    }

    @PostMapping("replicatorIps/check")
    public ApiResult preCheckBeforeBuildDrc(@RequestBody MessengerMetaDto dto) {
        logger.info("[meta] preCheck meta config for  {}", dto);
        try {
            return ApiResult.getSuccessInstance(drcBuildService.preCheckBeReplicatorIps(dto));
        } catch (SQLException e) {
            logger.error("[meta] preCheck meta config for {}", dto,e);
        }
        return ApiResult.getFailInstance(null);
    }


    @PostMapping("config")
    public ApiResult submitConfig(@RequestBody MessengerMetaDto dto) {
        logger.info("[meta] submit meta config for {}", dto);
        try {
            return ApiResult.getSuccessInstance(drcBuildService.submitConfig(dto));
        } catch (Exception e) {
            logger.error("[meta] submit meta config for {}", dto, e);
        }
        return ApiResult.getFailInstance(null);
    }


    @GetMapping("rowsFilterMappings/{applierGroupId}")
    public ApiResult getRowsFilterMappingVos (@PathVariable String applierGroupId) {
        Long id = Long.valueOf(applierGroupId);
        try {
            List<RowsFilterMappingVo> dataMediaVos = rowsFilterService.getRowsFilterMappingVos(id, ConsumeType.Applier.getCode());
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
            return ApiResult.getSuccessInstance(Lists.newArrayList(commonColumns));
        } catch (Exception e) {
            logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error",namespace,name, mhaName,e);
            if (e instanceof CompileExpressionErrorException) {
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
            @RequestParam String name,
            @RequestParam int applierType) {

        try {
            List<String> logicalTables =
                    rowsFilterService.getLogicalTables(applierGroupId, applierType,dataMediaId, namespace, name, mhaName);
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

    @GetMapping("dataMedia/conflictCheck/remote")
    public ApiResult getConflictTables(
            @RequestParam String mhaName,
            @RequestParam String logicalTables) {
        try {
            logger.info("[[tag=conflictTables]] get conflictTables from {} in mha: {}",logicalTables, mhaName);
            List<String> conflictTables =
                    rowsFilterService.getConflictTables(mhaName, logicalTables);
            return ApiResult.getSuccessInstance(conflictTables);
        } catch (Exception e) {
            logger.warn("[[tag=conflictTables]] get conflictTables error from {} in mha: {}",logicalTables, mhaName,e);
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
