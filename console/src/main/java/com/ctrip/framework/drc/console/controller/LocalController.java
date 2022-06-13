package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.healthcheck.task.ExecutedGtidQueryTask;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

/**
 * @ClassName LocalController
 * @Author haodongPan
 * @Date 2022/5/27 14:59
 * @Version: $
 */
@RestController
@RequestMapping("/api/drc/v1/local/")
public class LocalController {

    public static final Logger logger = LoggerFactory.getLogger(LocalController.class);

    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Autowired
    private RowsFilterService rowsFilterService;

    @GetMapping("dataMedia/check")
    public ApiResult getMatchTable(@RequestParam String namespace,
                                   @RequestParam String name,
                                   @RequestParam String srcDc,
                                   @RequestParam String dataMediaSourceName,
                                   @RequestParam Integer type) {
        try {
            logger.info("[[tag=matchTable]] get {}.{} from {} ", namespace, name, dataMediaSourceName);
            Endpoint mySqlEndpoint = dbClusterSourceProvider.getMasterEndpoint(dataMediaSourceName);
            if (mySqlEndpoint != null) {
                AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
                List<MySqlUtils.TableSchemaName> tables = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
                return ApiResult.getSuccessInstance(tables);
            }
            return ApiResult.getFailInstance("no machine find for " + dataMediaSourceName);
        } catch (Exception e) {
            logger.warn("[[tag=matchTable]] error when get {}.{} from {}", namespace, name, dataMediaSourceName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
        
    }

    @GetMapping("rowsFilter/commonColumns")
    public ApiResult getCommonColumnInDataMedias (
            @RequestParam String srcDc,
            @RequestParam String srcMha,
            @RequestParam String namespace,
            @RequestParam String name) {
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

    @GetMapping("dataMedia/conflictCheck")
    public ApiResult getConflictTables(
            @RequestParam String mhaName,
            @RequestParam String logicalTables) {
        try {
            logger.info("[[tag=conflictTables]] get conflictTables from {} in mha: {}",logicalTables, mhaName);
            List<String> conflictTables =
                    rowsFilterService.getConflictTables(mhaName,Lists.newArrayList(logicalTables.split(",")));
            return ApiResult.getSuccessInstance(conflictTables);
        } catch (Exception e) {
            logger.warn("[[tag=conflictTables]] get conflictTables error from {} in mha: {}",logicalTables, mhaName);
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


    @GetMapping("gtid")
    public ApiResult getRealExecutedGtid(
            @RequestParam String mha,
            @RequestParam String ip,
            @RequestParam Integer port,
            @RequestParam String user,
            @RequestParam String psw) {
        logger.info("[[tag=gtidQuery]] getRealExecutedGtid fail from {}:{} in mha:{}",ip,port,mha);
        MySqlEndpoint endpoint = new MySqlEndpoint(ip, port, user, psw, true);
        String gtid = MySqlUtils.getUnionExecutedGtid(endpoint);
        if (StringUtils.isEmpty(gtid)) {
            logger.warn("[[tag=gtidQuery]] getRealExecutedGtid fail from {} in mha:{}",endpoint.getSocketAddress(),mha);
            return ApiResult.getFailInstance(null);
        }
        return ApiResult.getSuccessInstance(gtid);
        
    }

}
