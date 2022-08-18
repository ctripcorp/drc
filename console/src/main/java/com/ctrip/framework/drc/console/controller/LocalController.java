package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.LocalService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
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


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
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

    private DalUtils dalUtils = DalUtils.getInstance();
    
    @GetMapping("sql/integer/query")
    public ApiResult querySqlReturnInteger(
            @RequestParam String mha,
            @RequestParam String sql,
            @RequestParam int index) {
        try {
            logger.error("[[tag=getCreateTableStatements,mha={}]] sql:{}",mha,sql);
            Endpoint masterEndpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
            Integer result = MySqlUtils.getSqlResultInteger(masterEndpoint, sql, index);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("[[tag=localQuery,mha={}]] error in sql:{}",mha,sql,e);
            return ApiResult.getFailInstance(null);
        }
    }
    
    @GetMapping("createTblStmts/query")
    public ApiResult getCreateTableStatements(@RequestParam String mha,@RequestParam String regexFilter) {
        try {
            logger.info("[[tag=getCreateTableStatements,mha={}]] regexFilter:{}",mha,regexFilter);
            Endpoint masterEndpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(regexFilter);
            Map<String, String> result = MySqlUtils.getDefaultCreateTblStmts(masterEndpoint, aviatorRegexFilter);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("[[tag=getCreateTableStatements,mha={}]] error,regexFilter is:{} ",mha,regexFilter,e);
            return ApiResult.getFailInstance(null);
        }
    }
    
    @PostMapping("ddlHistory")
    public ApiResult insertDdlHistory(
            @RequestParam String mhaName,
            @RequestParam String ddl,
            @RequestParam int queryType,
            @RequestParam String schemaName,
            @RequestParam String tableName) {
        try {
            logger.info("[[tag=insertDdlHistory,mha={}]] insertDdlHistory:{}",mhaName,ddl);
            int effectRows = dalUtils.insertDdlHistory(mhaName, ddl, queryType, schemaName, tableName);
            return ApiResult.getSuccessInstance(effectRows);
        } catch (SQLException e) {
            logger.error("[[tag=insertDdlHistory,mha={}]] insertDdlHistory error:{}",mhaName,ddl);
            return ApiResult.getFailInstance(null);
        }
    }
}
