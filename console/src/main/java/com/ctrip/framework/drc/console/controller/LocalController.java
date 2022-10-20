package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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

    private DalUtils dalUtils = DalUtils.getInstance();
    
    @GetMapping("sql/integer/query")
    public ApiResult querySqlReturnInteger(
            @RequestParam String mha,
            @RequestParam String sql,
            @RequestParam int index) {
        try {
            logger.info("[[tag=querySqlReturnInteger,mha={}]] sql:{}",mha,sql);
            Endpoint masterEndpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
            Integer result = MySqlUtils.getSqlResultInteger(masterEndpoint, sql, index);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("[[tag=querySqlReturnInteger,mha={}]] error in sql:{}",mha,sql,e);
            return ApiResult.getFailInstance(null);
        }
    }
    
    @GetMapping("createTblStmts/query")
    public ApiResult getCreateTableStatements(@RequestParam String mha,@RequestParam String unionFilter) {
        try {
            logger.info("[[tag=getCreateTableStatements,mha={}]] regexFilter:{}",mha, unionFilter);
            Endpoint masterEndpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(unionFilter);
            Map<String, String> result = MySqlUtils.getDefaultCreateTblStmts(masterEndpoint, aviatorRegexFilter);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("[[tag=getCreateTableStatements,mha={}]] error,regexFilter is:{} ",mha, unionFilter,e);
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
