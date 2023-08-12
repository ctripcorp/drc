package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/8/11 16:10
 */
@RestController
@RequestMapping("/api/drc/v2/mysql/")
public class MysqlController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MysqlServiceV2 mysqlServiceV2;

    @GetMapping("preCheckMySqlConfig")
    public ApiResult preCheckConfig(@RequestParam String mha) {
        try {
            logger.info("[[tag=preCheck,mha={}]] preCheckConfig ", mha);
            Map<String, Object> resMap = mysqlServiceV2.preCheckMySqlConfig(mha);
            return ApiResult.getSuccessInstance(resMap);
        } catch (Exception e) {
            logger.warn("[[tag=preCheck,mha={}]] error in preCheckMySqlConfig", mha, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("preCheckMySqlTables")
    public ApiResult preCheckTables(@RequestParam String mha, @RequestParam String nameFilter) {
        try {
            logger.info("[[tag=preCheck,mha={}]] preCheckTables with nameFilter:{}", mha, nameFilter);
            List<TableCheckVo> checkVos = mysqlServiceV2.preCheckMySqlTables(mha, nameFilter);
            return ApiResult.getSuccessInstance(checkVos);
        } catch (Exception e) {
            logger.warn("[[tag=preCheck,mha={}]]  in preCheckMySqlTables", mha, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("queryDbs")
    public ApiResult<List<String>> queryDbsWithNameFilter(@RequestParam String mha, @RequestParam String nameFilter) {
        try {
            logger.info("[[tag=queryDbs,mha={}]] queryDbsWithNameFilter with nameFilter:{}", mha, nameFilter);
            List<String> dbs = mysqlServiceV2.queryDbsWithNameFilter(mha, nameFilter);
            return ApiResult.getSuccessInstance(dbs);
        } catch (Exception e) {
            logger.warn("[[tag=queryDbs,mha={}]]  in queryDbsWithNameFilter", mha, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("queryTables")
    public ApiResult<List<String>> queryTablesWithNameFilter(@RequestParam String mha, @RequestParam String nameFilter) {
        try {
            logger.info("[[tag=queryTables,mha={}]] queryTablesWithNameFilter with nameFilter:{}", mha, nameFilter);
            List<String> dbs = mysqlServiceV2.queryTablesWithNameFilter(mha, nameFilter);
            return ApiResult.getSuccessInstance(dbs);
        } catch (Exception e) {
            logger.warn("[[tag=queryTables,mha={}]]  in queryTablesWithNameFilter", mha, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("commonColumns")
    public ApiResult getCommonColumn(@RequestParam String mhaName, @RequestParam String namespace, @RequestParam String name) {
        try {
            Set<String> commonColumns = mysqlServiceV2.getCommonColumnIn(mhaName, namespace, name);
            return ApiResult.getSuccessInstance(Lists.newArrayList(commonColumns));
        } catch (Exception e) {
            logger.warn("[[tag=commonColumns]] get columns {}\\.{} from {} error", namespace, name, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else if (e instanceof IllegalArgumentException) {
                return ApiResult.getFailInstance("no machine find for " + mhaName);
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }

    @GetMapping("columnCheck")
    public ApiResult columnCheck(@RequestParam String mhaName, @RequestParam String namespace, @RequestParam String name, @RequestParam String column) {
        try {
            Set<String> commonColumns = mysqlServiceV2.getTablesWithoutColumn(column, namespace, name, mhaName);
            return ApiResult.getSuccessInstance(Lists.newArrayList(commonColumns));
        } catch (Exception e) {
            logger.warn("[[tag=columnsCheck]]  columnsCheck column:{} in {}\\.{} from {}", column, namespace, name, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else {
                return ApiResult.getFailInstance("other error");
            }
        }
    }
}
