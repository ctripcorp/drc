package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

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
}
