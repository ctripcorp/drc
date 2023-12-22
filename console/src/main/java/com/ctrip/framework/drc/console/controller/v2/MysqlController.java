package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.console.param.mysql.DbFilterReq;
import com.ctrip.framework.drc.console.param.mysql.DrcDbMonitorTableCreateReq;
import com.ctrip.framework.drc.console.param.mysql.MysqlWriteEntity;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.check.v2.AutoIncrementVo;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.google.common.collect.Lists;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    @PostMapping("tableColumns")
    public ApiResult getTableColumns(@RequestBody DbFilterReq requestBody) {
        logger.info("getTableColumns requestBody: {}", requestBody);
        try {
            return ApiResult.getSuccessInstance(mysqlServiceV2.getTableColumns(requestBody));
        } catch (Exception e) {
            logger.info("getTableColumns fail: {}", e);
            return ApiResult.getFailInstance(null);
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


    @GetMapping("getMatchTable")
    public ApiResult getMatchTable(@RequestParam String mhaName,
                                   @RequestParam String nameFilter) {
        try {
            List<MySqlUtils.TableSchemaName> matchTables = mysqlServiceV2.getMatchTable(mhaName, nameFilter);
            return ApiResult.getSuccessInstance(matchTables);
        } catch (Exception e) {
            logger.warn("[[tag=matchTable]] error when get {} from {}", nameFilter, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else if (e instanceof IllegalArgumentException) {
                return ApiResult.getFailInstance("no machine find for " + mhaName);
            } else {
                return ApiResult.getFailInstance("other error see log");
            }
        }
    }

    @GetMapping("getAnyMatchTable")
    public ApiResult getAnyMatchTable(@RequestParam String mhaName,
                                      @RequestParam String nameFilters) {
        try {
            List<MySqlUtils.TableSchemaName> matchTables = mysqlServiceV2.getAnyMatchTable(mhaName, nameFilters);
            return ApiResult.getSuccessInstance(matchTables);
        } catch (Exception e) {
            logger.warn("[[tag=matchTable]] error when get {} from {}", nameFilters, mhaName, e);
            if (e instanceof CompileExpressionErrorException) {
                return ApiResult.getFailInstance("expression error");
            } else if (e instanceof IllegalArgumentException) {
                return ApiResult.getFailInstance("no machine find for " + mhaName);
            } else {
                return ApiResult.getFailInstance("other error see log");
            }
        }
    }

    @GetMapping("lastUpdateTime")
    @SuppressWarnings("unchecked")
    public ApiResult<Long> getMhaLastUpdateTime(@RequestParam String srcMha, @RequestParam String mha) {
        logger.info("getMhaLastUpdateTime: {} for {}", srcMha, mha);
        try {
            if (StringUtils.isBlank(srcMha) || StringUtils.isBlank(mha)) {
                return ApiResult.getFailInstance(null, "mha name should not be blank!");
            }
            Long time = mysqlServiceV2.getDelayUpdateTime(srcMha.trim(), mha.trim());
            return ApiResult.getSuccessInstance(time);
        } catch (Throwable e) {
            logger.error(String.format("getMhaDelay error: %s for %s", srcMha, mha), e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @GetMapping("currentTime")
    @SuppressWarnings("unchecked")
    public ApiResult<Long> getMhaCurrentTime(@RequestParam String mha) {
        logger.info("getMhaLastUpdateTime: {}", mha);
        try {
            if (StringUtils.isBlank(mha)) {
                return ApiResult.getFailInstance(null, "mha name should not be blank!");
            }
            Long time = mysqlServiceV2.getCurrentTime(mha.trim());
            return ApiResult.getSuccessInstance(time);
        } catch (Throwable e) {
            logger.error(String.format("getMhaLastUpdateTime error: %s", mha), e);
            return ApiResult.getFailInstance(null, e.getMessage());
        }
    }

    @PostMapping("queryTableRecords")
    public ApiResult<Map<String, Object>> queryTableRecords(@RequestBody QueryRecordsRequest requestBody) {
        try {
            logger.info("queryTableRecords: {}", requestBody);
            Map<String, Object> result = mysqlServiceV2.queryTableRecords(requestBody);
            return ApiResult.getSuccessInstance(result);
        } catch (Exception e) {
            logger.error("queryTableRecords error", requestBody, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("onUpdateColumns")
    public ApiResult<List<String>> getAllOnUpdateColumns(@RequestParam String mha, @RequestParam String db, @RequestParam String table) {
        try {
            logger.info("getAllOnUpdateColumns, mha: {}, db:{}, table: {}", mha, db, table);
            List<String> onUpdateColumns = mysqlServiceV2.getAllOnUpdateColumns(mha, db, table);
            return ApiResult.getSuccessInstance(onUpdateColumns);
        } catch (Exception e) {
            logger.error("getAllOnUpdateColumns, mha: {}, db:{}, table: {}", mha, db, table, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @PostMapping("createDrcMonitorDbTable")
    @SuppressWarnings("unchecked")
    public ApiResult<Boolean> createDrcMonitorDbTable(@RequestBody DrcDbMonitorTableCreateReq requestBody) {
        try {
            logger.info("createDrcMonitorDbTable, req: {}", requestBody);
            Boolean createResult = mysqlServiceV2.createDrcMonitorDbTable(requestBody);
            return ApiResult.getSuccessInstance(createResult);
        } catch (Exception e) {
            logger.error("createDrcMonitorDbTable, mha: " + requestBody, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("firstUniqueIndex")
    public ApiResult<String> getFirstUniqueIndex(@RequestParam String mha, @RequestParam String db, @RequestParam String table) {
        try {
            logger.info("getFirstUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table);
            return ApiResult.getSuccessInstance(mysqlServiceV2.getFirstUniqueIndex(mha, db, table));
        } catch (Exception e) {
            logger.error("getFirstUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @GetMapping("uniqueIndex")
    public ApiResult<List<String>> getUniqueIndex(@RequestParam String mha, @RequestParam String db, @RequestParam String table) {
        try {
            logger.info("getUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table);
            return ApiResult.getSuccessInstance(mysqlServiceV2.getUniqueIndex(mha, db, table));
        } catch (Exception e) {
            logger.error("getUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table, e);
            return ApiResult.getFailInstance(null);
        }
    }

    @PostMapping("write")
    public ApiResult<StatementExecutorResult> write(@RequestBody MysqlWriteEntity requestBody) {
        try {
            logger.info("write to mha entity: {}", requestBody);
            return ApiResult.getSuccessInstance(mysqlServiceV2.write(requestBody));
        } catch (Exception e) {
            logger.info("write to mha entity fail: {}", requestBody, e);
            return ApiResult.getFailInstance(new StatementExecutorResult(SqlResultEnum.FAIL.getCode(), e.getMessage()));
        }
    }

    @GetMapping("autoIncrement")
    public ApiResult getAutoIncrementAndOffset(@RequestParam String mha) {
        try {
            logger.info("getAutoIncrementAndOffset, mha: {}", mha);
            return ApiResult.getSuccessInstance(mysqlServiceV2.getAutoIncrementAndOffset(mha));
        } catch (Exception e) {
            logger.error("getAutoIncrementAndOffset fail, mha: {}", mha, e);
            return ApiResult.getFailInstance(null);
        }
    }

}
