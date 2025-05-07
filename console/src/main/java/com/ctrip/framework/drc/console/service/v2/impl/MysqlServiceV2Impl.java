package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.TableSchemaListApiResult;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.console.param.mysql.*;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.Constants;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.check.v2.*;
import com.ctrip.framework.drc.console.vo.response.StringSetApiResult;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.TablesCloneTask;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.AccountEndpoint;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.mq.MessengerGtidTbl;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ctrip.framework.drc.console.utils.MySqlUtils.CREATE_GTID_TABLE_SQL;
import static com.ctrip.framework.drc.core.monitor.column.DbDelayMonitorColumn.CREATE_DELAY_TABLE_SQL;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by dengquanliang
 * 2023/8/11 15:50
 */
@Service
public class MysqlServiceV2Impl implements MysqlServiceV2 {

    @Autowired
    private CacheMetaService cacheMetaService;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    @PossibleRemote(path = "/api/drc/v2/mha/gtid/executed")
    public String getMhaExecutedGtid(String mha) {
        logger.info("[[tag=gtidQuery]] try to getMhaExecutedGtid from mha{}", mha);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=gtidQuery]] getMhaExecutedGtid from mha {},machine not exist", mha);
            return null;
        } else {
            return MySqlUtils.getExecutedGtid(endpoint);
        }

    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mha/gtid/purged")
    public String getMhaPurgedGtid(String mha) {
        logger.info("[[tag=gtidQuery]] try to getMhaPurgedGtid from mha{}", mha);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=gtidQuery]] getMhaPurgedGtid from mha {},machine not exist", mha);
            return null;
        } else {
            return MySqlUtils.getPurgedGtid(endpoint);
        }
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mha/gtid/applied")
    public String getMhaAppliedGtid(String mha) {
        logger.info("[[tag=gtidQuery]] try to getMhaAppliedGtid from mha{}", mha);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=gtidQuery]] getMhaAppliedGtid from mha {},machine not exist", mha);
            return null;
        } else {
            return MySqlUtils.getMhaAppliedGtid(endpoint);
        }
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mha/db/gtid/applied")
    public Map<String, String> getMhaDbAppliedGtid(String mha) {
        logger.info("[[tag=gtidQuery]] try to getMhaDbAppliedGtid from mha{}", mha);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=gtidQuery]] getMhaDbAppliedGtid from mha {},machine not exist", mha);
            return null;
        } else {
            return MySqlUtils.getMhaDbAppliedGtid(endpoint);
        }
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/lastUpdateTime")
    public Long getDelayUpdateTime(String srcMha, String mha) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=delayQuery]] delayQuery from mha {},machine not exist", mha);
            return null;
        }
        return MySqlUtils.getDelayUpdateTime(endpoint, srcMha);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/db/lastUpdateTime")
    public Map<String, Long> getDbDelayUpdateTime(String srcMha, String mha, List<String> dbNames) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=delayQuery]] getDbDelayUpdateTime from mha {},machine not exist", mha);
            return null;
        }
        return MySqlUtils.getDbDelayUpdateTime(endpoint, srcMha, dbNames);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/currentTime")
    public Long getCurrentTime(String mha) {
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mha);
        if (mySqlEndpoint == null) {
            throw new IllegalArgumentException("no machine find for" + mha);
        }
        return MySqlUtils.getCurrentTime(mySqlEndpoint);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/preCheckMySqlConfig")
    public Map<String, Object> preCheckMySqlConfig(String mha) {
        Map<String, Object> res = new HashMap<>();
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlConfig from mha:{},db not exist", mha);
            return res;
        }
        res.put("binlogMode", MySqlUtils.checkBinlogMode(endpoint));
        res.put("binlogFormat", MySqlUtils.checkBinlogFormat(endpoint));
        res.put("binlogVersion1", MySqlUtils.checkBinlogVersion(endpoint)); //  todo 5.7 -> 8.0
        res.put("binlogTransactionDependency", MySqlUtils.checkBinlogTransactionDependency(endpoint));
        res.put("binlogTransactionDependencyHistorySize", MySqlUtils.checkBtdhs(endpoint));
        res.put("gtidMode", MySqlUtils.checkGtidMode(endpoint));
        res.put("drcTables", MySqlUtils.checkDrcTables(endpoint));
        res.put("autoIncrementStep", MySqlUtils.checkAutoIncrementStep(endpoint));
        res.put("autoIncrementOffset", MySqlUtils.checkAutoIncrementOffset(endpoint));
        res.put("binlogRowImage", MySqlUtils.checkBinlogRowImage(endpoint));
        List<Endpoint> endpoints = cacheMetaService.getMasterEndpointsInAllAccounts(mha);
        if (CollectionUtils.isEmpty(endpoints) || endpoints.size() != 3) {
            logger.error("[[tag=preCheck]] preCHeckDrcAccounts from mha:{},db not exist", mha);
            res.put("drcAccounts", "no db endpoint find");
        } else {
            res.put("drcAccounts", MySqlUtils.checkAccounts(endpoints));
        }
        return res;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/preCheckMySqlTables")
    public List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter) {
        List<TableCheckVo> tableVos = Lists.newArrayList();
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlTables from mha:{},db not exist", mha);
            return tableVos;
        }
        return MySqlUtils.checkTablesWithFilter(endpoint, nameFilter);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/getMatchTable", responseType = TableSchemaListApiResult.class)
    public List<MySqlUtils.TableSchemaName> getMatchTable(String mhaName, String nameFilter) {
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mhaName);
        if (mySqlEndpoint == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.GET_MYSQL_ENDPOINT_NULL, "no machine find for: " + mhaName);
        }
        return MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, new AviatorRegexFilter(nameFilter));
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/getAnyMatchTable", responseType = TableSchemaListApiResult.class)
    public List<MySqlUtils.TableSchemaName> getAnyMatchTable(String mhaName, String nameFilters) {
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mhaName);
        if (mySqlEndpoint == null) {
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.GET_MYSQL_ENDPOINT_NULL, "no machine find for: " + mhaName);
        }
        List<String> nameFiltersList = Lists.newArrayList(nameFilters.split(","));
        if (CollectionUtils.isEmpty(nameFiltersList)) {
            return Collections.emptyList();
        }
        List<AviatorRegexFilter> filterList = nameFiltersList.stream().distinct().map(AviatorRegexFilter::new).collect(Collectors.toUnmodifiableList());
        return MySqlUtils.getTablesMatchAnyRegexFilter(mySqlEndpoint, filterList);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/queryDbs")
    public List<String> queryDbsWithNameFilter(String mha, String nameFilter) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("queryDbsWithNameFilter from mha: {},db not exist", mha);
            return new ArrayList<>();
        }
        return MySqlUtils.queryDbsWithFilter(endpoint, nameFilter);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/queryTables")
    public List<String> queryTablesWithNameFilter(String mha, String nameFilter) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("queryTablesWithNameFilter from mha: {},db not exist", mha);
            return new ArrayList<>();
        }
        return MySqlUtils.queryTablesWithFilter(endpoint, nameFilter);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/commonColumns", responseType = StringSetApiResult.class)
    public Set<String> getCommonColumnIn(String mhaName, String namespace, String name) {
        logger.info("[[tag=commonColumns]] get columns {}\\.{} from {}", namespace, name, mhaName);
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mhaName);
        if (mySqlEndpoint != null) {
            AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
            return MySqlUtils.getAllCommonColumns(mySqlEndpoint, aviatorRegexFilter);
        } else {
            throw new IllegalArgumentException("no machine find for" + mhaName);
        }
    }

    /**
     * key: tableName, values: columns
     */
    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/tableColumns", httpType = HttpRequestEnum.POST, requestClass = DbFilterReq.class, responseType = TableColumnsApiResult.class)
    public Map<String, Set<String>> getTableColumns(DbFilterReq requestBody) {
        Map<String, Set<String>> result = null;
        logger.info("getTableColumns requestBody: {}", requestBody);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(requestBody.getMha());
        if (endpoint == null) {
            logger.error("getTableColumns from mha: {}, db not exit", requestBody.getMha());
        } else {
            result = MySqlUtils.getTableColumns(endpoint, requestBody.getDbFilter());
        }
        return result;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/columnCheck", responseType = StringSetApiResult.class)
    public Set<String> getTablesWithoutColumn(String column, String namespace, String name, String mhaName) {
        Set<String> tables = Sets.newHashSet();
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mhaName);
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
        List<MySqlUtils.TableSchemaName> tablesAfterRegexFilter = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
        Map<String, Set<String>> allColumnsByTable = MySqlUtils.getAllColumnsByTable(mySqlEndpoint, tablesAfterRegexFilter, false);
        for (Map.Entry<String, Set<String>> entry : allColumnsByTable.entrySet()) {
            String tableName = entry.getKey();
            if (!entry.getValue().contains(column)) {
                tables.add(tableName);
            }
        }
        return tables;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/queryTableRecords", httpType = HttpRequestEnum.POST, requestClass = QueryRecordsRequest.class)
    public Map<String, Object> queryTableRecords(QueryRecordsRequest requestBody) throws Exception {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(requestBody.getMha());
        if (endpoint == null) {
            logger.error("queryTableRecords from mha: {}, db not exist", requestBody.getMha());
            return new HashMap<>();
        }
        try {
            return MySqlUtils.queryRecords(endpoint, requestBody.getSql(), requestBody.getOnUpdateColumns(), requestBody.getUniqueIndexColumns(), requestBody.getColumnSize());
        } catch (Exception e) {
            throw ConsoleExceptionUtils.message(e.getMessage());
        }
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/onUpdateColumns")
    public List<String> getAllOnUpdateColumns(String mha, String db, String table) {
        logger.info("getAllOnUpdateColumns, mha: {}, db:{}, table: {}", mha, db, table);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("queryTableRecords from mha: {}, db not exist", mha);
            return new ArrayList<>();
        }
        return MySqlUtils.getAllOnUpdateColumns(endpoint, db, table);
    }

    protected Map<String, Map<String, String>> getDDLSchemas(Set<String> dbList) {
        Map<String, Map<String, String>> dbMap = new HashMap<>();
        Map<String, String> tableMap = new HashMap<>();
        dbMap.put(DRC_MONITOR_SCHEMA_NAME, tableMap);
        for (String db : dbList) {
            String delayTableName = DRC_DB_DELAY_MONITOR_TABLE_NAME_PREFIX + db;
            tableMap.put(delayTableName, String.format(CREATE_DELAY_TABLE_SQL, delayTableName));
            String gtidTableName = DRC_DB_TRANSACTION_TABLE_NAME_PREFIX + db;
            tableMap.put(gtidTableName, String.format(CREATE_GTID_TABLE_SQL, gtidTableName));
        }
        return dbMap;
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/firstUniqueIndex")
    public String getFirstUniqueIndex(String mha, String db, String table) {
        logger.info("getFirstUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("getFirstUniqueIndex from mha: {}, db not exist", mha);
            return null;
        }
        return MySqlUtils.getFirstUniqueIndex(endpoint, db, table);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/uniqueIndex")
    public List<String> getUniqueIndex(String mha, String db, String table) {
        logger.info("getUniqueIndex, mha: {}, db:{}, table: {}", mha, db, table);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("getFirstUniqueIndex from mha: {}, db not exist", mha);
            return null;
        }
        return MySqlUtils.getUniqueIndex(endpoint, db, table);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/write", httpType = HttpRequestEnum.POST, requestClass = MysqlWriteEntity.class, responseType = StatementExecutorApResult.class)
    public StatementExecutorResult write(MysqlWriteEntity requestBody) {
        logger.info("execute write sql, requestBody: {}", requestBody);
        Endpoint endpoint;
        if (requestBody.getAccountType() == DrcAccountTypeEnum.DRC_WRITE.getCode()) {
            endpoint = cacheMetaService.getMasterEndpointForWrite(requestBody.getMha());
        } else {
            endpoint = cacheMetaService.getMasterEndpoint(requestBody.getMha());
        }

        if (endpoint == null) {
            logger.error("write to mha: {}, db not exist", requestBody.getMha());
            return new StatementExecutorResult(SqlResultEnum.FAIL.getCode(), Constants.ENDPOINT_NOT_EXIST);
        }
        return MySqlUtils.write(endpoint, requestBody.getSql(), requestBody.getAccountType());
    }


    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/createDrcMessengerGtidTbl", httpType = HttpRequestEnum.POST, requestClass = DrcMessengerGtidTblCreateReq.class)
    public Boolean createDrcMessengerGtidTbl(DrcMessengerGtidTblCreateReq requestBody) {
        String mha = requestBody.getMha();
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("createDrcMessengerGtidTbl from {}, endpoint not exist", mha);
            return Boolean.FALSE;
        }
        List<String> tablesFromDb = MySqlUtils.getTablesFromDb(endpoint, DRC_MONITOR_SCHEMA_NAME);
        boolean messengerGtidTblExist = tablesFromDb.stream().anyMatch(MessengerGtidTbl.TABLE_NAME::equals);
        if (messengerGtidTblExist) {
            logger.info("no need to create messenger gtid table for {}", mha);
            return Boolean.TRUE;
        }

        logger.info("start create messenger gtid table for {}", mha);
        Map<String, Map<String, String>> ddlSchemas = MessengerGtidTbl.getDDLSchemas();
        Boolean res = new RetryTask<>(new TablesCloneTask(ddlSchemas, endpoint, DataSourceManager.getInstance().getDataSource(endpoint), null), 1).call();
        return Boolean.TRUE.equals(res);
    }



    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/createDrcMonitorDbTable", httpType = HttpRequestEnum.POST, requestClass = DrcDbMonitorTableCreateReq.class)
    public Boolean createDrcMonitorDbTable(DrcDbMonitorTableCreateReq requestBody) {
        String mha = requestBody.getMha();
        List<String> dbs = requestBody.getDbs();
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("createDrcMonitorDbTable from {} {}, endpoint not exist", mha, dbs);
            return Boolean.FALSE;
        }

        Set<String> dbList = dbs.stream().map(String::toLowerCase).collect(Collectors.toSet());
        List<String> dbsInDrcMonitorDb = MySqlUtils.getTablesFromDb(endpoint, DRC_MONITOR_SCHEMA_NAME);
        if (dbsInDrcMonitorDb == null) {
            logger.error("MySqlUtils getTablesFromDb from drcmonitordb fail, req:" + requestBody);
            return Boolean.FALSE;
        }

        Map<String, Map<String, String>> messengerGtidDdlSchema = Maps.newHashMap();
        if (!dbsInDrcMonitorDb.contains(MessengerGtidTbl.TABLE_NAME)) {
            logger.info("start create {} for {}", MessengerGtidTbl.TABLE_NAME, mha);
            messengerGtidDdlSchema = MessengerGtidTbl.getDDLSchemas();
        }

        Map<String, Map<String, String>> dbMonitorTblDdlSchemas = Maps.newHashMap();;
        Set<String> existDbs = MySqlUtils.getDbHasDrcMonitorTables(dbsInDrcMonitorDb);
        dbList.removeAll(existDbs);
        if (!CollectionUtils.isEmpty(dbList)) {
            logger.info("start create table for {} {}", mha, dbList);
            dbMonitorTblDdlSchemas = getDDLSchemas(dbList);
        }

        Map<String, Map<String, String>> ddlSchemas = Stream.concat(
                messengerGtidDdlSchema.entrySet().stream(),
                dbMonitorTblDdlSchemas.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (m1, m2) -> {
                            Map<String, String> mergeMap = Maps.newHashMap(m1);
                            mergeMap.putAll(m2);
                            return mergeMap;
                        }
                ));
        if (ddlSchemas.isEmpty()) {
            logger.info("no need to create table for {} {}", mha, dbs);
            return true;
        }
        Boolean res = new RetryTask<>(new TablesCloneTask(ddlSchemas, endpoint, DataSourceManager.getInstance().getDataSource(endpoint), null), 1).call();

        return Boolean.TRUE.equals(res);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/autoIncrement", responseType = AutoIncrementVoApiResult.class)
    public AutoIncrementVo getAutoIncrementAndOffset(String mha) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("getAutoIncrementAndOffset from {} , endpoint not exist", mha);
            return null;
        }

        AutoIncrementVo result = MySqlUtils.queryAutoIncrementAndOffset(endpoint);
        if (result == null) {
            logger.error("getAutoIncrementAndOffset null, mha: {}", mha);
        }
        return result;
    }
    
    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/accountPrivileges")
    public String queryAccountPrivileges(String mha, String account, String pwd) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        AccountEndpoint accEndpoint = new AccountEndpoint(endpoint.getHost(), endpoint.getPort(), account, pwd, true);
        return MySqlUtils.getAccountPrivilege(accEndpoint,true);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/columnDefault", responseType = ColumnDefaultApiResult.class)
    public List<MySqlUtils.ColumnDefault> getMhaColumnDefaultValue(String mha) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("getMhaColumnDefaultValue from {} , endpoint not exist", mha);
            return new ArrayList<>();
        }

        List<MySqlUtils.ColumnDefault> columnDefaultList = MySqlUtils.getColumnDefaultValue(endpoint);
        return columnDefaultList;
    }


}
