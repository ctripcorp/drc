package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.TableSchemaListApiResult;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.MysqlAccountTypeEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.enums.SqlResultEnum;
import com.ctrip.framework.drc.console.param.mysql.MysqlWriteEntity;
import com.ctrip.framework.drc.console.param.mysql.QueryRecordsRequest;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.Constants;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.response.StringSetApiResult;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

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

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/columnCheck", responseType = StringSetApiResult.class)
    public Set<String> getTablesWithoutColumn(String column, String namespace, String name, String mhaName) {
        Set<String> tables = Sets.newHashSet();
        Endpoint mySqlEndpoint = cacheMetaService.getMasterEndpoint(mhaName);
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(namespace + "\\." + name);
        List<MySqlUtils.TableSchemaName> tablesAfterRegexFilter = MySqlUtils.getTablesAfterRegexFilter(mySqlEndpoint, aviatorRegexFilter);
        Map<String, Set<String>> allColumnsByTable = MySqlUtils.getAllColumnsByTable(mySqlEndpoint, tablesAfterRegexFilter, true);
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
            return MySqlUtils.queryRecords(endpoint, requestBody.getSql(), requestBody.getOnUpdateColumns(), requestBody.getColumnSize());
        } catch (Exception e) {
            throw ConsoleExceptionUtils.message(e.getMessage());
        }
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/onUpdateColumns")
    public List<String> getAllOnUpdateColumns(String mha, String db, String table) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("queryTableRecords from mha: {}, db not exist", mha);
            return new ArrayList<>();
        }
        return MySqlUtils.getAllOnUpdateColumns(endpoint, db, table);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/uniqueIndex")
    public String getFirstUniqueIndex(String mha, String db, String table) {
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("getFirstUniqueIndex from mha: {}, db not exist", mha);
            return null;
        }
        return MySqlUtils.getFirstUniqueIndex(endpoint, db, table);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mysql/write", httpType = HttpRequestEnum.POST, requestClass = MysqlWriteEntity.class)
    public StatementExecutorResult write(MysqlWriteEntity requestBody) {
        Endpoint endpoint;
        if (requestBody.getAccountType() == MysqlAccountTypeEnum.DRC_WRITE.getCode()) {
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
}
