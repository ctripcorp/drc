package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.TableCheckVo;
import com.ctrip.framework.drc.console.vo.response.StringSetApiResult;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

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
    @PossibleRemote(path = "/api/drc/v2/local/createTblStmts/query", excludeArguments = {"endpoint"})
    public Map<String, String> getCreateTableStatements(String mha, String unionFilter, Endpoint endpoint) {
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(unionFilter);
        return MySqlUtils.getDefaultCreateTblStmts(endpoint, aviatorRegexFilter);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/local/sql/integer/query", excludeArguments = {"endpoint"})
    public Integer getAutoIncrement(String mha, String sql, int index, Endpoint endpoint) {
        return MySqlUtils.getSqlResultInteger(endpoint, sql, index);
    }

    @Override
    @PossibleRemote(path = "/api/drc/v2/mha/gtid/drcExecuted")
    public String getDrcExecutedGtid(String mha) {
        logger.info("[[tag=gtidQuery]] try to getDrcExecutedGtid from mha{}", mha);
        Endpoint endpoint = cacheMetaService.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.warn("[[tag=gtidQuery]] getDrcExecutedGtid from mha{},machine not exist", mha);
            return null;
        } else {
            return MySqlUtils.getUnionExecutedGtid(endpoint);
        }
    }

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
        res.put("binlogVersion1", MySqlUtils.checkBinlogVersion(endpoint));
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

}
