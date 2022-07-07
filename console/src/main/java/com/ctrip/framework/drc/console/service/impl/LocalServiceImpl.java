package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.LocalService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName LocalServiceImpl
 * @Author haodongPan
 * @Date 2022/6/21 19:10
 * @Version: $
 */
@Service
public class LocalServiceImpl implements LocalService {
    
    private static final Logger logger = LoggerFactory.getLogger(LocalServiceImpl.class);
    
    @Autowired
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Override
    public Map<String, Object> preCheckMySqlConfig(String mha) {
        Map<String, Object> res = new HashMap<>();
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlConfig from mha:{},db not exist", mha);
            return res;
        }
        res.put("binlogMode", MySqlUtils.checkBinlogMode(endpoint));
        res.put("binlogFormat", MySqlUtils.checkBinlogFormat(endpoint));
        res.put("binlogVersion1", MySqlUtils.checkBinlogVersion(endpoint));
        res.put("binlogTransactionDependency", MySqlUtils.checkBinlogTransactionDependency(endpoint));
        res.put("gtidMode", MySqlUtils.checkGtidMode(endpoint));
        res.put("drcTables", MySqlUtils.checkDrcTables(endpoint));
        res.put("autoIncrementStep", MySqlUtils.checkAutoIncrementStep(endpoint));
        res.put("autoIncrementOffset", MySqlUtils.checkAutoIncrementOffset(endpoint));
        List<Endpoint> endpoints = dbClusterSourceProvider.getMasterEndpointsInAllAccounts(mha);
        if (CollectionUtils.isEmpty(endpoints) || endpoints.size() != 3) {
            logger.error("[[tag=preCheck]] preCHeckDrcAccounts from mha:{},db not exist",mha);
            res.put("drcAccounts","no db endpoint find");
        } else {
            res.put("drcAccounts",MySqlUtils.checkAccounts(endpoints));
        }
        return res;
    }
    
    @Override
    public List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter) {
        List<TableCheckVo> tableVos = Lists.newArrayList();
        Endpoint endpoint = dbClusterSourceProvider.getMasterEndpoint(mha);
        if (endpoint == null) {
            logger.error("[[tag=preCheck]] preCheckMySqlTables from mha:{},db not exist",mha);
            return tableVos;
        }
        return MySqlUtils.checkTablesWithFilter(endpoint, nameFilter);
    }
    
    
}
