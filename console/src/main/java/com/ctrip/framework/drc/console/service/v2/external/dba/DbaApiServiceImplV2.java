package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.*;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Service
public class DbaApiServiceImplV2 extends DbaApiServiceImpl implements DbaApiService {

    public static final String OPERATOR = "drc";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String GET_CLUSTER_NODE_INFO = "/api/v1/mysql/cluster/{cluster}/instances/query";
    private static final String GET_DATABASE_CLUSTER_INFO = "/api/v1/mysql/database/{database}/clusters/query";
    private static final String GET_DB_CLUSTER_NODE_INFO = "/api/v1/mysql/dalcluster/{dalcluster}/clusters/query";

    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private DefaultConsoleConfig consoleConfig;


    private boolean useNewApi() {
        return consoleConfig.getMySQLApiV2Switch();
    }

    @Override
    public DbaClusterInfoResponse getClusterMembersInfo(String clusterName) {
        if (!useNewApi()) {
            return super.getClusterMembersInfo(clusterName);
        }
        Map<String, String> params = new HashMap<>();
        params.put("operator", OPERATOR);
        String url = domainConfig.getMysqlApiUrlV2() + GET_CLUSTER_NODE_INFO.replace("{cluster}", clusterName);
        String responseString = HttpUtils.get(url, String.class, params);
        logger.info("[getClusterMembersInfo] url: {}, resp: {}", url, responseString);

        DbaClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message(clusterName + " getClusterMembersInfo failed! Response: " + response);
        }
        List<MemberInfoV2> data = response.getData();
        if (CollectionUtils.isEmpty(data)) {
            throw ConsoleExceptionUtils.message(clusterName + " getClusterMembersInfo empty result ");
        }

        // compatibility
        return response.toV1();
    }

    @Override
    public List<ClusterInfoDto> getDatabaseClusterInfo(String dbName) {
        if (!useNewApi()) {
            return super.getDatabaseClusterInfo(dbName);
        }
        Map<String, String> params = new HashMap<>();
        params.put("operator", OPERATOR);
        String url = domainConfig.getMysqlApiUrlV2() + GET_DATABASE_CLUSTER_INFO.replace("{database}", dbName);
        String responseString = HttpUtils.get(url, String.class, params);
        logger.info("[getDatabaseClusterInfo] url: {}, resp: {}", url, responseString);

        DbaDbClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message(dbName + " getDatabaseClusterInfo failed! Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dbName + " getDatabaseClusterInfo empty result ");
        }

        return response.getData().stream().flatMap(e -> e.getClusterList().stream()).collect(Collectors.toList());
    }

    @Override
    public List<DbClusterInfoDto> getDatabaseClusterInfoList(String dalClusterName) {
        if (!useNewApi()) {
            return super.getDatabaseClusterInfoList(dalClusterName);
        }

        Map<String, String> params = new HashMap<>();
        params.put("operator", OPERATOR);
        String url = domainConfig.getMysqlApiUrlV2() + GET_DB_CLUSTER_NODE_INFO.replace("{dalcluster}", dalClusterName);
        String responseString = HttpUtils.get(url, String.class, params);
        logger.info("[getDatabaseClusterInfoList] url: {}, resp: {}", url, responseString);

        DbaDbClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message("Query db info error for dalCluster: " + dalClusterName + ". Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dalClusterName + " getDatabaseClusterInfoList empty result ");
        }

        return response.getData();
    }

    @Override
    public String getDbOwner(String dbName){
        return super.getDbOwner(dbName);
    }
}
