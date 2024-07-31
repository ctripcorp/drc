package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaDbClusterInfoResponseV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;


@Service
public class DbaApiServiceV2Impl extends DbaApiServiceImpl implements DbaApiService {

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
        String url = domainConfig.getMysqlApiUrlV2() + GET_CLUSTER_NODE_INFO.replace("{cluster}", clusterName);
        String responseString = HttpUtils.get(url, String.class);
        logger.info("[getClusterMembersInfo] url: {}, resp: {}", url, responseString);


        DbaClusterInfoResponse clusterInfo = JsonUtils.fromJson(responseString, DbaClusterInfoResponse.class);
        if (clusterInfo == null || !clusterInfo.getSuccess() || clusterInfo.getData() == null
                || clusterInfo.getData().getMemberlist() == null || clusterInfo.getData().getMemberlist().isEmpty()) {
            logger.error("clusterName:{}, getMembersInfo from cloud failedd", clusterName);
            throw ConsoleExceptionUtils.message(clusterName + " syncMhaInfoFormDbaApi failed! Response: " + clusterInfo);
        }
        return clusterInfo;
    }

    @Override
    public List<ClusterInfoDto> getDatabaseClusterInfo(String dbName) {
        if (!useNewApi()) {
            return super.getDatabaseClusterInfo(dbName);
        }
        String url = domainConfig.getMysqlApiUrlV2() + GET_DATABASE_CLUSTER_INFO.replace("{database}", dbName);
        String responseString = HttpUtils.get(url, String.class);
        logger.info("[getDatabaseClusterInfo] url: {}, resp: {}", url, responseString);

        DbaDbClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message(dbName + " getDatabaseClusterInfo failed! Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dbName + " empty result ");
        }

        return response.getData().stream().flatMap(e -> e.getClusterList().stream()).collect(Collectors.toList());
    }

    @Override
    public List<DbClusterInfoDto> getDatabaseClusterInfoList(String dalClusterName) {
        if (!useNewApi()) {
            return super.getDatabaseClusterInfoList(dalClusterName);
        }

        String url = domainConfig.getMysqlApiUrlV2() + GET_DB_CLUSTER_NODE_INFO.replace("{dalcluster}", dalClusterName);
        String responseString = HttpUtils.get(url, String.class);

        logger.info("[getDatabaseClusterInfoList] url: {}, resp: {}", url, responseString);

        DbaDbClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message("Query db info error for dalCluster: " + dalClusterName + ". Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dalClusterName + " empty result ");
        }

        return response.getData();
    }
}
