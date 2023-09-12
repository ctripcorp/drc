package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaClusterInfoResponse;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbaDbClusterInfoResponse;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @ClassName DbaApiServiceImpl
 * @Author haodongPan
 * @Date 2023/8/24 20:58
 * @Version: $
 */
@Service
public class DbaApiServiceImpl implements DbaApiService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String GET_CLUSTER_NODE_INFO = "/clusterapi/getmemberinfo";
    private static final String GET_DATABASE_CLUSTER_INFO = "/database/getdatabaseclusterinfo";
    private static final String GET_CLUSTER_NODE_INFO_CLOUD = "/clusterapi/getmemberinfo4cloud";

    @Autowired
    private DomainConfig domainConfig;


    @Override
    public DbaClusterInfoResponse getClusterMembersInfo(String clusterName) {
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("clustername", clusterName);
        String mysqlApiUrl = domainConfig.getMysqlApiUrl();
        String responseString = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO, requestBody, String.class);
        System.out.println("getClusterMembersInfo responseString" + responseString);
        DbaClusterInfoResponse clusterInfo = JsonUtils.fromJson(responseString, DbaClusterInfoResponse.class);

//        DbaClusterInfoResponse clusterInfo = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO, requestBody,DbaClusterInfoResponse.class);
        if (clusterInfo == null || !clusterInfo.getSuccess() || clusterInfo.getData() == null
                || clusterInfo.getData().getMemberlist() == null || clusterInfo.getData().getMemberlist().isEmpty()) {
            logger.info("clusterName:{}, getMembersInfo failed, try to get from cloud", clusterName);
            responseString = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO_CLOUD, requestBody, String.class);
            System.out.println("getClusterMembersInfo responseString" + responseString);
            clusterInfo = JsonUtils.fromJson(responseString, DbaClusterInfoResponse.class);
        }
        if (clusterInfo == null || !clusterInfo.getSuccess() || clusterInfo.getData() == null
                || clusterInfo.getData().getMemberlist() == null || clusterInfo.getData().getMemberlist().isEmpty()) {
            logger.error("clusterName:{}, getMembersInfo from cloud failedd", clusterName);
            throw ConsoleExceptionUtils.message(clusterName + " syncMhaInfoFormDbaApi failed! Response: " + clusterInfo);
        }
        return clusterInfo;
    }

    @Override

    public List<ClusterInfoDto> getDatabaseClusterInfo(String dbName) {
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("dbName", dbName);
        String mysqlApiUrl = domainConfig.getMysqlApiUrl();
        String responseString = HttpUtils.post(mysqlApiUrl + GET_DATABASE_CLUSTER_INFO, requestBody, String.class);
        logger.info("req: {}, resp: {}", requestBody, responseString);

        DbaDbClusterInfoResponse response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponse.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message(dbName + " getDatabaseClusterInfo failed! Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dbName + " empty result ");

        }

        return response.getData();

    }
}
