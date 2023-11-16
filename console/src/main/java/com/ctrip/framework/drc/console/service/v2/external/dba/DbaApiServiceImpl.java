package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.*;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
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
    private static final String GET_DB_CLUSTER_NODE_INFO_CLOUD_V2 = "/database/getclusterinfoviadalcluster";

    @Autowired
    private DomainConfig domainConfig;
    private UserService userService = ApiContainer.getUserServiceImpl();


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

    @Override
    public List<DbClusterInfoDto> getDatabaseClusterInfoList(String dalClusterName) {
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("dalClusterName", dalClusterName);
        String mysqlApiUrl = domainConfig.getMysqlApiUrl();

        String responseString = HttpUtils.post(mysqlApiUrl + GET_DB_CLUSTER_NODE_INFO_CLOUD_V2, requestBody, String.class);
        logger.info("req: {}, resp: {}", requestBody, responseString);

        DbaDbClusterInfoResponseV2 response = JsonUtils.fromJson(responseString, DbaDbClusterInfoResponseV2.class);
        if (response == null || !response.getSuccess()) {
            throw ConsoleExceptionUtils.message("Query db info error for dalCluster: " + dalClusterName + ". Response: " + response);
        }
        if (CollectionUtils.isEmpty(response.getData())) {
            throw ConsoleExceptionUtils.message(dalClusterName + " empty result ");
        }

        return response.getData();
    }

    // request
    // {
    //    "access_token":"",
    //    "request_body":{
    //          "user_name":""
    //     }
    // }
    // response
    // {
    //    "success": true,
    //    "message": "ok",
    //    "data": ["db1","db2"]
    // }
    @Override
    public List<String> getDBsWithQueryPermission() {
        String dotToken = domainConfig.getDotToken();
        String dotQueryApiUrl = domainConfig.getDotQueryApiUrl();
        String userName = userService.getInfo();
        LinkedHashMap<String, Object> request = Maps.newLinkedHashMap();
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        request.put("access_token", dotToken);
        request.put("request_body", requestBody);
        requestBody.put("user_name", userName);
        
        String responseString = HttpUtils.post(dotQueryApiUrl, request, String.class);
        
        JsonObject jsonObject = JsonUtils.parseObject(responseString);
        List<String> res = Lists.newArrayList();
        boolean success = jsonObject.get("success").getAsBoolean();
        if (success) {
            jsonObject.get("data").getAsJsonArray().forEach(jsonElement -> res.add(jsonElement.getAsString()));
        } else {
            logger.error("getDBsWithQueryPermission failed, use:{}, response: {}", userName, responseString);
        }
        return res;
    }
}
