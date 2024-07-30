package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.DrcTmpconninfoDao;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.impl.api.ApiContainer;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.*;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.SQLDigestInfo.Digest;
import com.ctrip.framework.drc.console.service.v2.security.KmsService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultTransactionMonitorHolder;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.fetcher.event.transaction.Transaction;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
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
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    
    private UserService userService = ApiContainer.getUserServiceImpl();

    @Autowired
    private KmsService kmsService;
    @Autowired
    private MhaServiceV2 mhaServiceV2;
    @Autowired
    private DrcTmpconninfoDao drcTmpconninfoDao;


    @Override
    public DbaClusterInfoResponse getClusterMembersInfo(String clusterName) {
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("clustername", clusterName);
        String mysqlApiUrl = domainConfig.getMysqlApiUrl();
        String responseString = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO, requestBody, String.class);
        DbaClusterInfoResponse clusterInfo = JsonUtils.fromJson(responseString, DbaClusterInfoResponse.class);

//        DbaClusterInfoResponse clusterInfo = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO, requestBody,DbaClusterInfoResponse.class);
        if (clusterInfo == null || !clusterInfo.getSuccess() || clusterInfo.getData() == null
                || clusterInfo.getData().getMemberlist() == null || clusterInfo.getData().getMemberlist().isEmpty()) {
            logger.info("clusterName:{}, getMembersInfo failed, try to get from cloud", clusterName);
            responseString = HttpUtils.post(mysqlApiUrl + GET_CLUSTER_NODE_INFO_CLOUD, requestBody, String.class);
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

    /**
     * {
     *     "access_token":"...",
     *     "request_body":
     *     {
     *   "db_name":"db",
     *   "table_name":"table",
     *   "begin_time":"2024-01-22 14:05",
     *   "end_time":"2024-01-29 14:05"
     * }
     * }
     *
     * center region
     * {
     *     "success": true,
     *     "content": {
     *         "seeks": 1,
     *         "scans": 2,
     *         "insert": 3,
     *         "update": 0,
     *         "delete": 0
     *     }
     * }
     * 
     * 
     * @return
     */

    @Override
    public boolean everUserTraffic(String region, String dbName, String tableName, long startTime, long endTime,
            boolean includeRead) {
        if ("fra".equalsIgnoreCase(region)) { // dba api not support fraaws temporarily
            return true;
        }
        boolean isOverSea = !consoleConfig.getCenterRegion().equalsIgnoreCase(region);
        String token = domainConfig.getOpsAccessToken();
        String url = isOverSea ? domainConfig.getOverSeaUserDMLQueryUrl() : domainConfig.getCenterRegionUserDMLCountQueryUrl();
        LinkedHashMap<String, Object> request = Maps.newLinkedHashMap();
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        request.put("access_token", token);
        request.put("request_body", requestBody);
        requestBody.put("db_name", dbName);
        requestBody.put("table_name", tableName);
        if (!isOverSea && startTime < 1708257387000L) { // temporary,dba clear data in 2024-02-18 19:56:27
            startTime = 1708257387000L;
        }
        requestBody.put("begin_time", DateUtils.longToString(startTime, "yyyy-MM-dd HH:mm"));
        requestBody.put("end_time", DateUtils.longToString(endTime, "yyyy-MM-dd HH:mm"));

        String responseString = HttpUtils.post(url, request, String.class);
        JsonObject jsonObject = JsonUtils.parseObject(responseString);
        boolean success = jsonObject.get("success").getAsBoolean();
        if (isOverSea) {
            if (!success) { // success means has write or read in overSea api
                logger.info("{} no user traffic, db:{}, table:{}, response: {}", region,dbName, tableName, responseString);
                return false;
            }
            SQLDigestInfo sqlDigestInfo = JsonUtils.fromJson(responseString, SQLDigestInfo.class);
            Digest write = sqlDigestInfo.getContent().getWrite();
            String digest_sql = write == null ? "" : write.getDigest_sql();
            logger.info("region:{} db:{}, table:{},has user traffic,response: {}", region,dbName,tableName,responseString);
            boolean hasWrite = StringUtils.isNotBlank(digest_sql);
            return includeRead || hasWrite;
        }

        if (success) { // success means api execute result in center region api
            JsonObject content = jsonObject.get("content").getAsJsonObject();
            int seeks = content.get("seeks").getAsInt();
            int insert = content.get("insert").getAsInt();
            int update = content.get("update").getAsInt();
            int delete = content.get("delete").getAsInt();
            boolean hasWrite = (insert > 0) || (update > 0) || (delete > 0);
            boolean hasRead = seeks > 0;
            return includeRead ? hasRead || hasWrite : hasWrite;
        } else {
            logger.error("everUserTraffic failed, db:{}, table:{}, response: {}", dbName, tableName, responseString);
            return true;
        }
    }

    @Override
    public MhaAccounts accountV2PwdChange(MhaTblV2 mhaTblV2) {
        try {
            MachineTbl masterNode = mhaServiceV2.getMasterNode(mhaTblV2.getId());
            if (!doChangeAccountV2Pwd(mhaTblV2.getMhaName(), masterNode)) {
                return null;
            }
            String kmsAccessToken = consoleConfig.getKMSAccessToken("dba.account");
            String secretKey = kmsService.getSecretKey(kmsAccessToken);
            return drcTmpconninfoDao.queryByHostPort(secretKey, masterNode.getIp(), masterNode.getPort());
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logEvent("drc.console.initAccountV2.fail", mhaTblV2.getMhaName());
            logger.error("initAccountV2 failed, mhaTblV2:{}", mhaTblV2, e);
            return null;
        }
    }

    @Override
    public MhaAccounts accountV2PwdChange(String mhaName, String masterNodeIp, Integer masterNodePort) {
        try {
            MachineTbl masterNode = new MachineTbl();
            masterNode.setIp(masterNodeIp);
            masterNode.setPort(masterNodePort);
            if (!doChangeAccountV2Pwd(mhaName, masterNode)) {
                return null;
            }
            String kmsAccessToken = consoleConfig.getKMSAccessToken("dba.account");
            String secretKey = kmsService.getSecretKey(kmsAccessToken);
            return drcTmpconninfoDao.queryByHostPort(secretKey, masterNodeIp, masterNodePort);
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logEvent("drc.console.initAccountV2.fail", mhaName);
            logger.error("initAccountV2 failed, mhaName:{}, masterNodeIp:{}, Port:{}", mhaName, masterNodeIp, masterNodePort, e);
            return null;
        }
        
    }

    public boolean doChangeAccountV2Pwd(String mhaName, MachineTbl masterNode) throws Exception {
        return DefaultTransactionMonitorHolder.getInstance().logTransaction(
                "drc.console.doChangeAccountV2Pwd", mhaName,
                ()-> {
                    Map<String,Object> params = Maps.newLinkedHashMap();
                    params.put("datasource", masterNode.getIp() + ";port=" + masterNode.getPort());
                    String kmsAccessToken = consoleConfig.getKMSAccessToken("dba.account");
                    String secretKey = kmsService.getSecretKey(kmsAccessToken);
                    params.put("token", secretKey);

                    String resString = HttpUtils.post(consoleConfig.getDbaApiPwdChangeUrl(), params, String.class);
                    JsonObject res = JsonUtils.parseObject(resString);
                    String status = res.get("status").getAsString();
                    if ("success".equalsIgnoreCase(status)) {
                        DefaultEventMonitorHolder.getInstance().logEvent("drc.console.changePassword.success", mhaName);
                        return true;
                    }
                    DefaultEventMonitorHolder.getInstance().logEvent("drc.console.changePassword.fail", mhaName);
                    logger.error("changePassword failed, mha:{}, masterNode:{}, response:{}", mhaName, masterNode, resString);
                    return false;
                });
    }


}
