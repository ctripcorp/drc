package com.ctrip.framework.drc.service.console;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.AuthorityConfig;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.enums.InitDalGoalEnum;
import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.ctrip.framework.drc.service.console.dbcluster.DbInfoDetail;
import com.ctrip.framework.drc.service.console.dbcluster.DbInfosResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.CollectionUtils;

import static com.ctrip.framework.drc.core.service.utils.Constants.MHA_INSTANCES_GROUP_LIST;

/**
 * @ClassName DbClusterApiServiceImpl
 * @Author haodongPan
 * @Date 2021/11/24 15:45
 * @Version: $
 */
// forbidden new instance ,use ApiContainer except for test
public class DbClusterApiServiceImpl implements DbClusterApiService {
    private static final Logger logger = LoggerFactory.getLogger(DbClusterApiServiceImpl.class);

    private static final String DB_CLUSTER_GET_CLUSTER_INFO = "clusters/%s?operator=DRCConsole";

    private static final String MHA_INSTANCES_GROUP_CLUSTER_INFO = "instanceGroups/%s/clusters?operator=DRCConsole";

    public static final String CHANGE_TO_NORMAL_MODE = "clusters/%s/types/normal?releaseZoneId=%s&operator=DRCConsole";

    public static final String CHANGE_TO_DRC_MODE = "clusters/%s/types/drc?operator=DRCConsole";

    private static final String OPERATOR = "DRCConsole";

    private static final String DAL_SERVICE_SUFFIX = "?operator=drcAdmin";
    
    private static final String GET_DB_INFO_BY_DBS = "/console/dal/v2/db/dbInfos/";
    

    @Override
    public JsonNode getDalClusterInfo(String dalServicePrefix,String dalClusterName) {
        String uri = String.format(dalServicePrefix + DB_CLUSTER_GET_CLUSTER_INFO, dalClusterName);
        return getResultNode(uri);
    }

    @Override
    public JsonNode getInstanceGroupsInfo(String dalServicePrefix,List<String> mhas) {
        String uri = String.format(dalServicePrefix + MHA_INSTANCES_GROUP_CLUSTER_INFO, StringUtils.join(mhas, ","));
        return getResultNode(uri);
    }

    @Override
    public JsonNode getMhaList(String dalServicePrefix) {
        String uri = dalServicePrefix + MHA_INSTANCES_GROUP_LIST;
        return getResultNode(uri);
    }

    @Override
    public ApiResult switchDalClusterType(String dalClusterName,String dalServicePrefix, DalClusterTypeEnum typeEnum, String zoneId) {
        String uri = null;
        if(DalClusterTypeEnum.NORMAL.equals(typeEnum)) {
            uri = String.format(dalServicePrefix + CHANGE_TO_NORMAL_MODE, dalClusterName, zoneId);
        } else if (DalClusterTypeEnum.DRC.equals(typeEnum)) {
            uri = String.format(dalServicePrefix + CHANGE_TO_DRC_MODE, dalClusterName);
        }
        return HttpUtils.put(uri, new Object());
    }

    @Override
    public JsonNode getIgnoreTableConfigs(String dalClusterUrl,String clusterName) {
        String uri = dalClusterUrl + "/api/dal/v2/clusters/" + clusterName + "/tableConfigs?operator=" + OPERATOR;
        return getResultNode(uri);
    }
    
    @Override
    public JsonNode getMhasNode(String dalClusterUrl,String clusterName) {
        String uri = dalClusterUrl + "/api/dal/v2/clusters/" + clusterName + "/mhas?operator=" + OPERATOR;
        return getResultNode(uri);
    }

    @Override
    public JsonNode getDalClusterNode(String dalClusterUrl) {
        String uri = dalClusterUrl + "/api/dal/v2/clusters?operator=" + OPERATOR;
        return getResultNode(uri);
    }

    @Override
    public Map<String, Object> getDalClusterFromDalService(String dalServicePrefix,String clusterName) {
        String uri = dalServicePrefix + "instanceGroups/" + clusterName + "/clusters" + DAL_SERVICE_SUFFIX;
        return HttpUtils.get(uri, Map.class);
    }

    @Override
    public JsonNode releaseDalCluster(String dalServicePrefix,String dalClusterName) {
        String uri = dalServicePrefix + "clusters/" + dalClusterName + "/releases" + DAL_SERVICE_SUFFIX;
        return JacksonUtils.getRootNode(uri, "", JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode registerDalCluster(String dalRegisterPrefix,String requestBody, String goal) {
        String uri;
        InitDalGoalEnum initDalGoalEnum;
        initDalGoalEnum = InitDalGoalEnum.getInitDalGoalEnum(goal);
        uri = dalRegisterPrefix + initDalGoalEnum.getDalRegisterSuffix();
        return JacksonUtils.getRootNode(uri, requestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode getResultNode(String uri) {
        JsonNode result = null;
        try {
            JsonNode rootNode = JacksonUtils.getRootNode(uri, JacksonUtils.HTTP_METHOD_GET);
            result = rootNode.get("result");
        } catch (Exception e) {
            logger.error("Exception, ", e);
        }
        return result;
    }

    @Override
    public String getDalClusterName(String dalClusterUrl, String dbName) {
        String URL = dalClusterUrl + GET_DB_INFO_BY_DBS + dbName;
        DbInfosResponse dbInfosResponse = HttpUtils.get(URL, DbInfosResponse.class, Maps.newHashMap());
        if (!dbInfosResponse.getStatus().equals(200)) {
            logger.error("getDalClusterName for dbName: {},error", dbName);
            throw new RuntimeException("get dalCluster fail for db:" + dbName);
        }

        List<DbInfoDetail> result = dbInfosResponse.getResult();
        if (CollectionUtils.isEmpty(result)) {
            throw new RuntimeException("no dalCluster found for db: " + dbName);
        }

        return result.get(0).getDbNameBase() + "_dalcluster";
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
