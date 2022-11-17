package com.ctrip.framework.drc.service.console;

import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.ops.AppClusterResult;
import com.ctrip.framework.drc.core.service.ops.AppNode;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallTrafficContext;
import com.ctrip.framework.drc.core.service.statistics.traffic.HickWallTrafficEntity;
import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @ClassName OPSApiServiceImpl
 * @Author haodongPan
 * @Date 2021/11/25 14:51
 * @Version: $
 */
public class OPSApiServiceImpl implements OPSApiService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String ACCESS_TOKEN_KEY = "access_token";

    private static final String REQUEST_BODY_KEY = "request_body";

    private static final String CLUSTER_NAME_KEY = "cluster_name";

    private static final String ENV_TYPE_KEY = "env_type";

    private static final String DB_TYPE_KEY = "db_type";

    private static final String DB_TYPE_MYSQL = "mysql";

    private static final String DAL_SERVICE_SUFFIX = "?operator=drcAdmin";


    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode getAllClusterInfo(String getAllClusterUrl,String accessToken) throws JsonProcessingException  {
        ObjectNode requestNode = objectMapper.createObjectNode();
        ObjectNode requestBodyNode = objectMapper.createObjectNode();
        requestBodyNode.put(DB_TYPE_KEY, DB_TYPE_MYSQL);
        requestNode.put(ACCESS_TOKEN_KEY, accessToken);
        requestNode.set(REQUEST_BODY_KEY, requestBodyNode);
        String requestBody = objectMapper.writeValueAsString(requestNode);
        return JacksonUtils.getRootNode(getAllClusterUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode getAllDbs(String mysqlDbClusterUrl,String accessToken,String clusterName, String env) throws JsonProcessingException {
        JsonNode root = null;
        String requestBody = null;
        ObjectNode requestNode = objectMapper.createObjectNode();
        ObjectNode requestBodyNode = objectMapper.createObjectNode();
        requestBodyNode.put(CLUSTER_NAME_KEY, clusterName);
        requestBodyNode.put(ENV_TYPE_KEY, env);
        requestNode.put(ACCESS_TOKEN_KEY, accessToken);
        requestNode.set(REQUEST_BODY_KEY, requestBodyNode);
        requestBody = objectMapper.writeValueAsString(requestNode);
        root = JacksonUtils.getRootNode(mysqlDbClusterUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
        return root;
    }

    @Override
    public List<AppNode> getAppNodes(String cmsGetServerUrl,String accessToken,List<String> appIds,String env) {
        Map<String, Object> body = Maps.newHashMap();
        body.put("app.appId@in", appIds);
        if (null != env) {
            body.put("subenvName@like", env);
        }
        Map<String, Object> requestBody = Maps.newHashMap();
        requestBody.put("request_body", body);
        requestBody.put("access_token", accessToken);
        AppClusterResult appClusterResult = HttpUtils.post(cmsGetServerUrl, requestBody, AppClusterResult.class);
        return appClusterResult.getData();
    }

    @Override
    public List<HickWallTrafficEntity> getTrafficFromHickWall(HickWallTrafficContext context) throws Exception {
        Map<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("access_token", context.getAccessToken());
        requestBody.put("request_body", null);
        String baseUrl = context.getBaseUrl();
        String formatUrl = baseUrl + "?query=sum_over_time(fx.drc.traffic.statistic_rcount" +
                "%7BsrcRegion=%22" + context.getSrcRegion() +
                "%22,%20dstRegion=%22" + context.getDstRegion() +
                "%22,%20dstType=%22" + "Applier" +
                "%22%7D%5B1h%5D)&start=" + context.getEndTime() +
                "&end=" + context.getEndTime() +
                "&step=1&db=APM-FX";

        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, JsonUtils.toJson(requestBody));
        Request request = new Request.Builder()
                .url(formatUrl)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();
        String responseStr = response.body().string();
        JsonObject jsonObject = JsonUtils.fromJson(responseStr, JsonObject.class);
        JsonObject data = jsonObject.get("data").getAsJsonObject();
        String result = JsonUtils.toJson(data.get("result"));

        List<HickWallTrafficEntity> costs= JsonUtils.fromJsonToList(result, HickWallTrafficEntity.class);
        logger.info("[cost] get_cost_from_hick_wall(size:{})", costs.size());
        return costs;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
