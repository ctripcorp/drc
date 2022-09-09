package com.ctrip.framework.drc.console.service.impl.openapi;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.cost.FlowEntity;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.utils.JsonUtils;
import com.ctrip.framework.drc.console.vo.response.*;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2021/7/14
 */
@Service
public class OpenService {

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    @Autowired
    private DomainConfig domainConfig;

    private static final Gson gson = new Gson();

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public MhaListApiResult getMhas(String uri, Map<String, String> params) {
        return HttpUtils.get(uri, MhaListApiResult.class, params);
    }

    public MhaNamesResponseVo getMhaNamesToBeMonitored(String uri) {
        return HttpUtils.get(uri, MhaNamesResponseVo.class);
    }

    public UuidResponseVo getUUIDFromRemoteDC(String uri, Map<String, Object> params) {
        return HttpUtils.get(uri,UuidResponseVo.class,params);
    }

    public ApiResult<String> updateUuidByMachineTbl(String uri, MachineTbl machineTbl) {
        return HttpUtils.post(uri, machineTbl, ApiResult.class);
    }

    public JsonArray getDbArray() throws Exception {
        Map<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("db_type","mysql");
        JsonArray dbArray = getDbArray(requestBody);
        logger.info("get_db_info_from_CMS_done(size:{})", dbArray.size());
        return dbArray;
    }

    @VisibleForTesting
    protected JsonArray getDbArray(Map<String, Object> body) throws Exception {
//        JsonObject requestBody = new JsonObject();
//        requestBody.addProperty("access_token", domainConfig.getCmsAccessToken());
//        requestBody.add("request_body", body);
        Map<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("access_token", domainConfig.getCmsAccessToken());
        requestBody.put("request_body", body);
        String response = HttpUtils.post(domainConfig.getCmsGetDbInfoUrl(),requestBody,String.class);
        JsonObject jsonObject = gson.fromJson(response, JsonObject.class);
        return jsonObject.get("data").getAsJsonArray();
    }

    public void refreshBuInfoMap(Map<Long, String> buId2BuCodeMap) throws Exception {
//        JsonObject requestBody = new JsonObject();
//        requestBody.addProperty("access_token", domainConfig.getCmsAccessToken());
//        requestBody.add("request_body", null);
        Map<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("access_token", domainConfig.getCmsAccessToken());
        requestBody.put("request_body", null);
        String response = HttpUtils.post(domainConfig.getCmsGetBuInfoUrl(),requestBody,String.class);
        JsonObject jsonObject = gson.fromJson(response, JsonObject.class);
        JsonArray buInfos = jsonObject.get("data").getAsJsonArray();
        for (int i = 0; i < buInfos.size();i ++) {
            JsonObject buInfo = buInfos.get(i).getAsJsonObject();
            buId2BuCodeMap.put(buInfo.get("organizationId").getAsLong(),buInfo.get("code").getAsString());
        }
    }

    public List<FlowEntity> getFlowCostFromHickWall(String srcRegion, String dstRegion, Long endTime) throws IOException {
        Map<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("access_token", domainConfig.getCmsAccessToken());
        requestBody.put("request_body", null);
        String url = domainConfig.getFlowCostFromHickWall();
        String formatUrl = url + "?query=sum_over_time(fx.drc.traffic.statistic_rcount%7BsrcRegion=%22" + srcRegion
                + "%22,%20dstRegion=%22" + dstRegion + "%22%7D%5B1h%5D)&start=" + endTime + "&end=" + endTime
                + "&step=1&db=APM-FX";

        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        RequestBody body = RequestBody.create(JsonUtils.toJson(requestBody), JSON);
        Request request = new Request.Builder()
                .url(formatUrl)
                .method("POST", body)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();
        String responseStr = response.body().string();
        JsonObject jsonObject = JsonUtils.fromJson(responseStr, JsonObject.class);
        JsonObject data = jsonObject.get("data").getAsJsonObject();
        String result = JsonUtils.toJson(data.get("result"));

        List<FlowEntity> flowCosts= JsonUtils.fromJsonToList(result, FlowEntity.class);
        logger.info("get_flow_cot_from_hick_wall(size:{})", flowCosts.size());
        return flowCosts;
    }
}
