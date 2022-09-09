package com.ctrip.framework.drc.console.service.impl.openapi;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.vo.response.*;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by jixinwang on 2021/7/14
 */
@Service
public class OpenService {
    
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
    
    
}
