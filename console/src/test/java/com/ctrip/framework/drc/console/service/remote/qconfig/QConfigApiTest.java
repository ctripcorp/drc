package com.ctrip.framework.drc.console.service.remote.qconfig;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigRevertParam;
import com.ctrip.framework.drc.console.service.filter.impl.QConfigApiServiceImpl;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.UpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigVersionResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/4/18 10:43
 */
public class QConfigApiTest {

    private static String TOKEN = "4477DB21A5689602722E2F979FEBF347";
    private static String REST_API_URL = "http://qconfig.ctripcorp.com/restapi/";
    private static String ENV = Foundation.server().getEnv().getName().toLowerCase(); //fat
    private static String SUB_ENV = Foundation.server().getSubEnv() == null ? "" : Foundation.server().getSubEnv(); //LOCAL-RB-CONSOLE
    private static String GROUP_ID = Foundation.app().getAppId();
    private static String TARGET_GROUP_ID = "100023498";
    private static String TARGET_SUBENV = "FAT-RB-REPLICATOR";

    @Mock
    private DomainConfig domainConfig;
    @Spy
    private QConfigApiServiceImpl qConfigApiService = new QConfigApiServiceImpl();

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        Mockito.when(domainConfig.getQConfigAPIToken()).thenReturn(TOKEN);
        Mockito.when(domainConfig.getQConfigRestApiUrl()).thenReturn(REST_API_URL);
    }

    @Test
    public void testConfig() {
        System.out.println("groupid: " + Foundation.app().getAppId());
        System.out.println("env: " + Foundation.server().getEnv().getName().toLowerCase());
        System.out.println("subenv: " + (Foundation.server().getSubEnv() == null ? "" : Foundation.server().getSubEnv()));
    }

    @Test
    public void testQueryConfig() {
        System.out.println("groupid: " + Foundation.app().getAppId());
        System.out.println("env: " + Foundation.server().getEnv().getName().toLowerCase());
        System.out.println("subenv: " + (Foundation.server().getSubEnv() == null ? "" : Foundation.server().getSubEnv()));

        String url = "http://qconfig.ctripcorp.com/restapi/configs";
        String getUrl = url + "?token={token}" +
                "&groupid={groupid}" +
                "&dataid={dataid}" +
                "&env={env}" +
                "&subenv={subenv}" +
                "&targetgroupid={targetgroupid}";

        HashMap<String, Object> urlParams = Maps.newHashMap();
        urlParams.put("token", "4477DB21A5689602722E2F979FEBF347");
        urlParams.put("groupid", Foundation.app().getAppId());
        urlParams.put("dataid", "drc.properties");
        urlParams.put("env", Foundation.server().getEnv().getName().toLowerCase());
        urlParams.put("subenv", TARGET_SUBENV);
        urlParams.put("targetgroupid", TARGET_GROUP_ID);
        String result = HttpUtils.get(getUrl, String.class, urlParams);
//        QConfigDataResponse response = HttpUtils.get(getUrl, QConfigDataResponse.class, urlParams);
        System.out.println("result---------------------- \n" + result);
        QConfigDataResponse response = JsonUtils.fromJson(result, QConfigDataResponse.class);
        System.out.println("response------------------- \n" + response);
    }

    @Test
    public void testAddOrUpdateConfig() {
        String urlFormat = domainConfig.getQConfigRestApiUrl() +  "/properties/%s/envs/%s/subenvs/%s/configs/%s";
        String url = String.format(urlFormat, TARGET_GROUP_ID, ENV, TARGET_SUBENV, "drc.properties");
        System.out.println(url);
        String postUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";

        HashMap<String, Object> urlParams = new HashMap<>();
        urlParams.put("token", TOKEN);
        urlParams.put("operator", "dengquanliang");
        urlParams.put("serverenv", ENV);
        urlParams.put("groupid", GROUP_ID);

        UpdateRequestBody requestBody = new UpdateRequestBody();
        requestBody.setVersion(16);
        Map<String, String> data = new HashMap<>();
        requestBody.setData(data);
        data.put("test1", "test1");
        data.put("key11", "val11");

        UpdateQConfigResponse response = HttpUtils.post(postUrl, requestBody, UpdateQConfigResponse.class, urlParams);
        System.out.println(response);
    }

    @Test
    public void testQueryConfigVersion() {
        String urlFormat = domainConfig.getQConfigRestApiUrl() +  "/configs/%s/envs/%s/subenvs/%s/versions";
        String url = String.format(urlFormat, GROUP_ID, ENV, SUB_ENV, "dql.properties");
        String getUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}&targetdataids={targetdataids}";
        Map<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", TOKEN);
        urlParams.put("operator", "dengquanliang");
        urlParams.put("serverenv", ENV);
        urlParams.put("groupid", GROUP_ID);
        urlParams.put("targetdataids", "test1.properties");
        QConfigVersionResponse response = HttpUtils.get(getUrl, QConfigVersionResponse.class, urlParams);
        System.out.println(response);
    }

    @Test
    public void testRevertConfig() {
        QConfigRevertParam param = new QConfigRevertParam();
        param.setToken(TOKEN);
        param.setOperator("");
        param.setServerEnv(ENV);
        param.setGroupId(GROUP_ID);
        param.setTargetGroupId(GROUP_ID);
        param.setTargetEnv(ENV);
        param.setTargetSubEnv(SUB_ENV);
        param.setTargetDataId("test.properties");
        param.setVersion(12);

        UpdateQConfigResponse response = qConfigApiService.revertConfig(param);
        System.out.println(response);

    }
}
