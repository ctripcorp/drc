package com.ctrip.framework.drc.console.service.v2.external.dba;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.SQLDigestInfo;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.SQLDigestInfo.Content;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.SQLDigestInfo.Digest;
import com.ctrip.framework.drc.console.utils.DateUtils;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.user.UserService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

public class DbaApiServiceTest {

    private AutoCloseable closeable;
    @InjectMocks
    private DbaApiServiceImpl dbaApiService;

    @Mock
    private DomainConfig domainConfig;
    @Mock
    private UserService userService;
    @Mock
    private DefaultConsoleConfig consoleConfig;


    @Before
    public void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testGetDatabaseClusterInfoList() {
        String clusterName = "xxxdb_dalcluster";
        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            String data = "[{\"dbName\":\"xxxshard01db\",\"clusterList\":[{\"clusterName\":\"mha1\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"SHAXY\",\"role\":\"master\",\"ipBusiness\":\"10.10.10.10\"}],\"env\":\"test\",\"zoneId\":\"SHAXY\"},{\"clusterName\":\"fra1\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"fra-aws\",\"role\":\"master\",\"ipBusiness\":\"xxx.rds.amazonaws.com\"}],\"env\":\"test\",\"zoneId\":\"fra-aws\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sinbbzmemberpub.cnd4gptdxrgp.ap-southeast-1.rds.amazonaws.com\"}],\"env\":\"pro\",\"zoneId\":\"sin-aws\"}]},{\"dbName\":\"xxxshard02db\",\"clusterList\":[{\"clusterName\":\"mha2\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"SHAXY\",\"role\":\"master\",\"ipBusiness\":\"10.10.10.10\"}],\"env\":\"test\",\"zoneId\":\"SHAXY\"},{\"clusterName\":\"fra1\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"fra-aws\",\"role\":\"master\",\"ipBusiness\":\"xxx.rds.amazonaws.com\"}],\"env\":\"test\",\"zoneId\":\"fra-aws\"},{\"clusterName\":\"sin1\",\"nodes\":[{\"instancePort\":55944,\"instanceZoneId\":\"sin-aws\",\"role\":\"master\",\"ipBusiness\":\"sinbbzmemberpub.cnd4gptdxrgp.ap-southeast-1.rds.amazonaws.com\"}],\"env\":\"pro\",\"zoneId\":\"sin-aws\"}]}]";
            Map<String, Object> jsonObject = new HashMap<>();
            jsonObject.put("data", JsonUtils.parseArray(data));
            jsonObject.put("success", "true");
            jsonObject.put("message", "ok");
            String responseJson = JsonUtils.toJson(jsonObject);
            mocked.when(() -> HttpUtils.post(anyString(), any(), any())).thenReturn(responseJson);

            List<DbClusterInfoDto> list = dbaApiService.getDatabaseClusterInfoList(clusterName);
            Assert.assertNotNull(list);
            Assert.assertEquals(2, list.size());
            for (DbClusterInfoDto dbClusterInfoDto : list) {
                Assert.assertNotNull(dbClusterInfoDto.getClusterList());
                Assert.assertTrue(dbClusterInfoDto.getClusterList().size() > 0);
                for (ClusterInfoDto clusterInfoDto : dbClusterInfoDto.getClusterList()) {
                    Assert.assertTrue(clusterInfoDto.getNodes().size() > 0);
                }
            }
        }
    }

    @Test
    public void testGetDBsWithQueryPermission() {
        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            Mockito.when(domainConfig.getDotToken()).thenReturn("token");
            Mockito.when(domainConfig.getDotQueryApiUrl()).thenReturn("http://localhost:8080/dot/api/query");
            Mockito.when(userService.getInfo()).thenReturn("user_name");
            String responseJson = "{\"data\":[\"db1\",\"db2\"],\"success\":true,\"message\":\"ok\"}";
            mocked.when(() -> HttpUtils.post(anyString(), any(), any())).thenReturn(responseJson);
            int size = dbaApiService.getDBsWithQueryPermission().size();
            Assert.assertEquals(2, size);
        }
    }
    
    @Test
    public void test() {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - 1000 * 60 * 60 * 24 * 7;
        System.out.println(DateUtils.longToString(startTime, "yyyy-MM-dd HH:mm"));
        System.out.println(DateUtils.longToString(endTime,  "yyyy-MM-dd HH:mm"));
    }
    
    @Test
    public void testEverUserTraffic() {
        when(consoleConfig.getCenterRegion()).thenReturn("sha");
        when(domainConfig.getOverSeaUserDMLQueryToken()).thenReturn("token1");
        when(domainConfig.getOverSeaUserDMLQueryUrl()).thenReturn("http://url1");
        when(domainConfig.getCenterRegionUserDMLCountQueryToken()).thenReturn("token2");
        when(domainConfig.getCenterRegionUserDMLCountQueryUrl()).thenReturn("http://url2");
        try (MockedStatic<HttpUtils> theMock = mockStatic(HttpUtils.class)) {
            theMock.when(() ->HttpUtils.post(eq("http://url1"), any(), any())).thenReturn("{\n"
                    + "    \"success\": true,\n"
                    + "    \"content\": {\n"
                    + "        \"select\": {},\n"
                    + "        \"write\": {\n"
                    + "            \"digest\": \"9f8d1da2a33fdb83616827b2a8398261391a3b8b4c9b56a4b531b97451150446\",\n"
                    + "            \"digest_sql\": \"UPDATE `table` SET `badge` = ? WHERE `uid` = ? AND `cid` = ? AND `type` = ?\",\n"
                    + "            \"datachange_lasttime\": \"2024-02-22 14:45:10\"\n"
                    + "        }\n"
                    + "    }\n"
                    + "}");

            boolean shaEver = dbaApiService.everUserTraffic("sin", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertTrue(shaEver);

            theMock.when(() ->HttpUtils.post(eq("http://url1"), any(), any())).thenReturn("{\n"
                    + "    \"success\": false,\n"
                    + "    \"content\": \"no sql\"\n"
                    + "}");
            shaEver = dbaApiService.everUserTraffic("sin", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertFalse(shaEver);

            theMock.when(() ->HttpUtils.post(eq("http://url1"), any(), any())).thenReturn("{\n"
                    + "    \"success\": true,\n"
                    + "    \"content\": {\n"
                    + "        \"select\": {"
                    + "            \"digest\": \"9f8d1da2a33fdb83616827b2a8398261391a3b8b4c9b56a4b531b97451150446\",\n"
                    + "            \"digest_sql\": \"select `table` SET `badge` = ? WHERE `uid` = ? AND `cid` = ? AND `type` = ?\",\n"
                    + "            \"datachange_lasttime\": \"2024-02-22 14:45:10\"\n"
                    + "         },\n"
                    + "        \"write\": {}\n"
                    + "    }\n"
                    + "}");

            shaEver = dbaApiService.everUserTraffic("sin", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertFalse(shaEver);
            
            
        }

        try (MockedStatic<HttpUtils> theMock = mockStatic(HttpUtils.class)) {
            theMock.when(() ->HttpUtils.post(eq("http://url2"), any(), any())).thenReturn("{\n"
                    + "    \"success\": true,\n"
                    + "    \"content\": {\n"
                    + "        \"seeks\": 5,\n"
                    + "        \"scans\": 5,\n"
                    + "        \"insert\": 5,\n"
                    + "        \"update\": 5,\n"
                    + "        \"delete\": 5\n"
                    + "    }\n"
                    + "}");

            boolean shaEver = dbaApiService.everUserTraffic("sha", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertTrue(shaEver);

            theMock.when(() ->HttpUtils.post(eq("http://url2"), any(), any())).thenReturn("{\n"
                    + "    \"success\": true,\n"
                    + "    \"content\": {\n"
                    + "        \"seeks\": 5,\n"
                    + "        \"scans\": 5,\n"
                    + "        \"insert\": 0,\n"
                    + "        \"update\": 0,\n"
                    + "        \"delete\": 0\n"
                    + "    }\n"
                    + "}");
            shaEver = dbaApiService.everUserTraffic("sha", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertFalse(shaEver);

            theMock.when(() ->HttpUtils.post(eq("http://url2"), any(), any())).thenReturn("{\n"
                    + "    \"success\": false,\n"
                    + "    \"content\": \"error\"\n"
                    + "}");
            shaEver = dbaApiService.everUserTraffic("sha", "db1", "table1",
                    System.currentTimeMillis() - 60 * 60 * 24 * 7, System.currentTimeMillis(), false);
            Assert.assertTrue(shaEver);

            SQLDigestInfo sqlDigestInfo = new SQLDigestInfo();
            Content content = new Content();
            Digest digest = new Digest();
            sqlDigestInfo.setSuccess(true);
            sqlDigestInfo.setContent(content);
            content.setSelect(digest);
            digest.setDigest("digest");
            digest.setDigest_sql("sql");
            digest.setDatachange_lasttime("2024-02-22 14:45:10");
            
            sqlDigestInfo.getContent();
            sqlDigestInfo.getContent().getSelect();
            sqlDigestInfo.getContent().getSelect().getDigest();
            sqlDigestInfo.getContent().getSelect().getDigest_sql();
            sqlDigestInfo.getContent().getSelect().getDatachange_lasttime();

        }
    }

}