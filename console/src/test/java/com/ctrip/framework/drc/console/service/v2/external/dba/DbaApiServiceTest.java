package com.ctrip.framework.drc.console.service.v2.external.dba;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.ClusterInfoDto;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.DbClusterInfoDto;
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

public class DbaApiServiceTest {

    private AutoCloseable closeable;
    @InjectMocks
    private DbaApiServiceImpl dbaApiService;

    @Mock
    private DomainConfig domainConfig;
    @Mock
    private UserService userService;


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
    
}