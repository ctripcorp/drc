package com.ctrip.framework.drc.console.service.v2.external.dba;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.v2.external.dba.response.*;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DbaApiServiceImplV2Test {

    private AutoCloseable closeable;

    @InjectMocks
    private DbaApiServiceImplV2 dbaApiService;
    @Mock
    private DomainConfig domainConfig;
    @Mock
    private DefaultConsoleConfig consoleConfig;


    @Before
    public void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        when(consoleConfig.getMySQLApiV2Switch()).thenReturn(true);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testGetClusterMembersInfo() {
        String clusterName = "xxxdb_dalcluster";
        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            String data = "[\n" +
                    "    {\n" +
                    "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                    "      \"machineName\": \"SVR33106IN5112\",\n" +
                    "      \"ipBusiness\": \"10.109.216.82\",\n" +
                    "      \"clusterPort\": 55945,\n" +
                    "      \"role\": \"master\",\n" +
                    "      \"status\": \"online\",\n" +
                    "      \"dataDir\": \"/data/mysql55945/\",\n" +
                    "      \"version\": \"8.0.36\",\n" +
                    "      \"machineLocated\": \"上海新源IDC(移动)\",\n" +
                    "      \"zone\": \"shaxy\",\n" +
                    "      \"OSVersion\": \"AlmaLinux release 9.1 (Lime Lynx)\",\n" +
                    "      \"insertTime\": \"2024-07-25T12:40:13+08:00\",\n" +
                    "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                    "      \"machineName\": \"SVR32258IN5112\",\n" +
                    "      \"ipBusiness\": \"10.58.235.208\",\n" +
                    "      \"clusterPort\": 55945,\n" +
                    "      \"role\": \"slave\",\n" +
                    "      \"status\": \"online\",\n" +
                    "      \"dataDir\": \"/data/mysql55945/\",\n" +
                    "      \"version\": \"8.0.36\",\n" +
                    "      \"machineLocated\": \"上海日阪IDC(联通)\",\n" +
                    "      \"zone\": \"sharb\",\n" +
                    "      \"OSVersion\": \"AlmaLinux release 9.1 (Lime Lynx)\",\n" +
                    "      \"insertTime\": \"2024-07-25T12:40:13+08:00\",\n" +
                    "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"clusterName\": \"bbzhistoricalorderindex102\",\n" +
                    "      \"machineName\": \"VMSALI838026\",\n" +
                    "      \"ipBusiness\": \"10.25.224.150\",\n" +
                    "      \"clusterPort\": 55945,\n" +
                    "      \"role\": \"slave-dr\",\n" +
                    "      \"status\": \"online\",\n" +
                    "      \"dataDir\": \"/data/mysql55945/\",\n" +
                    "      \"version\": \"8.0.36\",\n" +
                    "      \"machineLocated\": \"公有云(SHA-ALI)\",\n" +
                    "      \"zone\": \"sha-ali\",\n" +
                    "      \"OSVersion\": \"Alibaba Cloud Linux release 3 (Soaring Falcon)\",\n" +
                    "      \"insertTime\": \"2024-07-25T16:40:13+08:00\",\n" +
                    "      \"modifyTime\": \"2024-08-01T13:40:20+08:00\"\n" +
                    "    }\n" +
                    "  ]";
            Map<String, Object> jsonObject = new HashMap<>();
            jsonObject.put("data", JsonUtils.parseArray(data));
            jsonObject.put("success", "true");
            jsonObject.put("message", "ok");
            String responseJson = JsonUtils.toJson(jsonObject);
            mocked.when(() -> HttpUtils.get(anyString(), any(), anyMap())).thenReturn(responseJson);

            DbaClusterInfoResponse clusterMembersInfo = dbaApiService.getClusterMembersInfo(clusterName);
            Data data1 = clusterMembersInfo.getData();
            List<MemberInfo> memberlist = data1.getMemberlist();
            Assert.assertEquals(3, memberlist.size());
            Assert.assertEquals(1, memberlist.stream().filter(e -> e.getRole().equals("master")).count());
            Assert.assertEquals("10.109.216.82", memberlist.stream().filter(e -> e.getRole().equals("master")).map(MemberInfo::getService_ip).findFirst().get());
            Assert.assertEquals("10.109.216.82", memberlist.stream().filter(e -> e.getRole().equals("master")).map(MemberInfo::getIp_business).findFirst().get());
        }
    }

    @Test
    public void testGetDatabaseClusterInfo() {
        String clusterName = "xxxdb_dalcluster";
        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            String data = "[\n" +
                    "        {\n" +
                    "            \"clusterList\": [\n" +
                    "                {\n" +
                    "                    \"clusterName\": \"fat-bbz-pub19\",\n" +
                    "                    \"env\": \"fat\",\n" +
                    "                    \"zoneId\": \"NTGXH\",\n" +
                    "                    \"nodes\": [\n" +
                    "                        {\n" +
                    "                            \"instancePort\": 55111,\n" +
                    "                            \"instanceZoneId\": \"ntgxh\",\n" +
                    "                            \"role\": \"master\",\n" +
                    "                            \"ipBusiness\": \"10.120.16.198\"\n" +
                    "                        },\n" +
                    "                        {\n" +
                    "                            \"instancePort\": 55111,\n" +
                    "                            \"instanceZoneId\": \"ntgxh\",\n" +
                    "                            \"role\": \"slave\",\n" +
                    "                            \"ipBusiness\": \"10.120.16.199\"\n" +
                    "                        }\n" +
                    "                    ]\n" +
                    "                }\n" +
                    "            ],\n" +
                    "            \"dbName\": \"bbzorderindexbakshard01db\"\n" +
                    "        }\n" +
                    "    ]";
            Map<String, Object> jsonObject = new HashMap<>();
            jsonObject.put("data", JsonUtils.parseArray(data));
            jsonObject.put("success", "true");
            jsonObject.put("message", "ok");
            String responseJson = JsonUtils.toJson(jsonObject);
            mocked.when(() -> HttpUtils.get(anyString(), any(), anyMap())).thenReturn(responseJson);

            List<ClusterInfoDto> list = dbaApiService.getDatabaseClusterInfo(clusterName);
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            ClusterInfoDto dbClusterInfoDto = list.get(0);
            Assert.assertNotNull(dbClusterInfoDto);
            List<ClusterInfoDto.Node> nodes = dbClusterInfoDto.getNodes();
            Assert.assertFalse(nodes.isEmpty());
            Assert.assertEquals(2, nodes.size());
        }
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
            mocked.when(() -> HttpUtils.get(anyString(), any(), anyMap())).thenReturn(responseJson);

            List<DbClusterInfoDto> list = dbaApiService.getDatabaseClusterInfoList(clusterName);
            Assert.assertNotNull(list);
            Assert.assertEquals(2, list.size());
            for (DbClusterInfoDto dbClusterInfoDto : list) {
                Assert.assertNotNull(dbClusterInfoDto.getClusterList());
                Assert.assertFalse(dbClusterInfoDto.getClusterList().isEmpty());
                for (ClusterInfoDto clusterInfoDto : dbClusterInfoDto.getClusterList()) {
                    Assert.assertFalse(clusterInfoDto.getNodes().isEmpty());
                }
            }
        }
    }

}