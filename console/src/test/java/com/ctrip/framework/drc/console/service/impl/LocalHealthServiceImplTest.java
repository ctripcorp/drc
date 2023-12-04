package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.service.beacon.BeaconApiService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-11-09
 */
public class LocalHealthServiceImplTest {

    Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private HealthServiceImpl healthService;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;
    
    @Mock
    private BeaconApiService beaconApiServiceImpl;

    private static final String MHA = "hs";

    private static final String TARGET_MHA = "targeths";

    private static final String TARGET_DC = "shaoy";

    private static final String LOCAL_DC = "sharb";

    private static final String DAL_CLUSTER_NAME = "dalClusterName";

    private static final String DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME = "drc";

    @Before
    public void setUp() {
        Map<String, String> map = new HashMap<>() {{
            put("shaoy", "http://console_oy");
            put("sharb", "http://console_eb");
        }};
        MockitoAnnotations.openMocks(this);
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        Mockito.when(sourceProvider.getMasterEndpoint(MHA)).thenReturn(new DefaultEndPoint("127.0.0.1", 12345, "root", null));
        Mockito.when(sourceProvider.getLocalDcName()).thenReturn(LOCAL_DC);
        Mockito.when(defaultConsoleConfig.getConsoleDcInfos()).thenReturn(map);
        Mockito.when(defaultConsoleConfig.getBeaconPrefix()).thenReturn("http://beanconprefix/");
    }

    @Test
    public void testGetRegisteredClusters() throws IOException {
        List<String> registeredClusters = healthService.getRegisteredClusters(DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
        logger.info("registeredClusters: {}", registeredClusters);
    }

    @Test
    public void testtest() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String responseBody = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"\",\n" +
                "  \"readPassword\": \"\",\n" +
                "  \"writeUser\": \"\",\n" +
                "  \"writePassword\": \"\",\n" +
                "  \"monitorUser\": \"\",\n" +
                "  \"monitorPassword\": \"\"\n" +
                "}";
        JsonNode root = objectMapper.readTree(responseBody);
        System.out.println(root.get("status").asText());

        JsonNode readUser = root.get("readUser");
        Assert.assertNotNull(readUser);
        Assert.assertEquals("", readUser.asText());
        Assert.assertTrue(readUser.asText().isEmpty());

        JsonNode nosuchkey = root.get("nosuchkey");
        Assert.assertNull(nosuchkey);

        System.out.println("root.asText: " + root.asText());
        System.out.println("asString: " + objectMapper.writeValueAsString(root));
    }

    @Test
    public void testtesttest() throws IOException {
        String requestBody = "{\n" +
                "  \"clustername\": \"集群名称\",\n" +
                "  \"drczone\": \"shajq\",\n" +
                "  \"dalclustername\": \"test_dalcluster\",\n" +
                "  \"appid\": 100012345\n" +
                "}";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode originalRequestBody = objectMapper.readTree(requestBody);

        ObjectNode requestBodyNode = objectMapper.createObjectNode();
        requestBodyNode.put("clustername", originalRequestBody.get("clustername").asText());
        requestBodyNode.put("drczone", originalRequestBody.get("drczone").asText());
        System.out.println(objectMapper.writeValueAsString(requestBodyNode));

    }

    @Test
    public void ttt() {
        print("hello", "world", "sydney");
    }

    private void print(String... vals) {
        System.out.println("vals: " + vals);
        for(String val : vals) {
            System.out.println(val);
        }
    }
}
