package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dto.FailoverDto;
import com.ctrip.framework.drc.console.monitor.delay.DelayMap;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.beacon.BeaconApiService;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.core.service.beacon.RegisterDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static com.ctrip.framework.drc.console.service.impl.HealthServiceImpl.HEALTH_IS_UPATING;
import static com.ctrip.framework.drc.console.service.impl.HealthServiceImpl.SILENT_CAPACITY;
import static com.ctrip.framework.drc.core.service.beacon.BeaconResult.BEACON_FAIL_WITHOUT_RETRY;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-27
 */
public class HealthServiceImplTest {

    Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private HealthServiceImpl healthService;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Mock
    private DalServiceImpl dalService;

    @Mock
    private BeaconApiService beaconApiServiceImpl;
    

    private ObjectMapper objectMapper = new ObjectMapper();

    private DelayMap delayMap = DelayMap.getInstance();

    private static final String MHA = "hs";

    private static final String TARGET_MHA = "targeths";

    private static final String TARGET_DC = "shaoy";

    private static final String SHAOY = "shaoy";

    private static final String LOCAL_DC = "sharb";

    private static final String SHARB = "sharb";

    private static final String DAL_CLUSTER_NAME = "dalClusterName";

    private static final String SHARD1OY = "shard1oy";

    private static final String SHARD2OY = "shard2oy";

    private static final String SHARD1RB = "shard1rb";

    private static final String SHARD2RB = "shard2rb";

    private static final List<String> SHARD1OY_NODES = Arrays.asList("11oy", "12oy");

    private static final List<String> SHARD2OY_NODES = Arrays.asList("21oy", "22oy");

    private static final List<String> SHARD1RB_NODES = Arrays.asList("11rb", "12rb");

    private static final List<String> SHARD2RB_NODES = Arrays.asList("21rb", "22rb");

    private static final Map<String, String> EXTRA = Maps.newConcurrentMap();

    private static final String DAL_SUCCESS_RESPONSE = "{\"status\":200,\"message\":\"Transform to drc cluster success.\",\"result\":null}";

    private static final String DAL_FAIL_RESPONSE = "{\"status\":500,\"message\":\"Unknown Exception, caused by: cluster does not exists.\",\"result\":null}";

    private static final String DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME = "drc";

    @Before
    public void setUp() throws Exception {

        Map<String, String> map = new HashMap<>() {{
            put("shaoy", "http://console_oy");
            put("sharb", "http://console_rb");
        }};
        MockitoAnnotations.openMocks(this);
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        Mockito.when(sourceProvider.getMasterEndpoint(MHA)).thenReturn(new DefaultEndPoint("127.0.0.1", 12345, "root", null));
        Mockito.when(sourceProvider.getLocalDcName()).thenReturn(LOCAL_DC);
        Mockito.when(defaultConsoleConfig.getConsoleDcInfos()).thenReturn(map);
        Mockito.when(defaultConsoleConfig.getBeaconPrefix()).thenReturn("http://beaconprefix/");
    }

    @Test
    public void testIsDelayNormal() {
        DelayMap.DrcDirection drcDirection = new DelayMap.DrcDirection(TARGET_MHA, MHA);
        String uri = String.format("http://console_oy" + HEALTH_IS_UPATING, TARGET_MHA);

        delayMap.put(drcDirection, 4999);
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertTrue(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }

        delayMap.put(drcDirection, 5100);
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertTrue(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(int i = 0; i < SILENT_CAPACITY; ++i) {
            delayMap.put(drcDirection, 4999);
        }
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertTrue(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(int i = 0; i < SILENT_CAPACITY; ++i) {
            delayMap.put(drcDirection, 5100);
        }
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(false));
            Assert.assertTrue(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertFalse(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }

        delayMap.clear(drcDirection);
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertTrue(healthService.isDelayNormal(DAL_CLUSTER_NAME, MHA));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDoFailover() throws Exception {
        Mockito.when(dalService.switchDalClusterType(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenReturn(ApiResult.getInstance("", 200, "Transform to drc cluster success."));
        List<FailoverDto.NodeStatusInfo> groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, true, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, false, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, false, SHARB, SHARD2RB_NODES));
        }};
        FailoverDto failoverDto = new FailoverDto(DAL_CLUSTER_NAME, Arrays.asList(SHARD1OY, SHARD2OY), Arrays.asList(SHARD1RB, SHARD2RB), groups, new ArrayList<>(), true, EXTRA);
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), healthService.doFailover(failoverDto).getCode().intValue());

        groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, true, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, true, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, false, SHARB, SHARD2RB_NODES));
        }};
        failoverDto.setGroups(groups);
        Assert.assertEquals(BEACON_FAIL_WITHOUT_RETRY, healthService.doFailover(failoverDto).getCode().intValue());

        groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, false, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, false, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, false, SHARB, SHARD2RB_NODES));
        }};
        failoverDto.setGroups(groups);
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), healthService.doFailover(failoverDto).getCode().intValue());

        groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, false, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, true, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, false, SHARB, SHARD2RB_NODES));
        }};
        failoverDto.setGroups(groups);
        Assert.assertEquals(BEACON_FAIL_WITHOUT_RETRY, healthService.doFailover(failoverDto).getCode().intValue());

        groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, true, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, true, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, true, SHARB, SHARD2RB_NODES));
        }};
        failoverDto.setGroups(groups);
        Assert.assertEquals(BEACON_FAIL_WITHOUT_RETRY, healthService.doFailover(failoverDto).getCode().intValue());

        groups = new ArrayList<>() {{
            add(new FailoverDto.NodeStatusInfo(SHARD1OY, true, SHAOY, SHARD1OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2OY, true, SHAOY, SHARD2OY_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD1RB, false, SHARB, SHARD1RB_NODES));
            add(new FailoverDto.NodeStatusInfo(SHARD2RB, false, SHARB, SHARD2RB_NODES));
        }};
        failoverDto.setGroups(groups);
        Mockito.when(dalService.switchDalClusterType(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenReturn(ApiResult.getInstance("", 500, "Unknown Exception, caused by: cluster does not exists."));
        Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), healthService.doFailover(failoverDto).getCode().intValue());

        Mockito.when(dalService.switchDalClusterType(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.anyString())).thenThrow(new Exception("dal exception"));
        Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), healthService.doFailover(failoverDto).getCode().intValue());
    }

    @Test
    public void testIsLocalDcUpdating() throws SQLException, InterruptedException {
        WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(new DefaultEndPoint("127.0.0.1", 12345, "root", null));
        try {
            sqlOperatorWrapper.initialize();
            sqlOperatorWrapper.start();
        } catch (Exception e) {
            logger.error("sqlOperatorWrapper initialize: ", e);
        }

        Assert.assertFalse(healthService.isLocalDcUpdating(MHA));

        String sql = String.format("REPLACE INTO `drcmonitordb`.`delaymonitor`(`id`, `src_ip`, `dest_ip`) VALUES(%s, '%s', '%s');", 1, LOCAL_DC, LOCAL_DC);
        GeneralSingleExecution execution = new GeneralSingleExecution(sql);
        sqlOperatorWrapper.insert(execution);
        Assert.assertTrue(healthService.isLocalDcUpdating(MHA));

        Thread.sleep(1600);
        Assert.assertFalse(healthService.isLocalDcUpdating(MHA));

        try {
            sqlOperatorWrapper.stop();
            sqlOperatorWrapper.dispose();
        } catch (Exception e) {
            logger.error("sqlOperatorWrapper stop: ", e);
        }
    }

    @Test
    public void testIsTargetDcUpdating() {
        logger.info("case 1");
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(null);
        Assert.assertFalse(healthService.isTargetDcUpdating(MHA));
        logger.info("case 1 succeeded");

        logger.info("case 2");
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList("nosuchdc", TARGET_MHA));
        Assert.assertFalse(healthService.isTargetDcUpdating(MHA));
        logger.info("case 2 succeeded");

        logger.info("case 3");
        Mockito.when(sourceProvider.getTargetDcMha(MHA)).thenReturn(Arrays.asList(TARGET_DC, TARGET_MHA));
        String uri = String.format("http://console_oy" + HEALTH_IS_UPATING, TARGET_MHA);
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenReturn(ApiResult.getSuccessInstance(true));
            Assert.assertTrue(healthService.isTargetDcUpdating(MHA));
            logger.info("case 3 succeeded");
        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("case 4");
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when((MockedStatic.Verification) HttpUtils.get(uri)).thenThrow(new Exception("request failed"));
            Assert.assertFalse(healthService.isTargetDcUpdating(MHA));
            logger.info("case 4 succeeded");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDoRegister() {
        String dalCluster = "bbzdrccameldb_dalcluster";
        List<RegisterDto.NodeGroup> nodeGroups = new ArrayList<>() {{
            add(new RegisterDto.NodeGroup("fat-fx-drc1", "ntgxh", Arrays.asList("10.2.72.230:55111", "10.2.72.247:55111")));
            add(new RegisterDto.NodeGroup("fat-fx-drc2", "stgxh", Arrays.asList("10.2.72.246:55111", "10.2.72.248:55111")));
        }};
        RegisterDto.Extra extra = new RegisterDto.Extra();
        extra.setUsername("user");
        extra.setPassword("psw");
        RegisterDto registerDto = new RegisterDto(nodeGroups, extra);
        

        try {
            Mockito.doReturn(BeaconResult.getSuccessInstance("")).when(beaconApiServiceImpl).doRegister(Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.any());
            BeaconResult result = healthService.doRegister(dalCluster, registerDto, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertEquals(0, result.getCode().intValue());
            Assert.assertTrue("success".equalsIgnoreCase(result.getMsg()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(BeaconResult.getFailInstance("")).when(beaconApiServiceImpl).doRegister(Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.any());
            BeaconResult result = healthService.doRegister(dalCluster, registerDto, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertEquals(1, result.getCode().intValue());
            Assert.assertTrue("failure".equalsIgnoreCase(result.getMsg()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeRegister() {
        List<String> shouldDeRegisterDalClusters = Arrays.asList("bbzdrcbenchmarkdb_dalcluster");
        try{
            Mockito.doReturn(BeaconResult.getSuccessInstance("")).when(beaconApiServiceImpl).deRegister(Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
            List<String> actual = healthService.deRegister(shouldDeRegisterDalClusters, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertNotNull(actual);
            Assert.assertEquals(1, actual.size());
            Assert.assertTrue(actual.contains("bbzdrcbenchmarkdb_dalcluster"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
           Mockito.doReturn(BeaconResult.getFailInstance("")).when(beaconApiServiceImpl).deRegister(Mockito.anyString(),Mockito.anyString(),Mockito.anyString());
            List<String> actual = healthService.deRegister(shouldDeRegisterDalClusters, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertNotNull(actual);
            Assert.assertEquals(0, actual.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGetRegisteredClusters()  {
        try{
            String response = "{\"code\":0," +
                    "\"msg\":\"success\"," +
                    "\"data\":" +
                    "[             \"bbzdrccameldb_dalcluster\"," +
                                 "\"bbzdrcbenchmarkdb_dalcluster\"," +
                                "\"ibuitineraryshardbasedb_dalcluster\"]}";
            JsonNode dataNode = objectMapper.readTree(response).get("data");
            BeaconResult beaconResult = BeaconResult.getSuccessInstance(objectMapper.readValue(dataNode.traverse(), new TypeReference<ArrayList<String>>() {}));
            Mockito.doReturn(beaconResult).when(beaconApiServiceImpl).getRegisteredClusters(Mockito.anyString(),Mockito.anyString());
            List<String> registeredClusters = healthService.getRegisteredClusters(DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertEquals(3, registeredClusters.size());
            List<String> expected = Arrays.asList("bbzdrccameldb_dalcluster", "bbzdrcbenchmarkdb_dalcluster", "ibuitineraryshardbasedb_dalcluster");
            registeredClusters.forEach(registeredCluster -> Assert.assertTrue(expected.contains(registeredCluster)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(BeaconResult.getFailInstance(null)).when(beaconApiServiceImpl).getRegisteredClusters(Mockito.anyString(),Mockito.anyString());
            List<String> registeredClusters = healthService.getRegisteredClusters(DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
            Assert.assertEquals(0, registeredClusters.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {

    }

}