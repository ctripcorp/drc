package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.vo.display.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.mysql.MySQLToolsApiService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.ctrip.framework.drc.console.service.impl.AccessServiceImpl.*;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-07-28
 */
public class AccessServiceImplTest extends AbstractTest {

    Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private AccessServiceImpl accessService;

    @Mock
    private MhaServiceImpl mhaService;

    @Mock
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private DalServiceImpl dalService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MySQLToolsApiService mySQLToolsApiServiceImpl;
    @Mock
    private DbClusterApiService dbClusterApiServiceImpl;
    @Mock
    private DomainConfig domainConfig;
    
    
    private TransferServiceImpl transferService = new TransferServiceImpl();

    private MetaGenerator metaService = new MetaGenerator();

    private DrcMaintenanceServiceImpl drcMaintenanceServiceImpl = new DrcMaintenanceServiceImpl();

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        when(mhaService.getDcForMha("testClusterName")).thenReturn("shahq");
        doReturn(new DefaultEndPoint(CI_MYSQL_IP, CI_PORT1)).when(mhaService).getMasterMachineInstance("clustername");
        doReturn(CI_MYSQL_USER).when(monitorTableSourceProvider).getMonitorUserVal();
        doReturn(CI_MYSQL_PASSWORD).when(monitorTableSourceProvider).getMonitorPasswordVal();
        Mockito.when(dalService.getMhaList(any())).thenReturn(new HashMap<>());
        Mockito.when(mhaService.getDcForMha("mhatest")).thenReturn("shaoy");
    }

    @Test
    public void testApplyPreCheck() {
        String requestBody = "{\n" +
                "  \"clustername\": \"clustername\"\n" +
                "}";
        String defaultResponse = "{\"fail\": \"Unsupported HTTP method\"}";
        String successResponse = "{\"status\": \"\", \"message\": \"m_drcconsole be ready\"}";
        String errorResponse = "{\"status\": \"\", \"message\": \"clustername is Null\"}";
        
        try {
            Mockito.doReturn(objectMapper.readTree(defaultResponse)).when(mySQLToolsApiServiceImpl).applyPreCheck(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.applyPreCheck(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message"));
            Assert.assertEquals("F", map.get("status"));
            Assert.assertEquals("Unsupported HTTP method", map.get("message"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(successResponse)).when(mySQLToolsApiServiceImpl).applyPreCheck(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.applyPreCheck(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message") && map.containsKey("result"));
            Assert.assertEquals("T", map.get("status"));
            Assert.assertEquals("m_drcconsole be ready", map.get("message"));
            Map<String, Object> result = (Map<String, Object>) map.get("result");
            Assert.assertNotNull(result.get("noOnUpdate"));
            Assert.assertNotNull(result.get("noPkUk"));
            Assert.assertNotNull(result.get("gtidMode"));
            Assert.assertNotNull(result.get("binlogTransactionDependency"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(errorResponse)).when(mySQLToolsApiServiceImpl).applyPreCheck(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.applyPreCheck(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message"));
            Assert.assertEquals("F", map.get("status"));
            Assert.assertEquals("clustername is Null", map.get("message"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBuildMhaCluster(){
        String requestBody = "{\n" +
                "  \"clustername\": \"mhatest\",\n" +
                "  \"drczone\": \"sharb\",\n" +
                "  \"dalclustername\": \"test_dalcluster\",\n" +
                "  \"appid\": 100012345,\n" +
                "  \"bu\": \"BBZ\"\n" +
                "}";
        String buildMhaClusterRequestBody = "{\"clustername\":\"mhatest\",\"drczone\":\"sharb\"}";
        String defaultResponse = "{\"fail\": \"Unsupported HTTP method\"}";
        String wResponse = "{\n" +
                "  \"status\": \"W\",\n" +
                "  \"drcCluster\": \"mhatestsharb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String tResponse = "{\n" +
                "  \"status\": \"T\",\n" +
                "  \"drcCluster\": \"mhatestsharb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        
        try {
            Mockito.doReturn(objectMapper.readTree(defaultResponse)).when(mySQLToolsApiServiceImpl).buildMhaCluster(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.buildMhaCluster(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("drcCluster"));
            Assert.assertEquals("W", map.get("status"));
            Assert.assertEquals("Unsupported HTTP method", map.get("drcCluster"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(wResponse)).when(mySQLToolsApiServiceImpl).buildMhaCluster(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.buildMhaCluster(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("drcCluster"));
            Assert.assertEquals("W", map.get("status"));
            Assert.assertEquals("mhatestsharb", map.get("drcCluster"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(tResponse)).when(mySQLToolsApiServiceImpl).buildMhaCluster(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.buildMhaCluster(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("drcCluster"));
            Assert.assertEquals("T", map.get("status"));
            Assert.assertEquals("mhatestsharb", map.get("drcCluster"));

            MhaGroupPair mhaGroupPair = new MhaGroupPair();
            mhaGroupPair.setSrcMha("mhatest");
            mhaGroupPair.setDestMha("mhatestsharb");
            // make config for mhatest viewable
            drcMaintenanceServiceImpl.changeMhaGroupStatus(mhaGroupPair, 60);
            String allMetaData = metaService.getDrc().toString();
            System.out.println("allMetaData: " + allMetaData);
            Assert.assertTrue(allMetaData.contains("mhatest"));
            Assert.assertTrue(allMetaData.contains("mhatestsharb"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStopCheckNewMhaBuilt() {
        ScheduledFuture<?> scheduledFuture = accessService.checkNewMhaBuiltService.scheduleWithFixedDelay(() -> {
        }, 1, 1, TimeUnit.MINUTES);
        accessService.checkNewMhaBuiltMap.put("mhatest", scheduledFuture);
        ApiResult result = accessService.stopCheckNewMhaBuilt("mhatest");
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
        System.out.println(result.getData().toString());

        result = accessService.stopCheckNewMhaBuilt("mhatest");
        Assert.assertEquals(ResultCode.HANDLE_FAIL.getCode(), result.getStatus().intValue());
        System.out.println(result.getData().toString());
    }

    @Test
    public void testGetCopyResult() throws SQLException {
        String requestBody = "{\n" +
                "  \"OriginalCluster\": \"dbatmptest4\",\n" +
                "  \"DrcCluster\": \"dbatmptest4rb\",\n" +
                "  \"rplswitch\": 1\n" +
                "}";
        String defaultResponse = "{\"fail\": \"Unsupported HTTP method\"}";
        String fResponse = "{\"status\": \"F\", \"message\": \"Repl is not OK\"}";
        String tResponse = "{\"status\": \"T\", \"message\": \"Repl is OK\"}";
        
        try  {
            Mockito.doReturn(objectMapper.readTree(defaultResponse)).when(mySQLToolsApiServiceImpl).getCopyResult(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.getCopyResult(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message"));
            Assert.assertEquals("F", map.get("status"));
            Assert.assertEquals("Unsupported HTTP method", map.get("message"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(fResponse)).when(mySQLToolsApiServiceImpl).getCopyResult(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.getCopyResult(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message"));
            Assert.assertEquals("F", map.get("status"));
            Assert.assertEquals("Repl is not OK", map.get("message"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Mockito.doNothing().when(drcMaintenanceService).updateMhaGroup("dbatmptest4", "dbatmptest4rb", EstablishStatusEnum.CUT_REPLICATION);
        try {
            Mockito.doReturn(objectMapper.readTree(tResponse)).when(mySQLToolsApiServiceImpl).getCopyResult(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.getCopyResult(requestBody);
            Assert.assertTrue(map.containsKey("status") && map.containsKey("message"));
            Assert.assertEquals("T", map.get("status"));
            Assert.assertEquals("Repl is OK", map.get("message"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeployDns() throws Throwable {
        String requestBody = "{\n" +
                "  \"cluster\": \"clusterName\",\n" +
                "  \"dbnames\": \"dbName\",\n" +
                "  \"env\": \"product\",\n" +
                "  \"needread\": 0\n" +
                "}";
        String defaultResponse = "{\"fail\": \"Unsupported HTTP method\"}";
        String fResponse = "{\"success\": \"false\", \"content\": \"not done\"}";
        String tResponse = "{\"success\": \"true\", \"content\": \"done\"}";

        try {
            Mockito.doReturn(objectMapper.readTree(defaultResponse)).when(mySQLToolsApiServiceImpl).deployDns(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.deployDns(requestBody);
            Assert.assertTrue(map.containsKey("success") && map.containsKey("content"));
            Assert.assertEquals(false, map.get("success"));
            Assert.assertEquals("Unsupported HTTP method", map.get("content"));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        try {
            Mockito.doReturn(objectMapper.readTree(fResponse)).when(mySQLToolsApiServiceImpl).deployDns(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.deployDns(requestBody);
            Assert.assertTrue(map.containsKey("success") && map.containsKey("content"));
            Assert.assertEquals(false, map.get("success"));
            Assert.assertEquals("not done", map.get("content"));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Mockito.doNothing().when(drcMaintenanceService).updateMhaDnsStatus("clusterName", BooleanEnum.TRUE);
        try {
            Mockito.doReturn(objectMapper.readTree(tResponse)).when(mySQLToolsApiServiceImpl).deployDns(Mockito.any(),eq(requestBody));
            Map<String, Object> map = accessService.deployDns(requestBody);
            Assert.assertTrue(map.containsKey("success") && map.containsKey("content"));
            Assert.assertEquals(true, map.get("success"));
            Assert.assertEquals("done", map.get("content"));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testRegisterDalCluster() throws JsonProcessingException {
        String requestBody = "{\n" +
                "  \"dalclustername\": \"HtlInputRequestDB_dalcluster\",\n" +
                "  \"dbname\": \"HtlInputRequestDB\",\n" +
                "  \"clustername\": \"fatpub2\",\n" +
                "  \"isshard\": false,\n" +
                "  \"shardstartno\": 0\n" +
                "}";
        String response = "{\n" +
                "    \"fail\": 403\n" +
                "}";
        Mockito.doReturn(objectMapper.readTree(response)).when(dbClusterApiServiceImpl).registerDalCluster(Mockito.any(),Mockito.eq(requestBody), Mockito.eq("instance"));
        Map<String, Object> map = accessService.registerDalCluster(requestBody, "fat", "instance");
        Assert.assertNotNull(map);
        Assert.assertNotEquals(0, map.size());
    }

    @Test
    public void testReleaseDalCluster() {
        String tResponse = "{\"success\": \"true\", \"status\": 500,\"message\": \"testMessage\"}";
        try {
            Mockito.doReturn(objectMapper.readTree(tResponse)).when(dbClusterApiServiceImpl).releaseDalCluster(Mockito.any(),eq("bbzdrccameldb_dalcluster"));
            Map<String, Object> map = accessService.releaseDalCluster("bbzdrccameldb_dalcluster","fat");
            Assert.assertNotNull(map);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testQueryDbDnsHelper() throws IOException {
        Map<String, Object> map = new HashMap();
        String responseBody = "{\"content\":{\"slave\":[\"carisdcoreslave.mysql.db.t.com\",\"carisdcoreslave.mysql.shaoy.db.t.com\"],\"master\":[\"carisdcore.mysql.db.t.com\",\"carisdcore.mysql.shaoy.db.t.com\"]},\"success\":true}";
        JsonNode content = objectMapper.readTree(responseBody).get("content");
        accessService.queryDbDnsHelper(content, "master", "shaoy", map);
        accessService.queryDbDnsHelper(content, "slave", "shaoy", map);
        accessService.queryDbDnsHelper(content, "read", "shaoy", map);
        System.out.println(map);
        Assert.assertNotNull(map);
        Assert.assertNotEquals(0, map.size());
    }

//    @Test
    public void testGenerateMhaGroup() throws Exception {
        String requestBody = "{\n" +
                "  \"clustername\": \"testClusterName\",\n" +
                "  \"drczone\": \"shajq\"\n" +
                "}";
        Mockito.when(monitorTableSourceProvider.getReadUserVal()).thenReturn("root");
        Mockito.when(monitorTableSourceProvider.getReadPasswordVal()).thenReturn("root");
        Mockito.when(monitorTableSourceProvider.getWritePasswordVal()).thenReturn("root");
        Mockito.when(monitorTableSourceProvider.getWriteUserVal()).thenReturn("root");
        Mockito.when(monitorTableSourceProvider.getMonitorUserVal()).thenReturn("root");
        Mockito.when(monitorTableSourceProvider.getMonitorPasswordVal()).thenReturn("root");
//        accessService.generateMhaGroup(requestBody);
        Assert.assertNotNull(TableEnum.DC_TABLE.getAllPojos().stream().filter(p -> ("shahq".equalsIgnoreCase(((DcTbl) p).getDcName()))).findFirst().orElse(null));
        MhaTbl mhaTbl = (MhaTbl) TableEnum.MHA_TABLE.getAllPojos().stream().filter(p -> ("testClusterName".equalsIgnoreCase(((MhaTbl) p).getMhaName()))).findFirst().orElse(null);
        Assert.assertNotNull(mhaTbl);
        Assert.assertNotNull(mhaTbl.getMhaGroupId());
        Assert.assertNotNull(TableEnum.MHA_GROUP_TABLE.getAllPojos().stream().filter(p -> (mhaTbl.getMhaGroupId().equals(((MhaGroupTbl) p).getId()))).findFirst().orElse(null));
    }

    private void printMap(Map<String, Object> map) {
        logger.info("{");
        for(Map.Entry<String, Object> entry : map.entrySet()) {
            logger.info(entry.getKey() + ": " + entry.getValue());
        }
        logger.info("}");
    }

    @Test
    public void testGetUsersAndPasswords() throws IOException {

        String responseBodyWithoutUsersAndPassword = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\"\n" +
                "}";
        String responseBodyWithoutReadUser = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String responseBodyWithoutReadPassword = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String responseBodyWithoutWriteUser = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String responseBodyWithoutWritePassword = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String responseBodyWithoutMonitorUser = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";
        String responseBodyWithoutMonitorPassword = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\"\n" +
                "}";
        String responseBodyNormal = "{\n" +
                "  \"status\": \"W/T\",\n" +
                "  \"drcCluster\": \"dbatmptest4rb\",\n" +
                "  \"readUser\": \"dbaReadUser\",\n" +
                "  \"readPassword\": \"dbaReadPassword\",\n" +
                "  \"writeUser\": \"dbaWriteUser\",\n" +
                "  \"writePassword\": \"dbaWritePassword\",\n" +
                "  \"monitorUser\": \"dbaMonitorUser\",\n" +
                "  \"monitorPassword\": \"dbaMonitorPassword\"\n" +
                "}";

        Map<String, String> defaultUsersAndPasswords = new HashMap<>() {{
            put(READ_USER_KEY, "testReadUser");
            put(READ_PASSWORD_KEY, "testReadPassword");
            put(WRITE_USER_KEY, "testWriteUser");
            put(WRITE_PASSWORD_KEY, "testWritePassword");
            put(MONITOR_USER_KEY, "testMonitorUser");
            put(MONITOR_PASSWORD_KEY, "testMonitorPassword");
        }};
        Map<String, String> dbaUsersAndPasswords = new HashMap<>() {{
            put(READ_USER_KEY, "dbaReadUser");
            put(READ_PASSWORD_KEY, "dbaReadPassword");
            put(WRITE_USER_KEY, "dbaWriteUser");
            put(WRITE_PASSWORD_KEY, "dbaWritePassword");
            put(MONITOR_USER_KEY, "dbaMonitorUser");
            put(MONITOR_PASSWORD_KEY, "dbaMonitorPassword");
        }};

        Mockito.when(monitorTableSourceProvider.getReadUserVal()).thenReturn("testReadUser");
        Mockito.when(monitorTableSourceProvider.getReadPasswordVal()).thenReturn("testReadPassword");
        Mockito.when(monitorTableSourceProvider.getWriteUserVal()).thenReturn("testWriteUser");
        Mockito.when(monitorTableSourceProvider.getWritePasswordVal()).thenReturn("testWritePassword");
        Mockito.when(monitorTableSourceProvider.getMonitorUserVal()).thenReturn("testMonitorUser");
        Mockito.when(monitorTableSourceProvider.getMonitorPasswordVal()).thenReturn("testMonitorPassword");

        JsonNode root = null;
        Map<String, String> usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutUsersAndPassword);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutReadUser);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutReadPassword);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutWriteUser);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutWritePassword);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutMonitorUser);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyWithoutMonitorPassword);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertNotEquals(usersAndPasswords, dbaUsersAndPasswords);

        root = objectMapper.readTree(responseBodyNormal);
        usersAndPasswords = accessService.getUsersAndPasswords(root);
        Assert.assertNotEquals(usersAndPasswords, defaultUsersAndPasswords);
        Assert.assertEquals(usersAndPasswords, dbaUsersAndPasswords);
    }



}
