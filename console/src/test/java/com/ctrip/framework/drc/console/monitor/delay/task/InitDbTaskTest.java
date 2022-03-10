package com.ctrip.framework.drc.console.monitor.delay.task;

import com.ctrip.framework.drc.console.AllTests;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.impl.execution.GeneralSingleExecution;
import com.ctrip.framework.drc.console.monitor.delay.impl.operator.WriteSqlOperatorWrapper;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.vo.response.MhaResponseVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by jixinwang on 2021/7/27
 */
public class InitDbTaskTest extends TestCase {

    private Dc dc;

    private Drc drc;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private OpenService openService;

    @InjectMocks
    private InitDbTask initDbTask;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        String drcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"null\">\n" +
                "                    <db ip=\"127.0.0.1\" port=\"12345\" master=\"true\" uuid=\"bd9b313c-b56d-11ea-825d-fa163e02998c\"/>\n" +
                "                    <db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"b73fd53a-b56d-11ea-ac10-fa163ec90ff6\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <replicator ip=\"10.2.63.130\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"sharb\">\n" +
                "        <clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.83.114:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcRb\" name=\"integration-test\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "                    <db ip=\"10.2.83.107\" port=\"3306\" master=\"true\" uuid=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69\"/>\n" +
                "                    <db ip=\"10.2.83.108\" port=\"3306\" master=\"false\" uuid=\"a4f134a7-b56d-11ea-a7ab-fa163e2d32b8\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <replicator ip=\"10.2.86.199\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"shali\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcAli\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"null\">\n" +
                "                    <db ip=\"127.0.0.1\" port=\"12345\" master=\"true\" uuid=\"bd9b313c-b56d-11ea-825d-fa163e02998c\"/>\n" +
                "                    <db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"b73fd53a-b56d-11ea-ac10-fa163ec90ff6\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <replicator ip=\"10.2.63.130\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "</drc>";
        drc = DefaultSaxParser.parse(drcXmlStr);

        Mockito.when(consoleConfig.getPublicCloudDc()).thenReturn(Sets.newHashSet("shali"));

        Map<String, String> consoleDcInfos = Maps.newHashMap();
        consoleDcInfos.put("shaoy", "http://console_oy");
        Mockito.when(consoleConfig.getConsoleDcInfos()).thenReturn(consoleDcInfos);
//        Mockito.when(consoleConfig.getGrayMha()).thenReturn(Sets.newHashSet("drcOy","drcAli"));
//        Mockito.when(consoleConfig.getGrayMhaSwitch()).thenReturn("on");
        Mockito.when(monitorTableSourceProvider.getDeleteRedundantDelayMonitorRecordSwitch()).thenReturn("on");

//        AllTests.init();
    }

    @Test
    public void testInitPrivateCloudDbTask() throws Exception {
        Mockito.when(dbClusterSourceProvider.getLocalDcName()).thenReturn("shaoy");
        dc = drc.findDc("shaoy");
        Mockito.when(dbClusterSourceProvider.getLocalDc()).thenReturn(dc);

        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setMhaName("drcOy");
        mhaTbl.setId(10000L);
        Mockito.when(metaInfoService.getMhas(Mockito.anyString())).thenReturn(Lists.newArrayList(mhaTbl));
        String sql = "insert into `drcmonitordb`.`delaymonitor`(id, src_ip, dest_ip) values(10001, 'shaoy', 'testMha');";
        initData(sql);
        initDbTask.initDelayMonitorTbl();
    }

    @Test
    public void testGetMhaNameAndIdMap() throws SQLException {
        Mockito.when(consoleConfig.getLocalConfigCloudDc()).thenReturn(new HashSet<>(){{add("shaoy");}});
        Map<String, Long> map = initDbTask.getMhaNameAndIdMap("shaoy");
        Assert.assertEquals(0,map.size());
    }
    @Test
    public void testInitPublicCloudDbTask() throws Exception {
        Mockito.when(dbClusterSourceProvider.getLocalDcName()).thenReturn("shali");
        dc = drc.findDc("shali");
        Mockito.when(dbClusterSourceProvider.getLocalDc()).thenReturn(dc);

        MhaResponseVo mhaResponseVo = new MhaResponseVo();
        mhaResponseVo.setStatus(0);
        mhaResponseVo.setMessage("success");
        MhaTbl mhaTbl = new MhaTbl();
        mhaTbl.setMhaName("drcAli");
        mhaTbl.setId(10002L);
        mhaResponseVo.setData(Lists.newArrayList(mhaTbl));
        Mockito.when(openService.getMhas(Mockito.anyString(), Mockito.anyMap())).thenReturn(mhaResponseVo);

        String sql = "insert into `drcmonitordb`.`delaymonitor`(id, src_ip, dest_ip) values(10003, 'shali', 'testMha');";
        initData(sql);
        initDbTask.initDelayMonitorTbl();
    }

    private void initData(String sql) throws Exception {
        WriteSqlOperatorWrapper operatorWrapper = getSqlOperator();

        GeneralSingleExecution execution = new GeneralSingleExecution(sql);
        operatorWrapper.insert(execution);
    }

    private WriteSqlOperatorWrapper getSqlOperator() throws Exception {
        Endpoint endpoint = new MySqlEndpoint("127.0.0.1", 12345, "root", null, BooleanEnum.TRUE.isValue());
        WriteSqlOperatorWrapper writeSqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
        writeSqlOperatorWrapper.initialize();
        writeSqlOperatorWrapper.start();
        return writeSqlOperatorWrapper;
    }
}