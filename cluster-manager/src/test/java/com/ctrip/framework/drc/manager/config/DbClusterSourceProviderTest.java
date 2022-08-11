package com.ctrip.framework.drc.manager.config;

import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.manager.service.ConsoleServiceImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-08-26
 */
public class DbClusterSourceProviderTest {

    @InjectMocks
    private DbClusterSourceProvider sourceProvider = new DbClusterSourceProvider();

    @Mock
    private ConsoleServiceImpl consoleService;

    private String drcXmlStr;
    private String drcXmlStrFromConsole;
    private String wrongDrcXmlStr;
    private String nullDrcXmlStr = null;
    private String initDrcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "<drc>\n" +
            "</drc>";
    private static final String SHAOY = "shaoy";

    @Before
    public void setUp() {
        drcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "                    <db ip=\"10.2.83.109\" port=\"3306\" master=\"true\" uuid=\"bd9b313c-b56d-11ea-825d-fa163e02998c\"/>\n" +
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
                "</drc>";
        drcXmlStrFromConsole = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.fat-fx-drc1\" name=\"integration-test\" mhaName=\"fat-fx-drc1\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
                "                    <db ip=\"10.2.72.233\" port=\"55222\" master=\"true\" uuid=\"12345678-a097-11ea-aade-fa163efb7175\"/>\n" +
                "                    <db ip=\"10.2.72.244\" port=\"55222\" master=\"false\" uuid=\"12345678-a098-11ea-a665-fa163eb5defa\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.87.155\" port=\"8088\" applierPort=\"8388\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <replicator ip=\"10.2.83.100\" port=\"8088\" applierPort=\"8388\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <applier ip=\"10.2.83.300\" port=\"8088\" targetIdc=\"sharb\" targetMhaName=\"fat-fx-drc2\" gtidExecuted=\"12345678-a097-11ea-aade-fa163efb7175:1-194\"/>\n" +
                "            </dbCluster>\n" +
                "            <dbCluster id=\"train.ticketorder\" name=\"train\" mhaName=\"ticketorder\" buName=\"TRA\" appId=\"100023456\">\n" +
                "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
                "                    <db ip=\"10.2.72.255\" port=\"55222\" master=\"true\" uuid=\"12345678-a097-11ea-aade-fa163efb7175\"/>\n" +
                "                    <db ip=\"10.2.72.266\" port=\"55222\" master=\"false\" uuid=\"12345678-a098-11ea-a665-fa163eb5defa\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.87.155\" port=\"8088\" applierPort=\"8389\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <replicator ip=\"10.2.83.100\" port=\"8088\" applierPort=\"8389\" gtidSkip=\"12345678-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <applier ip=\"10.2.83.300\" port=\"8089\" targetIdc=\"sharb\" targetMhaName=\"ticketorderrb\" gtidExecuted=\"12345678-a097-11ea-aade-fa163efb7175:1-194\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"sharb\">\n" +
                "        <clusterManager ip=\"10.2.84.109\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.83.114:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.fat-fx-drc2\" name=\"integration-test\" mhaName=\"fat-fx-drc2\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
                "                    <db ip=\"10.2.72.246\" port=\"55111\" master=\"true\" uuid=\"12345678-a099-11ea-a955-fa163ec687c3\"/>\n" +
                "                    <db ip=\"10.2.72.248\" port=\"55111\" master=\"false\" uuid=\"e434e42b-a09a-11ea-9340-fa163ebaf157\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8384\" gtidSkip=\"69f3dff0-a098-11ea-a665-fa163eb5defa:1-194\"/>\n" +
                "                <applier ip=\"10.2.83.111\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"fat-fx-drc1\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <applier ip=\"10.2.86.136\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"fat-fx-drc1\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "            </dbCluster>\n" +
                "            <dbCluster id=\"train.ticketorderrb\" name=\"train\" mhaName=\"ticketorderrb\" buName=\"TRA\" appId=\"100023456\">\n" +
                "                <dbs readUser=\"m_drc_w_new\" readPassword=\"80+H44bA5wwqA(!R_new\" writeUser=\"m_drc_w_new\" writePassword=\"80+H44bA5wwqA(!R_new\" monitorUser=\"m_drc_w_new\" monitorPassword=\"80+H44bA5wwqA(!R_new\">\n" +
                "                    <db ip=\"10.2.72.256\" port=\"55111\" master=\"true\" uuid=\"12345678-a099-11ea-a955-fa163ec687c3\"/>\n" +
                "                    <db ip=\"10.2.72.258\" port=\"55111\" master=\"false\" uuid=\"e434e42b-a09a-11ea-9340-fa163ebaf157\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.106\" port=\"8080\" applierPort=\"8385\" gtidSkip=\"69f3dff0-a098-11ea-a665-fa163eb5defa:1-194\"/>\n" +
                "                <applier ip=\"10.2.83.111\" port=\"8088\" targetIdc=\"shaoy\" targetMhaName=\"ticketorder\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "                <applier ip=\"10.2.86.136\" port=\"8088\" targetIdc=\"shaoy\" targetMhaName=\"ticketorder\" gtidExecuted=\"63d32bf0-a099-11ea-a955-fa163ec687c3:1-614\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "</drc>";
        wrongDrcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"10.2.84.122\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"10.2.84.112:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"integration-test.drcOy\" name=\"integration-test\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100024819\">\n" +
                "                <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\" monitorUser=\"root\" monitorPassword=\"root\">\n" +
                "                    <db ip=\"10.2.83.109\" port=\"3306\" master=\"true\" uuid=\"bd9b313c-b56d-11ea-825d-fa163e02998c\"/>\n" +
                "                    <db ip=\"10.2.83.110\" port=\"3306\" master=\"false\" uuid=\"b73fd53a-b56d-11ea-ac10-fa163ec90ff6\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"10.2.83.105\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <replicator ip=\"10.2.63.130\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"9534ea47-b56d-11ea-b18f-fa163eaa9d69:1-26\"/>\n" +
                "                <applier ip=\"10.2.83.100\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "                <applier ip=\"10.2.86.137\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"bd9b313c-b56d-11ea-825d-fa163e02998c:1-26\"/>\n" +
                "            </dbCluster>\n" +
                "    </dc>\n" +
                "</drc>";
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetDc() {
        Mockito.when(consoleService.getDbClusters(SHAOY)).thenReturn(drcXmlStrFromConsole);
        Dc actual = sourceProvider.getDc(SHAOY);
        Assert.assertNotNull(actual);
        Map<String, DbCluster> dbClusters = actual.getDbClusters();
        Assert.assertEquals(2, dbClusters.size());
        Assert.assertNotNull(dbClusters.get("integration-test.fat-fx-drc1"));
        Assert.assertNotNull(dbClusters.get("train.ticketorder"));

        Mockito.when(consoleService.getDbClusters(SHAOY)).thenReturn(drcXmlStr);
        actual = sourceProvider.getDc(SHAOY);
        Assert.assertNotNull(actual);
        Assert.assertEquals("shaoy", actual.getId());
        Assert.assertEquals(1, actual.getDbClusters().size());
        Assert.assertTrue(actual.getDbClusters().containsKey("integration-test.drcOy"));
        actual = sourceProvider.getDc("ntgxh");
        Assert.assertNull(actual);

        Mockito.when(consoleService.getDbClusters(SHAOY)).thenReturn(wrongDrcXmlStr);
        actual = sourceProvider.getDc("ntgxh");
        Assert.assertNull(actual);

        Mockito.when(consoleService.getDbClusters(SHAOY)).thenReturn(nullDrcXmlStr);
        actual = sourceProvider.getDc("ntgxh");
        Assert.assertNull(actual);

        Mockito.when(consoleService.getDbClusters(SHAOY)).thenReturn(initDrcXmlStr);
        actual = sourceProvider.getDc("ntgxh");
        Assert.assertNull(actual);
    }

    @After
    public void tearDown() {

    }
}
