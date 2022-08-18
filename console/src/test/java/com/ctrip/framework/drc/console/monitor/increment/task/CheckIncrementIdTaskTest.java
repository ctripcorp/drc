package com.ctrip.framework.drc.console.monitor.increment.task;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.MySqlService;
import com.ctrip.framework.drc.console.service.impl.MySqlServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.monitor.entity.MhaGroupEntity;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-06-28
 */
public class CheckIncrementIdTaskTest {

    private static Logger logger = LoggerFactory.getLogger(CheckIncrementIdTaskTest.class);

    @InjectMocks
    private CheckIncrementIdTask task = new CheckIncrementIdTask();

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MySqlService mySqlService = new MySqlServiceImpl();

    private static Drc drc;

    private static Set<DbClusterSourceProvider.Mha> mhaGroup;

    private static Set<DbClusterSourceProvider.Mha> mhaGroup3;

    private DataSource dataSource;

    private static DB ntDb1;

    private static DB oyDb1;

    private static final int NT_PORT1 = 13366;

    private static final int OY_PORT1 = 13377;

    private static final String IP = "127.0.0.1";

    private static final String MYSQL_USER = "root";

    private static final String MYSQL_PASSWORD = "";

    private static final String MHA_GROUP_KEY = "drcNt.drcOy";

    private static final String MHA_GROUP3_KEY = "drcNt.drcOy.drcRb";

    private static final String CHECK_INCREMENT_SQL = "show global variables like 'auto_increment_increment';";

    private static final String AUTO_INCREMENT_INCREMENT = "Set global auto_increment_increment=2;";

    private static final String AUTO_INCREMENT_OFFSET_1 = "Set global auto_increment_offset=1;";

    private static final String AUTO_INCREMENT_OFFSET_2 = "Set global auto_increment_offset=2;";

    private static Map<MetaKey, MySqlEndpoint> masterMySQLEndpointMap = Maps.newConcurrentMap();

    private static final MySqlEndpoint endpointNt = new MySqlEndpoint("127.0.0.1", 13366, "root", "", true);
    private static final MySqlEndpoint endpointOy = new MySqlEndpoint("127.0.0.1", 13377, "root", "", true);

    @BeforeClass
    public static void setUp() throws IOException, SAXException {
        // for db
        try {
            logger.info("start and init dbs");
            ntDb1 = getDb(NT_PORT1);
            oyDb1 = getDb(OY_PORT1);
            logger.info("started and initted dbs");
        } catch (ManagedProcessException e) {
            logger.error("Create DB failed: ", e);
        }

        String drcXmlStr = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<drc>\n" +
                "    <dc id=\"ntgxh\">\n" +
                "        <clusterManager ip=\"127.0.0.1\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"127.0.0.1:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"drc-Test01.drcNt\" name=\"drc-Test01\" mhaName=\"drcNt\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.1\" port=\"13366\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.1\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.1\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.1\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "            </dbCluster>\n" +
                "            <dbCluster id=\"drc-Test02.drcNt2\" name=\"drc-Test02\" mhaName=\"drcNt2\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.1\" port=\"23366\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.1\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.1\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.1\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy2\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"shaoy\">\n" +
                "        <clusterManager ip=\"127.0.0.2\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"127.0.0.2:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"drc-Test01.drcOy\" name=\"drc-Test01\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.1\" port=\"13377\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.2\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.2\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.2\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "            </dbCluster>\n" +
                "            <dbCluster id=\"drc-Test02.drcOy2\" name=\"drc-Test02\" mhaName=\"drcOy2\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.2\" port=\"23377\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.2\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.2\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.2\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt2\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "    <dc id=\"sharb\">\n" +
                "        <clusterManager ip=\"127.0.0.3\" port=\"8080\" master=\"true\"/>\n" +
                "        <zkServer address=\"127.0.0.3:2181\"/>\n" +
                "        <dbClusters>\n" +
                "            <dbCluster id=\"drc-Test01.drcRb\" name=\"drc-Test01\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.3\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.3\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.3\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.3\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
                "            </dbCluster>\n" +
                "            <dbCluster id=\"drc-Test02.drcRb2\" name=\"drc-Test02\" mhaName=\"drcRb2\" buName=\"BBZ\" appId=\"100023928\">\n" +
                "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"root\" monitorPassword=\"\">\n" +
                "                    <db ip=\"127.0.0.3\" port=\"4406\" master=\"true\" uuid=\"\"/>\n" +
                "                    <db ip=\"127.0.0.3\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
                "                </dbs>\n" +
                "                <replicator ip=\"127.0.0.3\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
                "                <replicatorMonitor ip=\"127.0.0.3\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
                "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt2\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
                "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy2\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
                "            </dbCluster>\n" +
                "        </dbClusters>\n" +
                "    </dc>\n" +
                "</drc>";
        drc = DefaultSaxParser.parse(drcXmlStr);
        Map<String, Dc> dcs = drc.getDcs();
        Dc ntgxh = dcs.get("ntgxh");
        Dc shaoy = dcs.get("shaoy");
        Dc sharb = dcs.get("sharb");
        mhaGroup = new HashSet<>() {{
            add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test01.drcNt")));
            add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test01.drcOy")));
        }};
        mhaGroup3 = new HashSet<>() {{
            add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test01.drcNt")));
            add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test01.drcOy")));
            add(new DbClusterSourceProvider.Mha("sharb", sharb.getDbClusters().get("drc-Test01.drcRb")));
        }};
        
        masterMySQLEndpointMap.put(
                new MetaKey("ntgxh", "drc-Test01.drcNt", "drc-Test01", "drcNt"),
                endpointNt
        );
        masterMySQLEndpointMap.put(
                new MetaKey("shaoy", "drc-Test01.drcOy", "drc-Test01", "drcOy"),
                endpointOy
        );
        
    }

    @Test
    public void testCheckIncrementId() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mySqlService.getAutoIncrement(
                Mockito.anyString(),
                Mockito.eq("show global variables like 'auto_increment_increment';"),
                Mockito.anyInt(),
                Mockito.any(MySqlEndpoint.class)
        )).thenReturn(2);
        Mockito.when(mySqlService.getAutoIncrement(
                Mockito.anyString(),
                Mockito.eq("show global variables like 'auto_increment_offset';"),
                Mockito.anyInt(),
                Mockito.eq(endpointNt)
        )).thenReturn(1);
        Mockito.when(mySqlService.getAutoIncrement(
                Mockito.anyString(),
                Mockito.eq("show global variables like 'auto_increment_offset';"),
                Mockito.anyInt(),
                Mockito.eq(endpointOy)
        )).thenReturn(2);
        
        task.setMasterMySQLEndpointMap(masterMySQLEndpointMap);
        Assert.assertTrue(task.checkIncrementId(MHA_GROUP_KEY, mhaGroup));
    }

    @Test
    public void testCheckIncrementConfig() {
        Set<Integer> autoIncrementIncrementSet = Sets.newHashSet(2, 3);
        Set<Integer> autoIncrementOffsetSet = Sets.newHashSet(1, 2, 3);
        Assert.assertFalse(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));

        autoIncrementIncrementSet = Sets.newHashSet(2);
        autoIncrementOffsetSet = Sets.newHashSet(1, 2, 3);
        Assert.assertFalse(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));

        autoIncrementIncrementSet = Sets.newHashSet(3);
        autoIncrementOffsetSet = Sets.newHashSet(1, 2);
        Assert.assertFalse(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));

        autoIncrementIncrementSet = Sets.newHashSet(3);
        autoIncrementOffsetSet = Sets.newHashSet(0, 2, 3);
        Assert.assertFalse(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));
        autoIncrementIncrementSet = Sets.newHashSet(3);
        autoIncrementOffsetSet = Sets.newHashSet(1, 2, 4);
        Assert.assertFalse(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));

        autoIncrementIncrementSet = Sets.newHashSet(3);
        autoIncrementOffsetSet = Sets.newHashSet(1, 2, 3);
        Assert.assertTrue(task.checkIncrementConfig(autoIncrementIncrementSet, autoIncrementOffsetSet, mhaGroup3, MHA_GROUP3_KEY));
    }

//    @Test
//    public void testGetAutoIncrement() throws Exception {
//        Endpoint endpoint = new DefaultEndPoint("127.0.0.1", NT_PORT1, MYSQL_USER, MYSQL_PASSWORD);
//        WriteSqlOperatorWrapper sqlOperatorWrapper = new WriteSqlOperatorWrapper(endpoint);
//        sqlOperatorWrapper.initialize();
//        sqlOperatorWrapper.start();
//
//        Integer autoIncrement = task.getAutoIncrement(CHECK_INCREMENT_SQL, sqlOperatorWrapper);
//        Assert.assertEquals(2, autoIncrement.intValue());
//
//        sqlOperatorWrapper.stop();
//        sqlOperatorWrapper.dispose();
//    }

    @Test
    public void testGetMhaGroupKey() {
        Assert.assertEquals("drcNt.drcOy", task.getMhaGroupKey(mhaGroup));
        Assert.assertEquals("drcNt.drcOy.drcRb", task.getMhaGroupKey(mhaGroup3));
    }

    @Test
    public void testGetMhaGroupEntity() {
        MhaGroupEntity expected1 = new MhaGroupEntity.Builder()
                .clusterAppId(100023928L)
                .buName("BBZ")
                .clusterName("drc-Test01")
                .mhaGroupKey(MHA_GROUP_KEY)
                .build();
        MhaGroupEntity expected2 = new MhaGroupEntity.Builder()
                .clusterAppId(100023928L)
                .buName("BBZ")
                .clusterName("drc-Test01")
                .mhaGroupKey(MHA_GROUP3_KEY)
                .build();

        String mhaGroupKey = task.getMhaGroupKey(mhaGroup);
        MhaGroupEntity actual = task.getMhaGroupEntity(mhaGroupKey, mhaGroup);
        Assert.assertEquals(expected1, actual);
        actual = task.getMhaGroupEntity(mhaGroupKey, mhaGroup3);
        Assert.assertEquals(expected1, actual);
        mhaGroupKey = task.getMhaGroupKey(mhaGroup3);
        actual = task.getMhaGroupEntity(mhaGroupKey, mhaGroup3);
        Assert.assertEquals(expected2, actual);
    }

    @AfterClass
    public static void tearDown() {
        try {
            ntDb1.stop();
            oyDb1.stop();
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, NT_PORT1, MYSQL_USER, MYSQL_PASSWORD));
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, OY_PORT1, MYSQL_USER, MYSQL_PASSWORD));
            logger.info("dbs are stopped");
        } catch (Exception e) {
            logger.error("Stop dbs error: ", e);
        }
    }

    private static DB getDb(int port) throws ManagedProcessException {
        DBConfigurationBuilder builder = DBConfigurationBuilder.newBuilder();
        builder.setPort(port);
        builder.addArg("--user=root");
        DB db = DB.newEmbeddedDB(builder.build());
        db.start();
        return db;
    }

    private void execute(String sql, int port) {
        dataSource = DataSourceManager.getInstance().getDataSource(new DefaultEndPoint(IP, port, MYSQL_USER, MYSQL_PASSWORD));
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (Exception e) {
            logger.error("execute query error: ", e);
        }
    }
}
