package com.ctrip.framework.drc.console.monitor.delay.task;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.google.common.collect.Sets;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;
import org.xml.sax.SAXException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-05-08
 */
public class PeriodicalUpdateDbTaskTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private PeriodicalUpdateDbTask task;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

//    @Mock
//    private MetaInfoServiceImpl metaInfoService;
//
//    @Mock
//    private MetaServiceImpl metaService;
//
//    @Mock
//    private MonitorServiceImpl monitorService;

    private Drc drc;

//    private Set<Long> mhaIdsInSameGroup;

//    private Map<String, DelayMonitorConfig> delayMonitorConfigs;

//    private DelayMonitorConfig delayMonitorConfig1;
//    private DelayMonitorConfig delayMonitorConfig3;

    private DataSource dataSource;

    private static final int PORT = 9876;

    private static final String IP = "127.0.0.1";

    private static final String MYSQL_USER = "root";

    private static final String MYSQL_PASSWORD = "";

//    private static final String CLUSTER = "drc-Test01";

//    private static final String MHA_NT1 = "fxdrcnt";
//    private static final String MHA_ST1 = "fxdrcst";
//    private static final String MHA_NT2 = "fxdrcnt2";
//    private static final String MHA_ST2 = "fxdrcst2";

    private static DB db;

    private static final String CREATE_DB = "create database drcmonitordb;";

    private static final String USE_DB = "use drcmonitordb;";

    private static final String CREATE_TABLE = "CREATE TABLE `delaymonitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;";

    private static final String INIT_TBL = "INSERT INTO `drcmonitordb`.`delaymonitor`(`src_ip`, `dest_ip`) VALUES('dc1', 'mha1dc1');";

    private static final String SELECT_ALL = "select * from `drcmonitordb`.`delaymonitor`;";

    private static final MySqlEndpoint mysqlEndpoint = new MySqlEndpoint("127.0.0.1", 9876, "root", "", true);

    private MetaKey metaKey1;
    private MetaKey metaKey2;

    @Before
    public void setUp() throws Exception {
        // for db
        createDb();

        metaKey1 = new MetaKey.Builder()
                .dc(DC1)
                .clusterId(CLUSTER_ID1)
                .clusterName(CLUSTER1)
                .mhaName(MHA1DC1)
                .build();
        metaKey2 = new MetaKey.Builder()
                .dc(DC2)
                .clusterId(CLUSTER_ID1)
                .clusterName(CLUSTER1)
                .mhaName(MHA1DC2)
                .build();

//        String drcXmlStr = "<?xml version='1.0' encoding='utf-8' ?> <drc> <dc id=\"ntgxh\">     <clusterManager ip=\"127.0.0.1\" port=\"8080\" master=\"true\"/>     <zkServer address=\"127.0.0.1:2181\"/>     <dbClusters>         <dbCluster name=\"drc-Test01\" mhaName=\"fxdrc\">             <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\"  monitorUser=\"root\" monitorPassword=\"\">                 <db ip=\"127.0.0.1\" port=\"9876\" master=\"true\" uuid=\"\" />                 <db ip=\"127.0.0.1\" port=\"10086\" master=\"false\" uuid=\"\" />             </dbs>             <replicator ip=\"127.0.0.1\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/> <replicatorMonitor ip=\"127.0.0.1\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\" />        </dbCluster>     </dbClusters> </dc> <dc id=\"shajq\">     <clusterManager ip=\"127.0.0.1\" port=\"8080\" master=\"true\"/>     <zkServer address=\"127.0.0.1:2181\"/>     <dbClusters>         <dbCluster name=\"drc-Test01\" mhaName=\"fxdrcrb\">             <dbs readUser=\"root\" readPassword=\"root\" writeUser=\"root\" writePassword=\"root\"  monitorUser=\"root\" monitorPassword=\"1234\">  <db ip=\"127.0.0.1\" port=\"13306\" master=\"true\" uuid=\"\" />   <db ip=\"127.0.0.1\" port=\"10087\" master=\"false\" uuid=\"\" />    </dbs>  <replicator ip=\"127.0.0.2\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"dsfjkjweopuewf\"/> <replicatorMonitor ip=\"127.0.0.2\" port=\"18383\" applierPort=\"8384\" gtidSkip=\"\"/>  </dbCluster> </dbClusters> </dc> </drc>";
//        drc = DefaultSaxParser.parse(drcXmlStr);
//        List<DbCluster> dbClusters = Lists.newArrayList();
//        Map<String, Dc> dcs = drc.getDcs();
//        for(Dc dc : dcs.values()) {
//            if("ntgxh".equalsIgnoreCase(dc.getId())) {
//                dbClusters = Lists.newArrayList(dc.getDbClusters().values());
//                break;
//            }
//        }

//        Map<String, Endpoint> mapper = Maps.newConcurrentMap();
//        mapper.put(CLUSTER+'.'+MHA_NT1, new DefaultEndPoint("127.0.0.1", 9876, "root", ""));

        MockitoAnnotations.openMocks(this);
        task.setLocalDcName(DC1);
        Mockito.doReturn(DC1).when(sourceProvider).getLocalDcName();
        Mockito.doReturn(Sets.newHashSet("dc-readMhaFromConfig")).when(consoleConfig).getLocalConfigCloudDc();
        Mockito.doReturn("sha").when(consoleConfig).getRegion();
        Mockito.doReturn(Sets.newHashSet(Lists.newArrayList(DC1))).when(consoleConfig).getDcsInLocalRegion();
        Mockito.doNothing().when(currentMetaManager).addObserver(Mockito.any());
        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getDelayMonitorUpdatedbSwitch();

        MhaTbl mhaTbl1 = new MhaTbl();
        mhaTbl1.setMhaName(MHA1DC1);
        mhaTbl1.setId(4L);
        MhaTbl mhaTbl2 = new MhaTbl();
        mhaTbl2.setId(2L);
        mhaTbl2.setMhaName(MHA1DC2);
        
        Mockito.doReturn(Lists.newArrayList(mhaTbl1)).when(metaInfoService).getMhasByDc(Mockito.eq(DC1));
        Mockito.doReturn(Lists.newArrayList(mhaTbl2)).when(metaInfoService).getMhasByDc(Mockito.eq(DC2));
        task.isleader();
//        when(metaInfoService.getEstablishedDbEndpointMap("ntgxh")).thenReturn(mapper);

//        // data consistency
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl1 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl1.setId(1);
//        dataConsistencyMonitorTbl1.setMhaId(1);
//        dataConsistencyMonitorTbl1.setMonitorSchemaName("schema1");
//        dataConsistencyMonitorTbl1.setMonitorTableName("table1");
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl2 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl2.setId(2);
//        dataConsistencyMonitorTbl2.setMhaId(2);
//        dataConsistencyMonitorTbl2.setMonitorSchemaName("schema2");
//        dataConsistencyMonitorTbl2.setMonitorTableName("table2");
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl3 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl3.setId(3);
//        dataConsistencyMonitorTbl3.setMhaId(2);
//        dataConsistencyMonitorTbl3.setMonitorSchemaName("schema3");
//        dataConsistencyMonitorTbl3.setMonitorTableName("table3");
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl4 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl4.setId(4);
//        dataConsistencyMonitorTbl4.setMhaId(3);
//        dataConsistencyMonitorTbl4.setMonitorSchemaName("schema4");
//        dataConsistencyMonitorTbl4.setMonitorTableName("table4");
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl5 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl5.setId(5);
//        dataConsistencyMonitorTbl5.setMhaId(1);
//        dataConsistencyMonitorTbl5.setMonitorSchemaName("schema5");
//        dataConsistencyMonitorTbl5.setMonitorTableName("table5");
//        DataConsistencyMonitorTbl dataConsistencyMonitorTbl6 = new DataConsistencyMonitorTbl();
//        dataConsistencyMonitorTbl6.setId(6);
//        dataConsistencyMonitorTbl6.setMhaId(4);
//        dataConsistencyMonitorTbl6.setMonitorSchemaName("schema6");
//        dataConsistencyMonitorTbl6.setMonitorTableName("table6");
//
//        mhaIdsInSameGroup = Sets.newHashSet(Arrays.asList(1L, 2L));
//        delayMonitorConfig1 = new DelayMonitorConfig();
//        delayMonitorConfig1.setSchema("`schema1`");
//        delayMonitorConfig1.setTable("`table1`");
//        delayMonitorConfig1.setKey("id1");
//        delayMonitorConfig1.setOnUpdate("onupdate1");
//        delayMonitorConfig3 = new DelayMonitorConfig();
//        delayMonitorConfig3.setSchema("`schema3`");
//        delayMonitorConfig3.setTable("`table3`");
//        delayMonitorConfig3.setKey("id3");
//        delayMonitorConfig3.setOnUpdate("onupdate3");
//        delayMonitorConfigs = new HashMap<>() {{
//            put("`schema1`.`table1`", delayMonitorConfig1);
//            put("`schema3`.`table3`", delayMonitorConfig3);
//        }};
//        Mockito.when(metaService.getDataConsistencyMonitorTbls()).thenReturn(Arrays.asList(
//                dataConsistencyMonitorTbl2, dataConsistencyMonitorTbl3, dataConsistencyMonitorTbl4, dataConsistencyMonitorTbl5, dataConsistencyMonitorTbl6
//        ));
    }

//    @Test
//    public void testAddAndDeleteConsistencyMeta() throws SQLException {
//        MhaTbl mhaTbl1 = new MhaTbl();
//        mhaTbl1.setId(1L);
//        mhaTbl1.setMhaName(MHA_NT1);
//        MhaTbl mhaTbl2 = new MhaTbl();
//        mhaTbl2.setId(2L);
//        mhaTbl2.setMhaName(MHA_ST1);
//        List<MhaTbl> mhaTblsInSameGroup = Arrays.asList(mhaTbl1, mhaTbl2);
//
//        Mockito.doNothing().when(monitorService).addDataConsistencyMonitor(MHA_NT1, MHA_ST1, delayMonitorConfig1);
//        Mockito.doThrow(new SQLException()).when(monitorService).addDataConsistencyMonitor(MHA_NT1, MHA_ST1, delayMonitorConfig3);
//        Mockito.doNothing().when(monitorService).deleteDataConsistencyMonitor(anyInt());
//        Pair<Integer, Integer> addDeletePair = task.addAndDeleteConsistencyMeta(mhaTblsInSameGroup, delayMonitorConfigs);
//        Assert.assertEquals(1, addDeletePair.getKey().intValue());
//        Assert.assertEquals(2, addDeletePair.getValue().intValue());
//
//        Mockito.doThrow(new SQLException()).when(monitorService).deleteDataConsistencyMonitor(anyInt());
//        addDeletePair = task.addAndDeleteConsistencyMeta(mhaTblsInSameGroup, delayMonitorConfigs);
//        Assert.assertEquals(1, addDeletePair.getKey().intValue());
//        Assert.assertEquals(0, addDeletePair.getValue().intValue());
//    }
//
//    @Test
//    public void testGetIdsToBeDeleted() throws SQLException {
//        List<Integer> idsToBeDeleted = task.getIdsToBeDeleted(mhaIdsInSameGroup, delayMonitorConfigs);
//        Assert.assertEquals(2, idsToBeDeleted.size());
//        List<Integer> expectedIds = Arrays.asList(2, 5);
//        for(int id : idsToBeDeleted) {
//            Assert.assertTrue(expectedIds.contains(id));
//        }
//    }
//
//    @Test
//    public void testGetDelayMonitorConfigsToBeAdded() throws SQLException {
//        List<DelayMonitorConfig> delayMonitorConfigsToBeAdded = task.getDelayMonitorConfigsToBeAdded(mhaIdsInSameGroup, delayMonitorConfigs);
//        Assert.assertEquals(1, delayMonitorConfigsToBeAdded.size());
//        Assert.assertEquals(delayMonitorConfig1, delayMonitorConfigsToBeAdded.get(0));
//    }

    @Test
    public void testReconnect() throws Exception {
        Triple<MetaKey, MySqlEndpoint, ActionEnum> triple = new Triple<>(metaKey1, mysqlEndpoint, ActionEnum.ADD);
        task.update(triple, new LocalMasterMySQLEndpointObservable());
        task.setInitialDelay(0);
        task.setPeriod(100);
        task.setTimeUnit(TimeUnit.MILLISECONDS);

        task.start();
        String timestamp1 = getTimestamp();
        Thread.sleep(300);
        String timestamp2 = getTimestamp();
//        task.getScheduledFuture().cancel(true);
        System.out.println("time1: " + timestamp1 + ", time2: " + timestamp2);
        Assert.assertNotEquals(timestamp1, timestamp2);

        // simulate db disconnection
        logger.info("db is being stopped for simulation");
        db.stop();
        DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, PORT, MYSQL_USER, MYSQL_PASSWORD));
        logger.info("db is stopped for simulation");
        createDb();
        String timestamp3 = getTimestamp();
        Thread.sleep(200);
        String timestamp4 = getTimestamp();
        System.out.println("time3: " + timestamp3 + ", time4: " + timestamp4);
        Assert.assertNotEquals(timestamp3, timestamp4);
    }

    @Test
    public void testUpdate() {
        task.setLocalDcName(DC1);
        Triple<MetaKey, MySqlEndpoint, ActionEnum> triple1 = new Triple<>(metaKey2, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.ADD);
        Triple<MetaKey, MySqlEndpoint, ActionEnum> triple2 = new Triple<>(metaKey1, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.ADD);
        Triple<MetaKey, MySqlEndpoint, ActionEnum> triple3 = new Triple<>(metaKey1, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.UPDATE);
        Triple<MetaKey, MySqlEndpoint, ActionEnum> triple4 = new Triple<>(metaKey1, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.DELETE);
        task.update(triple1, new Observable() {
            @Override
            public void addObserver(Observer observer) {
            }
            @Override
            public void removeObserver(Observer observer) {
            }
        });
        Assert.assertEquals(0, task.getMasterMySQLEndpointMap().size());

        task.update(triple2, new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(1, task.getMasterMySQLEndpointMap().size());
        Assert.assertEquals(MYSQL_ENDPOINT1_MHA1DC1, task.getMasterMySQLEndpointMap().get(metaKey1));

        task.update(triple3, new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(1, task.getMasterMySQLEndpointMap().size());
        Assert.assertEquals(MYSQL_ENDPOINT2_MHA1DC1, task.getMasterMySQLEndpointMap().get(metaKey1));

        task.update(triple4, new LocalMasterMySQLEndpointObservable());
        Assert.assertEquals(0, task.getMasterMySQLEndpointMap().size());
    }

    private void createDb() throws InterruptedException {
        try {
            db = getDb(PORT);
        } catch (ManagedProcessException e) {
            logger.error("Create db error: ", e);
        }

        dataSource = DataSourceManager.getInstance().getDataSource(new DefaultEndPoint(IP, PORT, MYSQL_USER, MYSQL_PASSWORD));
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_DB);
            stmt.execute(USE_DB);
            stmt.execute(CREATE_TABLE);
            stmt.execute(INIT_TBL);
        } catch (Exception e) {
            logger.error("init db error: ", e);
        }
    }

    private String getTimestamp() {
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SELECT_ALL)) {
            if(rs.next()) {
                return rs.getString(4);
            }
        } catch (Exception e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
        }
        return "";
    }

    /**
     * brew update && brew upgrade
     * brew uninstall --ignore-dependencies openssl
     * brew install https://github.com/tebelorg/Tump/releases/download/v1.0.0/openssl.rb
     *
     *
     * ssl need 1.0.0 see https://github.com/kelaberetiv/TagUI/issues/635
     * @param port
     * @return
     * @throws ManagedProcessException
     */
    private static DB getDb(int port) throws ManagedProcessException {
        DBConfigurationBuilder builder = DBConfigurationBuilder.newBuilder();
        builder.setPort(port);
        builder.addArg("--user=root");
        DB db = DB.newEmbeddedDB(builder.build());
        db.start();
        return db;
    }

    @After
    public void tearDown() {
        try {
            db.stop();
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, PORT, MYSQL_USER, MYSQL_PASSWORD));
            logger.info("db is stopped");
        } catch (Exception e) {
            logger.error("Stop db error: ", e);
        }
    }

    public static final class LocalMasterMySQLEndpointObservable implements MasterMySQLEndpointObservable {

        @Override
        public void addObserver(Observer observer) {

        }

        @Override
        public void removeObserver(Observer observer) {

        }
    }
}
