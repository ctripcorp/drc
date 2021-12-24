package com.ctrip.framework.drc.console.monitor.table.task;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-06-18
 */
public class LocalCheckTableConsistencyTaskTest {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private CheckTableConsistencyTask task;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    private CheckTableConsistencyTask checkTableConsistencyTask = new CheckTableConsistencyTask();

    private DataSource dataSource;

    private static final int NT_PORT1 = 13366;

    private static final int OY_PORT1 = 13377;

    private static final int NT_PORT2 = 23366;

    private static final int OY_PORT2 = 23377;

    private static final String IP = "127.0.0.1";

    private static final String MYSQL_USER = "root";

    private static final String MYSQL_PASSWORD = "";

    private static DB ntDb1;

    private static DB oyDb1;

    private static DB ntDb2;

    private static DB oyDb2;

    private List<Set<DbClusterSourceProvider.Mha>> mhaGroups;

    private static final String CREATE_DB = "create database drcmonitordb;";

    private static final String USE_DB = "use drcmonitordb;";

    private static final String CREATE_TABLE1 = "CREATE TABLE `delaymonitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;";


    private Drc drc;

    @Before
    public void setUp() throws IOException, SAXException {
        // for db
        try {
            logger.info("start and init dbs");
            ntDb1 = getDb(NT_PORT1);
            initDb(NT_PORT1);
            oyDb1 = getDb(OY_PORT1);
            initDb(OY_PORT1);
            ntDb2 = getDb(NT_PORT2);
            initDb(NT_PORT2);
            oyDb2 = getDb(OY_PORT2);
            initDb(OY_PORT2);
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
                "                    <db ip=\"127.0.0.2\" port=\"13377\" master=\"true\" uuid=\"\"/>\n" +
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
                "</drc>";
        drc = DefaultSaxParser.parse(drcXmlStr);
        Map<String, Dc> dcs = drc.getDcs();
        Dc ntgxh = dcs.get("ntgxh");
        Dc shaoy = dcs.get("shaoy");
        mhaGroups = new ArrayList<>() {{
            add(new HashSet<>() {{
                add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test01.drcNt")));
                add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test01.drcOy")));
            }});
            add(new HashSet<>() {{
                add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test02.drcNt2")));
                add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test02.drcOy2")));
            }});
        }};
        List<List<DbClusterSourceProvider.Mha>> mhaCombinationList1 = new ArrayList<>() {{
            add(new ArrayList<>() {{
                add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test01.drcNt")));
                add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test01.drcOy")));
            }});
        }};
        List<List<DbClusterSourceProvider.Mha>> mhaCombinationList2 = new ArrayList<>() {{
            add(new ArrayList<>() {{
                add(new DbClusterSourceProvider.Mha("ntgxh", ntgxh.getDbClusters().get("drc-Test02.drcNt2")));
                add(new DbClusterSourceProvider.Mha("shaoy", shaoy.getDbClusters().get("drc-Test02.drcOy2")));
            }});
        }};

        DefaultEndPoint ntMaster1 = new DefaultEndPoint(IP, NT_PORT1, MYSQL_USER, MYSQL_PASSWORD);
        DefaultEndPoint ntMaster2 = new DefaultEndPoint(IP, NT_PORT2, MYSQL_USER, MYSQL_PASSWORD);
        DefaultEndPoint oyMaster1 = new DefaultEndPoint(IP, OY_PORT1, MYSQL_USER, MYSQL_PASSWORD);
        DefaultEndPoint oyMaster2 = new DefaultEndPoint(IP, OY_PORT2, MYSQL_USER, MYSQL_PASSWORD);

        MockitoAnnotations.initMocks(this);
        task.setInitialDelay(1000);
        task.setPeriod(100);
        task.setTimeUnit(TimeUnit.MILLISECONDS);
        when(monitorTableSourceProvider.getTableConsistencySwitch()).thenReturn("on");
        when(dbClusterSourceProvider.getMhaGroups()).thenReturn(mhaGroups);
        when(dbClusterSourceProvider.getCombinationListFromSet(mhaGroups.get(0))).thenReturn(mhaCombinationList1);
        when(dbClusterSourceProvider.getCombinationListFromSet(mhaGroups.get(1))).thenReturn(mhaCombinationList2);
        when(dbClusterSourceProvider.getMaster(mhaCombinationList1.get(0).get(0).getDbCluster())).thenReturn(ntMaster1);
        when(dbClusterSourceProvider.getMaster(mhaCombinationList1.get(0).get(1).getDbCluster())).thenReturn(oyMaster1);
        when(dbClusterSourceProvider.getMaster(mhaCombinationList2.get(0).get(0).getDbCluster())).thenReturn(ntMaster2);
        when(dbClusterSourceProvider.getMaster(mhaCombinationList2.get(0).get(1).getDbCluster())).thenReturn(oyMaster2);
    }

    @Test
    public void testReconnect() throws Exception {
//        task.periodicalCheck();
        Thread.sleep(1500);
        Map<String, Boolean> consistencyMapper = task.getConsistencyMapper();
        System.out.println("RECONNECT drcNt.drcOy 1: " + consistencyMapper.get("drcNt.drcOy"));
        Assert.assertTrue(consistencyMapper.get("drcNt.drcOy"));

        // simulate db disconnection
        logger.info("db is being stopped for simulation");
        ntDb1.stop();
        DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, NT_PORT1, MYSQL_USER, MYSQL_PASSWORD));
        logger.info("db is stopped for simulation");
        ntDb1 = getDb(NT_PORT1);
        Thread.sleep(1500);
        System.out.println("RECONNECT drcNt.drcOy 2: " + consistencyMapper.get("drcNt.drcOy"));
        Assert.assertFalse(consistencyMapper.get("drcNt.drcOy"));
        initDb(NT_PORT1);
        Thread.sleep(200);
        System.out.println("RECONNECT drcNt.drcOy 3: " + consistencyMapper.get("drcNt.drcOy"));
        Assert.assertTrue(consistencyMapper.get("drcNt.drcOy"));
    }

    private void initDb(int port) {
        dataSource = DataSourceManager.getInstance().getDataSource(new DefaultEndPoint(IP, port, MYSQL_USER, MYSQL_PASSWORD));
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_DB);
            stmt.execute(USE_DB);
            stmt.execute(CREATE_TABLE1);
        } catch (Exception e) {
            logger.error("init db error: ", e);
        }
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

    private static DB getDb(int port) throws ManagedProcessException {
        DB db = DB.newEmbeddedDB(port);
        db.start();
        return db;
    }

    @After
    public void tearDown() {
        try {
            ntDb1.stop();
            oyDb1.stop();
            ntDb2.stop();
            oyDb2.stop();
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, NT_PORT1, MYSQL_USER, MYSQL_PASSWORD));
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, OY_PORT1, MYSQL_USER, MYSQL_PASSWORD));
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, NT_PORT2, MYSQL_USER, MYSQL_PASSWORD));
            DataSourceManager.getInstance().clearDataSource(new DefaultEndPoint(IP, OY_PORT2, MYSQL_USER, MYSQL_PASSWORD));
            logger.info("dbs are stopped");
        } catch (Exception e) {
            logger.error("Stop dbs error: ", e);
        }
    }
}
