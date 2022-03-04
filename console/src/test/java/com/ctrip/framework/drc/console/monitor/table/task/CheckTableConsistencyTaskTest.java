package com.ctrip.framework.drc.console.monitor.table.task;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
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
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl.ALLMATCH;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-02-26
 */
public class CheckTableConsistencyTaskTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private CheckTableConsistencyTask task1 = new CheckTableConsistencyTask();

    @InjectMocks
    private CheckTableConsistencyTask checkTableConsistencyTask;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Mock
    private MetaInfoServiceImpl metaInfoService;

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

    private DefaultEndPoint ntMaster1;
    private DefaultEndPoint ntMaster2;
    private DefaultEndPoint oyMaster1;
    private DefaultEndPoint oyMaster2;

    private List<Set<DbClusterSourceProvider.Mha>> mhaGroups;

    private static final String CREATE_DB = "create database drcmonitordb;";

    private static final String USE_DB = "use drcmonitordb;";

    private static final String CREATE_TABLE1 = "CREATE TABLE `delaymonitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=latin1;";

    private static final String ADD_INDEX1 = "ALTER TABLE drcmonitordb.delaymonitor ADD INDEX src_ip1 (src_ip);";

    private static final String ADD_INDEX2 = "ALTER TABLE drcmonitordb.delaymonitor ADD INDEX src_ip2 (src_ip);";

    private static final String DROP_INDEX1 = "ALTER TABLE drcmonitordb.delaymonitor DROP INDEX src_ip1;";

    private static final String DROP_INDEX2 = "ALTER TABLE drcmonitordb.delaymonitor DROP INDEX src_ip2;";

    private static final String DELETE_TABLE1 = "DROP TABLE `drcmonitordb`.`delaymonitor`;";

    private Drc drc;

    @Before
    public void setUp() throws IOException, SAXException, SQLException {
        MockitoAnnotations.openMocks(this);
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

        Mockito.doReturn(ALLMATCH).when(metaInfoService).getApplierFilter(Mockito.anyString(),Mockito.anyString());

        ntMaster1 = new DefaultEndPoint(IP, NT_PORT1, MYSQL_USER, MYSQL_PASSWORD);
        ntMaster2 = new DefaultEndPoint(IP, NT_PORT2, MYSQL_USER, MYSQL_PASSWORD);
        oyMaster1 = new DefaultEndPoint(IP, OY_PORT1, MYSQL_USER, MYSQL_PASSWORD);
        oyMaster2 = new DefaultEndPoint(IP, OY_PORT2, MYSQL_USER, MYSQL_PASSWORD);
    }

    @Test
    public void testCheckTableConsistency() throws InterruptedException {

        
        Thread.sleep(200);
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(DELETE_TABLE1, OY_PORT1);
        Thread.sleep(100);
        Assert.assertFalse(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(CREATE_TABLE1, OY_PORT1);
        Thread.sleep(100);
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(DELETE_TABLE1, OY_PORT2);
        Thread.sleep(100);
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertFalse(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(CREATE_TABLE1, OY_PORT2);
        Thread.sleep(100);
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(ADD_INDEX1, NT_PORT1);
        Thread.sleep(100);
        Assert.assertFalse(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(ADD_INDEX2, OY_PORT1);
        Thread.sleep(100);
        Assert.assertFalse(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));

        execute(DROP_INDEX1, NT_PORT1);
        execute(DROP_INDEX2, OY_PORT1);
        Thread.sleep(100);
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster1, oyMaster1, "drcNt", "drcOy", "drc-Test01"));
        Assert.assertTrue(checkTableConsistencyTask.checkTableConsistency(ntMaster2, oyMaster2, "drcNt2", "drcOy2", "drc-Test02"));
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
        DBConfigurationBuilder builder = DBConfigurationBuilder.newBuilder();
        builder.setPort(port);
        builder.addArg("--user=root");
        DB db = DB.newEmbeddedDB(builder.build());
        db.start();
        return db;
    }

    @Test
    public void testIsFilteredOut() {
        Mockito.doReturn(ArrayUtils.EMPTY_STRING_ARRAY).when(monitorTableSourceProvider).getFilterOutMhasForMultiSideMonitor();
        DbCluster dbCluster = new DbCluster();
        dbCluster.setMhaName("mha1");
        DbCluster dbCluster2 = new DbCluster();
        dbCluster2.setMhaName("mha2");
        List<DbClusterSourceProvider.Mha> mhas = new ArrayList<>() {{
            add(new DbClusterSourceProvider.Mha("dc1", dbCluster));
            add(new DbClusterSourceProvider.Mha("dc2", dbCluster2));
        }};
        Assert.assertFalse(checkTableConsistencyTask.isFilteredOut(mhas));

        Mockito.doReturn(new String[]{"mha1"}).when(monitorTableSourceProvider).getFilterOutMhasForMultiSideMonitor();
        Assert.assertTrue(checkTableConsistencyTask.isFilteredOut(mhas));

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
