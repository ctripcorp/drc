package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;

/**
 * @Author: hbshen
 * @Date: 2021/4/25
 */
public class UTConstants {
    // xml files
    public static final String XML_FILE_META = "meta.xml";
    public static final String XML_FILE_META_one2many = "meta_one2many.xml";
    public static final String XML_FILE_META1 = "meta1.xml";
    public static final String XML_FILE_META2 = "meta2.xml";
    public static final String XML_FILE_META2OLD = "meta2old.xml";
    public static final String XML_FILE_META2_1 = "meta2_1.xml";
    public static final String XML_FILE_META2_2 = "meta2_2.xml";
    public static final String XML_COMPLICATED = "meta_complicated.xml";
    public static final String XML_OLD_ROUTE_DRC = "old_route_drc.xml";
    public static final String XML_NEW_ROUTE_DRC = "new_route_drc.xml";

    // dc names
    public static final String DC1 = "dc1";
    public static final String DC2 = "dc2";
    public static final String SHAOY = "shaoy";

    // dal cluster names
    public static final String CLUSTER1 = "dbcluster1";
    public static final String CLUSTER2 = "dbcluster2";

    // registryKeys/clusterIds, aka. dalClusterName.mhaName
    public static final String CLUSTER_ID1 = "dbcluster1.mha1dc1";
    public static final String CLUSTER_ID2 = "dbcluster1.mha2dc1";
    public static final String CLUSTER_ID3 = "dbcluster1.mha1dc2";

    // mha names
    public static final String MHA1DC1 = "mha1dc1";
    public static final String MHA2DC1 = "mha2dc1";
    public static final String MHA1DC2 = "mha1dc2";

    // mysql instances
    public static final int MYSQL_PORT = 3306;
    public static final String IP1 = "10.0.2.1";
    public static final String IP2 = "10.0.2.2";
    public static final MySqlEndpoint MYSQL_ENDPOINT1_MHA1DC1 = new MySqlEndpoint("10.0.2.1", 3306, "testMonitorUser", "testMonitorPassword", true);
    public static final MySqlEndpoint MYSQL_ENDPOINT2_MHA1DC1 = new MySqlEndpoint("10.0.2.2", 3306, "testMonitorUser", "testMonitorPassword", false);
    public static final MySqlEndpoint MYSQL_ENDPOINT1_MHA2DC1 = new MySqlEndpoint("10.0.2.3", 3306, "testMonitorUser", "testMonitorPassword", true);
    public static final MySqlEndpoint MYSQL_ENDPOINT2_MHA2DC1 = new MySqlEndpoint("10.0.2.4", 3306, "testMonitorUser", "testMonitorPassword", false);
    // docker mysql or MariaDb
    public static final String CI_MYSQL_IP = "127.0.0.1";
    public static final String CI_MYSQL_USER = "root";
    public static final String CI_MYSQL_PASSWORD = "123456";
    public static final int CI_PORT1 = 3306;
    public static final int CI_PORT2 = 3307;
    public static final int META_DB_PORT = 12345;
    public static final int NO_SUCH_PORT = 4404;
    public static final int SRC_PORT = 13306;
    public static final int DST_PORT = 13307;
    public static final String MYSQL_IP = "127.0.0.1";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "";

    //
    public static final MetaKey META_KEY1 = new MetaKey(DC1, CLUSTER_ID1, CLUSTER1, MHA1DC1);
    public static final MetaKey META_KEY2 = new MetaKey(DC1, CLUSTER_ID2, CLUSTER1, MHA2DC1);
    public static final MetaKey META_KEY3 = new MetaKey(DC2, CLUSTER_ID3, CLUSTER1, MHA1DC2);


    public static final String TABLE = "drcmonitordb.delaymonitor";
    public static final String SCHEMA_NAME = "drcmonitordb";
    public static final String TABLE_NAME = "delaymonitor";
    public static final String KEY = "id";
    public static final String ON_UPDATE = "datachange_lasttime";
    public static final String TABLE0 = "`db0`.`tbl0`";
    public static final String SCHEMA_NAME0 = "db0";
    public static final String TABLE_NAME0 = "tbl0";
    public static final String TABLE1 = "`db1`.`tbl1`";
    public static final String SCHEMA_NAME1 = "db1";
    public static final String TABLE_NAME1 = "tbl1";
    public static final String TABLE2 = "`db2`.`tbl2`";
    public static final String SCHEMA_NAME2 = "db2";
    public static final String TABLE_NAME2 = "tbl2";
    public static final String TABLE3 = "`db3`.`tbl3`";
    public static final String SCHEMA_NAME3 = "db3";
    public static final String TABLE_NAME3 = "tbl3";

    public static final String GET_ALL_TABLES = "SELECT DISTINCT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'mysql', 'sys', 'performance_schema', 'configdb');";

    /**
     * Deprecated
     */
    public static final String DRC_XML_STR = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "<drc>\n" +
            "    <dc id=\"ntgxh\">\n" +
            "        <route id=\"1\" org-id=\"1\" tag=\"console\" src-dc=\"ntgxh\" dst-dc=\"shaoy\" routeInfo=\"PROXYTCP://127.0.0.28:80,PROXYTCP://127.0.0.82:80,PROXYTCP://127.0.0.135:80,PROXYTCP://127.0.0.188:80 PROXYTLS://127.0.0.8:443,PROXYTLS://127.0.0.11:443\"/>" +
            "        <clusterManager ip=\"127.0.0.1\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"127.0.0.1:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"drc-Test01.drcNt\" name=\"drc-Test01\" mhaName=\"drcNt\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.1\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.1\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.1\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.1\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
            "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"drc-Test02.drcNt2\" name=\"drc-Test02\" mhaName=\"drcNt2\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.1\" port=\"4406\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.1\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.1\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.1\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy2\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
            "                <applier ip=\"127.0.0.1\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb2\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "    <dc id=\"shaoy\">\n" +
            "        <clusterManager ip=\"127.0.0.2\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"127.0.0.2:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"drc-Test01.drcOy\" name=\"drc-Test01\" mhaName=\"drcOy\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.2\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.2\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.2\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.2\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"drc-Test02.drcOy2\" name=\"drc-Test02\" mhaName=\"drcOy2\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.2\" port=\"4406\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.2\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.2\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.2\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt2\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb2\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "    <dc id=\"sharb\">\n" +
            "        <clusterManager ip=\"127.0.0.3\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"127.0.0.3:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"drc-Test01.drcRb\" name=\"drc-Test01\" mhaName=\"drcRb\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.3\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.3\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.3\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.3\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"ntgxh\" targetMhaName=\"drcNt\" gtidExecuted=\"02878c56-9375-11ea-b1c4-fa163eaa9d69:1-23\"/>\n" +
            "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"drc-Test02.drcRb2\" name=\"drc-Test02\" mhaName=\"drcRb2\" buName=\"BBZ\" org-id=\"1\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
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
    public static final String DRC_XML_STR2 = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
            "<drc>\n" +
            "    <dc id=\"shaoy\">\n" +
            "        <clusterManager ip=\"127.0.0.2\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"127.0.0.2:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"drc-Test01.drcOy\" name=\"drc-Test01\" mhaName=\"drcOy\" buName=\"BBZ\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.2\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.2\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.2\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.2\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"drc-Test02.drcOy2\" name=\"drc-Test02\" mhaName=\"drcOy2\" buName=\"BBZ\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.2\" port=\"4406\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.2\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.2\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.2\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.2\" port=\"8080\" targetIdc=\"sharb\" targetMhaName=\"drcRb2\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "    <dc id=\"sharb\">\n" +
            "        <clusterManager ip=\"127.0.0.3\" port=\"8080\" master=\"true\"/>\n" +
            "        <zkServer address=\"127.0.0.3:2181\"/>\n" +
            "        <dbClusters>\n" +
            "            <dbCluster id=\"drc-Test01.drcRb\" name=\"drc-Test01\" mhaName=\"drcRb\" buName=\"BBZ\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.3\" port=\"3306\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.3\" port=\"13306\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.3\" port=\"8080\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.3\" port=\"18080\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "            <dbCluster id=\"drc-Test02.drcRb2\" name=\"drc-Test02\" mhaName=\"drcRb2\" buName=\"BBZ\" appId=\"100023928\">\n" +
            "                <dbs readUser=\"rroot\" readPassword=\"rroot\" writeUser=\"wroot\" writePassword=\"wroot\" monitorUser=\"mroot\" monitorPassword=\"mpassword\">\n" +
            "                    <db ip=\"127.0.0.3\" port=\"4406\" master=\"true\" uuid=\"\"/>\n" +
            "                    <db ip=\"127.0.0.3\" port=\"14406\" master=\"false\" uuid=\"\"/>\n" +
            "                </dbs>\n" +
            "                <replicator ip=\"127.0.0.3\" port=\"9090\" applierPort=\"8383\" gtidSkip=\"12344dfasfas\"/>\n" +
            "                <replicatorMonitor ip=\"127.0.0.3\" port=\"19090\" applierPort=\"18383\" gtidSkip=\"123\"/>\n" +
            "                <applier ip=\"127.0.0.3\" port=\"8080\" targetIdc=\"shaoy\" targetMhaName=\"drcOy2\" gtidExecuted=\"4ab5df09-3b79-11ea-aac2-fa163eaa9d69:1-24\"/>\n" +
            "            </dbCluster>\n" +
            "        </dbClusters>\n" +
            "    </dc>\n" +
            "</drc>";

}
