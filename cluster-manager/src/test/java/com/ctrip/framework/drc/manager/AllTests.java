package com.ctrip.framework.drc.manager;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.manager.config.DataCenterServiceTest;
import com.ctrip.framework.drc.manager.config.DbClusterSourceProviderTest;
import com.ctrip.framework.drc.manager.ha.DefaultStateChangeHandlerTest;
import com.ctrip.framework.drc.manager.ha.cluster.impl.*;
import com.ctrip.framework.drc.manager.ha.cluster.task.*;
import com.ctrip.framework.drc.manager.ha.config.DefaultClusterManagerConfigTest;
import com.ctrip.framework.drc.manager.ha.localdc.LocalDcNotifierTest;
import com.ctrip.framework.drc.manager.ha.meta.comparator.*;
import com.ctrip.framework.drc.manager.ha.meta.impl.*;
import com.ctrip.framework.drc.manager.ha.meta.server.impl.DefaultClusterManagerMultiDcServiceManagerTest;
import com.ctrip.framework.drc.manager.ha.meta.server.impl.DefaultClusterManagerMultiDcServiceTest;
import com.ctrip.framework.drc.manager.ha.multidc.*;
import com.ctrip.framework.drc.manager.ha.rest.MultiMetaServerTest;
import com.ctrip.framework.drc.manager.healthcheck.DefaultMySQLMasterManagerTest;
import com.ctrip.framework.drc.manager.healthcheck.datasource.DataSourceManagerTest;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ApplierNotifierTest;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ConsoleNotifierTest;
import com.ctrip.framework.drc.manager.healthcheck.notifier.MessengerNotifierTest;
import com.ctrip.framework.drc.manager.healthcheck.notifier.ReplicatorNotifierTest;
import com.ctrip.framework.drc.manager.healthcheck.tracker.HeartBeatTrackerImplTest;
import com.ctrip.framework.drc.manager.service.ConsoleServiceImplTest;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.Maps;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.springframework.util.ClassUtils;

import java.io.*;
import java.net.ServerSocket;
import java.util.LinkedHashMap;
import java.util.UUID;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DEFAULT_CONFIG_FILE_NAME;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * Created by mingdongli
 * 2019/11/3 下午10:42.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        DefaultClusterManagerConfigTest.class,
        DefaultMultiDcServiceTest.class,
        ApplierComparatorTest.class,
        DefaultInstanceStateControllerTest.class,
        ApplierNotifierTest.class,
        ReplicatorNotifierTest.class,
        MessengerNotifierTest.class,
        DbClusterSourceProviderTest.class,
        DefaultMySQLMasterManagerTest.class,
        DefaultInstanceActiveElectAlgorithmTest.class,
        ConsoleServiceImplTest.class,
        HeartBeatTrackerImplTest.class,
        DataSourceManagerTest.class,


        ConsoleNotifierTest.class,
        DataCenterServiceTest.class,
        MultiMetaServerTest.class,

        // for ha
        ClusterManagerLeaderElectorTest.class,
        DefaultSlotManagerTest.class,
        DefaultStateChangeHandlerTest.class,
        MultiDcNotifierTest.class,
        LocalDcNotifierTest.class,
        DefaultDcApplierMasterChooserAlgorithmTest.class,
        CompositeApplierMasterChooserAlgorithmTest.class,
        DefaultConfigApplierMasterChooserAlgorithmTest.class,
        DefaultClusterManagerMultiDcServiceTest.class,
        DefaultClusterManagerMultiDcServiceManagerTest.class,
        DefaultDcApplierMasterChooserTest.class,
        DefaultApplierMasterChooserManagerTest.class,
        ServerDeadReshardingTest.class,
        RollbackMovingTaskTest.class,
        ServerBalanceReshardingTest.class,
        ContinueReshardingTest.class,
        MoveSlotFromDeadOrEmptyTest.class,
        InitReshardingTest.class,
        DefaultClusterArrangerTest.class,
        ArrangeTaskTriggerTest.class,
        ArrangeTaskExecutorTest.class,
        DefaultClusterManagersTest.class,
        DefaultRemoteClusterManagerFactoryTest.class,
        RemoteClusterManagerTest.class,
        DefaultClusterManagerTest.class,
        CurrentMetaTest.class,
        DefaultCurrentMetaManagerTest.class,
        DcComparatorTest.class,
        DcComparatorTest2.class,
        DefaultDcCacheTest.class,
        DefaultRegionCacheTest.class,
        DefaultDrcManagerTest.class,
        MessengerInstanceManagerTest.class,
        ReplicatorInstanceElectorManagerTest.class,
        MessengerInstanceElectorManagerTest.class,
        ClusterComparatorTest.class,
        ReplicatorComparatorTest.class,
        MessengerComparatorTest.class,
        ApplierInstanceManagerTest.class
})
public class AllTests {

    public static final String DC = "shaoy";

    public static final String REGION = "sha";

    public static final String TARGET_DC = "sharb";

    public static final String TARGET_REGION = "sha";

    public static final String XML_FILE = "test.xml";

    public static String DRC_XML;

    public static final String DAL_CLUSTER_NAME = "integration-test";

    public static final String DAL_CLUSTER_ID = "integration-test.fxdrc";

    public static final String BACKUP_DAL_CLUSTER_ID = "integration-test.fxdrcrb";

    public static final String OY_MHA_NAME = "fxdrc";

    public static final String RB_MHA_NAME = "fxdrcrb";

    public static final int SRC_PORT = 13306;

    public static final int DST_PORT = 13307;

    public static final String MYSQL_IP = "127.0.0.1";

    public static final String MYSQL_USER = "root";

    public static final String MYSQL_PASSWORD = "";

    private static DB srcDb;

    private static DB dstDb;

    private static TestingServer server;

    public static WireMockServer wireMockServer;

    public static final String HTTP_IP = "127.0.0.1";

    public static int HTTP_PORT = 8080;

    public static UUID ID_POST = UUID.randomUUID();

    public static UUID ID_PUT = UUID.randomUUID();

    public static final String CI_MYSQL_IP = "127.0.0.1";
    public static final String CI_MYSQL_USER = "root";
    public static final String CI_MYSQL_PASSWORD = "123456";
    public static final int CI_PORT1 = 3306;

    public static Endpoint ciEndpoint = new DefaultEndPoint(CI_MYSQL_IP, CI_PORT1, CI_MYSQL_USER, CI_MYSQL_PASSWORD);

    @BeforeClass
    public static void setUp() {
        try {
            File envFile = new File("src/test/resources/server.properties");
            InputStream inputStream = new FileInputStream(envFile);
            Foundation.server().initialize(inputStream);

            String file = ClassUtils.getDefaultClassLoader().getResource(XML_FILE).getPath();
            DRC_XML = readFileContent(file);
            System.out.println(DRC_XML);

            //for zookeeper
            server = new TestingServer(12181, true);

            //for http server
            wireMockServer = new WireMockServer();
            wireMockServer.start();
            WireMock.configureFor(HTTP_IP, wireMockServer.port());  //important to add https://github.com/tomakehurst/wiremock/issues/369

            wireMockServer.stubFor(post(urlPathMatching("/replicators")).withId(ID_POST).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));
            wireMockServer.stubFor(put(urlPathMatching("/replicators")).withId(ID_PUT).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));
            wireMockServer.stubFor(post(urlPathMatching("/appliers")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
            wireMockServer.stubFor(put(urlPathMatching("/appliers")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
            wireMockServer.stubFor(post(urlPathMatching("/api/meta/clusterchange/integration-test.fxdrc/")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(put(urlPathMatching("/api/meta/clusterchange/integration-test.fxdrc/")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(delete(urlPathMatching("/api/meta/clusterchange/integration-test.fxdrc/")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/clustermanager/addslot/1")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/clustermanager/deleteslot/1")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/clustermanager/importslot/1")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/clustermanager/exportslot/1")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/clustermanager/notifyslotchange/1")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            Replicator replicator = new Replicator();
            replicator.setIp(HTTP_IP);
            replicator.setPort(HTTP_PORT);
            wireMockServer.stubFor(get(urlPathMatching("/api/meta/getactivereplicator/integration-test.fxdrc/")).willReturn(okJson(Codec.DEFAULT.encode(replicator))));
            wireMockServer.stubFor(put(urlPathMatching("/api/meta/upstreamchange/integration-test.fxdrc/integration-test.fxdrcrb/127.0.0.1/8080")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));  //http://127.0.0.1:8080/api/meta/clusterchange/integration-test.fxdrc
            wireMockServer.stubFor(post(urlPathMatching("/api/*")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(false)))));

            wireMockServer.stubFor(put(urlPathMatching("/replicators/register")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
            wireMockServer.stubFor(put(urlPathMatching("/appliers/register")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
            wireMockServer.stubFor(put(urlPathMatching("/api/drc/v1/switch/clusters/integration-test.fxdrc/dbs/master/")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));
            wireMockServer.stubFor(put(urlPathMatching("/api/drc/v1/switch/clusters/integration-test.fxdrc/replicators/master/")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(true)))));

            LinkedHashMap<String, String> qconfigRes = Maps.newLinkedHashMap();
            qconfigRes.put("dataId", DEFAULT_CONFIG_FILE_NAME);
            qconfigRes.put("editVersion", "410");
            qconfigRes.put("data", "drc.dbclusters=" + DRC_XML);
            wireMockServer.stubFor(get(urlPathMatching("/qconfig/restapi/configs")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getInstance(qconfigRes, 0, "success")))));
            wireMockServer.stubFor(post(urlPathMatching("/qconfig/restapi/properties/100025243/envs/local/subenvs/null/configs/drc.properties")).willReturn(okJson(Codec.DEFAULT.encode(ApiResult.getSuccessInstance(0L)))));


            //for db
            srcDb = getDb(SRC_PORT);
            dstDb = getDb(DST_PORT);
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }

    @AfterClass
    public static void tearDown()
    {
        try {
            server.close();

            wireMockServer.stop();

            srcDb.stop();
            dstDb.stop();

        } catch (Exception e) {
        }
    }

    private static DB getDb(int port) throws ManagedProcessException {
        DB db = DB.newEmbeddedDB(port);
        db.start();
        db.source("db/init.sql");
        return db;
    }

    public static String readFileContent(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuffer sbf = new StringBuffer();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();
            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }

    public static boolean isUsed(int port) {
        try (ServerSocket ignored = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
