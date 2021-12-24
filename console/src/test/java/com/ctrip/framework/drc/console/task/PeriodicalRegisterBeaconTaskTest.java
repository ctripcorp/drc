package com.ctrip.framework.drc.console.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.ClusterTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.core.service.beacon.BeaconResult;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.console.service.impl.HealthServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.foundation.Env;
import com.ctrip.xpipe.tuple.Pair;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.task.PeriodicalRegisterBeaconTask.DEFAULT_HIGH_LATENCY_SYSTEM_NAME;
import static com.ctrip.framework.drc.console.task.PeriodicalRegisterBeaconTask.DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME;
import static org.mockito.ArgumentMatchers.any;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-10-19
 */
public class PeriodicalRegisterBeaconTaskTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @InjectMocks
    private PeriodicalRegisterBeaconTask task;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DalServiceImpl dalService;

    @Mock
    private HealthServiceImpl healthService;

    @Mock
    private DefaultConsoleConfig defaultConsoleConfig;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    private MetaInfoServiceImpl metaInfoServiceImpl = new MetaInfoServiceImpl();

    private ObjectMapper objectMapper = new ObjectMapper();

    private DalUtils dalUtils = DalUtils.getInstance();

    public static final String MHA1 = "fat-fx-drc1";

    public static final String MHA2 = "fat-fx-drc2";

    public static final String DAL_CLUSTER = "integration-test";

    public static final String DAL_CLUSTER_2 = "integration-test2";

    public static final String SHAOY = "shaoy";

    public static final String SHARB = "sharb";

    public static final String OYURL = "oyurl";

    public static final String RBURL = "rburl";

    private List<String> mhas;

    private Set<String> dalClusters;

    private Map<String, List<String>> realDalClusterMap;

    @Before
    public void setUp() {
        mhas = Arrays.asList(MHA1, MHA2);
        Map<String, String> mhaDalClusterInfo = new HashMap<>() {{
            put(MHA1, DAL_CLUSTER+ ","  +DAL_CLUSTER_2);
            put(MHA2, DAL_CLUSTER);
        }};
        Map<String, String> consoleDcInfos = new HashMap<>() {{
            put(SHAOY, OYURL);
            put(SHARB, RBURL);
        }};
        dalClusters = new HashSet<>() {{
            add(DAL_CLUSTER);
            add(DAL_CLUSTER_2);
        }};
        realDalClusterMap = new HashMap<>() {{
            put(DAL_CLUSTER, mhas);
            put(DAL_CLUSTER_2, mhas);
        }};

        MockitoAnnotations.openMocks(this);
        Mockito.when(dalService.getInstanceGroupsInfo(mhas, Env.FAT)).thenReturn(mhaDalClusterInfo);
        Mockito.when(defaultConsoleConfig.getConsoleDcEndpointInfos()).thenReturn(consoleDcInfos);
        Mockito.when(monitorTableSourceProvider.getBeaconRegisterSwitch()).thenReturn(SWITCH_STATUS_ON);
        Mockito.when(monitorTableSourceProvider.getBeaconRegisterMySqlSwitch()).thenReturn(SWITCH_STATUS_ON);
        Mockito.when(monitorTableSourceProvider.getBeaconRegisterDelaySwitch()).thenReturn(SWITCH_STATUS_ON);
        Mockito.when(monitorTableSourceProvider.getBeaconFilterOutMhaForMysql()).thenReturn(new String[]{MHA1});
        Mockito.when(monitorTableSourceProvider.getBeaconFilterOutMhaForDelay()).thenReturn(ArrayUtils.EMPTY_STRING_ARRAY);
        Mockito.when(monitorTableSourceProvider.getBeaconFilterOutCluster()).thenReturn(ArrayUtils.EMPTY_STRING_ARRAY);
        task.setEnv(Env.FAT);
    }

    @Test
    public void testUpdateBeaconRegistration() throws SQLException {
        Mockito.when(healthService.doRegister(any(), any(), any())).thenReturn(BeaconResult.getSuccessInstance(""));
        Mockito.when(healthService.deRegister(any(), Mockito.anyString())).thenReturn(new ArrayList<>());
        Mockito.when(healthService.getRegisteredClusters(Mockito.anyString())).thenReturn(Arrays.asList(DAL_CLUSTER, DAL_CLUSTER_2));
        ClusterTbl clusterTbl = dalUtils.getClusterTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted())).findFirst().orElse(null);
        List<String> allMhaNamesInCluster = metaInfoServiceImpl.getAllMhaNamesInCluster(clusterTbl.getId());
        Mockito.when(metaInfoService.getAllMhaNamesInCluster(any())).thenReturn(allMhaNamesInCluster);
        Mockito.doReturn(realDalClusterMap).when(metaInfoService).getRealDalClusterMap(clusterTbl.getClusterName(), allMhaNamesInCluster);
        Pair<Set<String>, Set<String>> shouldRegisterDalClustersPair = task.updateBeaconRegistration();
        Set<String> shouldRegisterDalClustersForMysql = shouldRegisterDalClustersPair.getKey();
        Set<String> shouldRegisterDalClustersForDelay = shouldRegisterDalClustersPair.getValue();
        Assert.assertEquals(0, shouldRegisterDalClustersForMysql.size());
        Assert.assertEquals(2, shouldRegisterDalClustersForDelay.size());
        Assert.assertTrue(shouldRegisterDalClustersForDelay.contains(DAL_CLUSTER));
        Assert.assertTrue(shouldRegisterDalClustersForDelay.contains(DAL_CLUSTER_2));
    }

    @Test
    public void testRegisterDalClusterInBeacon() throws Exception {
        Mockito.when(dalService.getDc(Mockito.anyString(), any())).thenReturn(SHAOY);
        List<String> machines = metaInfoServiceImpl.getMachines(MHA1);
        MhaGroupTbl mhaGroupForMha = metaInfoServiceImpl.getMhaGroupForMha(MHA1);
        Mockito.when(metaInfoService.getMachines(MHA1)).thenReturn(machines);
        Mockito.when(metaInfoService.getMachines(MHA2)).thenReturn(machines);
        Mockito.when(metaInfoService.getMhaGroupForMha(MHA1)).thenReturn(mhaGroupForMha);
        Mockito.when(metaInfoService.getMhaGroupForMha(MHA2)).thenReturn(mhaGroupForMha);


        List<String> registeredDalClusters = task.registerDalClusterInBeacon(realDalClusterMap, "nosuchsystem");
        Assert.assertEquals(0, registeredDalClusters.size());

        Mockito.when(healthService.doRegister(Mockito.anyString(), any(), any())).thenReturn(BeaconResult.getSuccessInstance(""));
        registeredDalClusters = task.registerDalClusterInBeacon(realDalClusterMap, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
        Assert.assertEquals(2, registeredDalClusters.size());

        registeredDalClusters = task.registerDalClusterInBeacon(new HashMap<>() {{
            put(DAL_CLUSTER, Arrays.asList("nosuchmha"));
            put(DAL_CLUSTER_2, Arrays.asList("nosuchmha"));
        }}, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
        Assert.assertEquals(0, registeredDalClusters.size());

        Mockito.when(healthService.doRegister(any(), any(), any())).thenReturn(BeaconResult.getFailInstance(""));
        registeredDalClusters = task.registerDalClusterInBeacon(realDalClusterMap, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
        Assert.assertEquals(0, registeredDalClusters.size());

        Mockito.when(healthService.doRegister(Mockito.anyString(), any(), any())).thenReturn(BeaconResult.getSuccessInstance(""));
        registeredDalClusters = task.registerDalClusterInBeacon(realDalClusterMap, DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
        Assert.assertEquals(2, registeredDalClusters.size());

        Mockito.when(dalService.getDc(Mockito.anyString(), any())).thenReturn("nosuchdc");
        registeredDalClusters = task.registerDalClusterInBeacon(realDalClusterMap, DEFAULT_HIGH_LATENCY_SYSTEM_NAME);
        Assert.assertEquals(0, registeredDalClusters.size());
    }

    @Test
    public void testGetDeregisterDalCluster() {
        Set<String> shouldRegisterDalClusters = new HashSet<>() {{
            add("dalcluster1");
            add("dalcluster2");
        }};
        Mockito.when(healthService.getRegisteredClusters(DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME)).thenReturn(Arrays.asList("dalcluster1", "dalcluster2", "dalcluster3"));
        List<String> deregisterDalClusters = task.getDeregisterDalCluster(shouldRegisterDalClusters, DEFAULT_MYSQL_ALL_DOWN_SYSTEM_NAME);
        Assert.assertEquals(1, deregisterDalClusters.size());
        Assert.assertTrue(deregisterDalClusters.contains("dalcluster3"));
    }

    @Test
    public void testIsFilterOut() {
        Assert.assertTrue(task.isFilteredOut(mhas, new String[]{"fat-fx-drc2"}));
        Assert.assertFalse(task.isFilteredOut(mhas, new String[]{"nosuchmha"}));
    }
}