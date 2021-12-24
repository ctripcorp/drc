package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.monitor.comparator.MySQLEndpointComparator;
import com.ctrip.framework.drc.console.pojo.MetaKey;
import com.ctrip.framework.drc.console.pojo.MonitorMetaInfo;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceTwoImpl;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.MasterMySQLEndpointObserver;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.core.server.observer.endpoint.SlaveMySQLEndpointObserver;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.unidal.tuple.Triple;

import java.sql.SQLException;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @Author: hbshen
 * @Date: 2021/4/25
 */
public class DefaultCurrentMetaManagerTest {

    @InjectMocks
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;


    private Map<MetaKey, MySqlEndpoint> masterMySQLEndpoint;
    private Map<MetaKey, MySqlEndpoint> slaveMySQLEndpoint;
    private MetaKey metaKey;

    MasterMySQLEndpointObserver masterMySQLEndpointObserver;
    SlaveMySQLEndpointObserver slaveMySQLEndpointObserver;

    // notified once while adding observer
    public static final int MASTER_MYSQL_NOTIFY_OFFSET = 1;
    public static final int SLAVE_MYSQL_NOTIFY_OFFSET = 1;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        masterMySQLEndpoint = Maps.newConcurrentMap();
        slaveMySQLEndpoint = Maps.newConcurrentMap();
        metaKey = new MetaKey.Builder()
                .dc(DC1)
                .clusterId(CLUSTER_ID1)
                .clusterName(CLUSTER1)
                .mhaName(MHA1DC1)
                .build();
        masterMySQLEndpoint.put(metaKey, MYSQL_ENDPOINT1_MHA1DC1);
        slaveMySQLEndpoint.put(metaKey, MYSQL_ENDPOINT2_MHA1DC1);

        currentMetaManager.masterMySQLEndpoint = masterMySQLEndpoint;
        currentMetaManager.slaveMySQLEndpoint = slaveMySQLEndpoint;

        MonitorMetaInfo monitorMetaInfo = new MonitorMetaInfo();
        monitorMetaInfo.setMasterMySQLEndpoint(masterMySQLEndpoint);
        monitorMetaInfo.setSlaveMySQLEndpoint(slaveMySQLEndpoint);
        Mockito.doReturn(monitorMetaInfo).when(metaInfoServiceTwo).getMonitorMetaInfo();

        masterMySQLEndpointObserver = mock(MasterMySQLEndpointObserver.class);
        currentMetaManager.addObserver(masterMySQLEndpointObserver);
        slaveMySQLEndpointObserver = mock(SlaveMySQLEndpointObserver.class);
        currentMetaManager.addObserver(slaveMySQLEndpointObserver);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testInit() throws SQLException {
        currentMetaManager.init();
        verify(metaInfoServiceTwo, times(0)).getMonitorMetaInfo();

        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET)).update(Mockito.eq(new Triple<>(metaKey, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.ADD)), Mockito.any(MasterMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET)).update(Mockito.eq(new Triple<>(metaKey, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.ADD)), Mockito.any(SlaveMySQLEndpointObservable.class));
    }



    @Test
    public void testUpdateMasterMySQLForNonExist() {
        currentMetaManager.updateMasterMySQL(CLUSTER_ID2, MYSQL_ENDPOINT1_MHA2DC1);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.masterMySQLEndpoint.size());

        verify(slaveMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.slaveMySQLEndpoint.size());
    }

    @Test
    public void testUpdateMasterMySQLForSlaveToMaster() {
        MySqlEndpoint slaveToMaster = new MySqlEndpoint("10.0.2.2", 3306, "testMonitorUser", "testMonitorPassword", true);
        currentMetaManager.updateMasterMySQL(CLUSTER_ID1, slaveToMaster);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 1)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        verify(masterMySQLEndpointObserver, times(1)).update(Mockito.eq(new Triple<>(metaKey, slaveToMaster, ActionEnum.UPDATE)), Mockito.any(MasterMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.masterMySQLEndpoint.size());
        Assert.assertTrue(currentMetaManager.masterMySQLEndpoint.containsKey(metaKey));
        Assert.assertEquals(slaveToMaster, currentMetaManager.masterMySQLEndpoint.get(metaKey));

        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET  + 1)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(1)).update(Mockito.eq(new Triple<>(metaKey, MYSQL_ENDPOINT2_MHA1DC1, ActionEnum.DELETE)), Mockito.any(SlaveMySQLEndpointObservable.class));
        Assert.assertEquals(0, currentMetaManager.slaveMySQLEndpoint.size());
    }

    @Test
    public void testUpdateMasterMySQLForSameMaster() {
        currentMetaManager.updateMasterMySQL(CLUSTER_ID1, MYSQL_ENDPOINT1_MHA1DC1);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.masterMySQLEndpoint.size());

        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET  + 0)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.slaveMySQLEndpoint.size());
    }

    @Test
    public void testUpdateMasterMySQLForNewMaster() {
        MySqlEndpoint newMaster = new MySqlEndpoint("10.0.2.1", 3307, "testMonitorUser", "testMonitorPassword", true);
        currentMetaManager.updateMasterMySQL(CLUSTER_ID1, newMaster);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 1)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        verify(masterMySQLEndpointObserver, times(1)).update(Mockito.eq(new Triple<>(metaKey, newMaster, ActionEnum.UPDATE)), Mockito.any(MasterMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.masterMySQLEndpoint.size());
        Assert.assertTrue(currentMetaManager.masterMySQLEndpoint.containsKey(metaKey));
        Assert.assertEquals(newMaster, currentMetaManager.masterMySQLEndpoint.get(metaKey));

        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET  + 0)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.slaveMySQLEndpoint.size());
    }

    @Test
    public void testAddSlaveMySQLForNonExist() {
        currentMetaManager.addSlaveMySQL(MHA2DC1, MYSQL_ENDPOINT1_MHA2DC1);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(), Mockito.any(MasterMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));

        currentMetaManager.addSlaveMySQL(MHA1DC1, MYSQL_ENDPOINT1_MHA2DC1);
        verify(masterMySQLEndpointObserver, times(MASTER_MYSQL_NOTIFY_OFFSET + 0)).update(Mockito.any(),  Mockito.any(MasterMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET + 1)).update(Mockito.any(), Mockito.any(SlaveMySQLEndpointObservable.class));
        verify(slaveMySQLEndpointObserver, times(SLAVE_MYSQL_NOTIFY_OFFSET)).update(Mockito.eq(new Triple<>(metaKey, MYSQL_ENDPOINT1_MHA2DC1, ActionEnum.ADD)), Mockito.any(SlaveMySQLEndpointObservable.class));
        Assert.assertEquals(1, currentMetaManager.slaveMySQLEndpoint.size());
        Assert.assertEquals(MYSQL_ENDPOINT1_MHA2DC1, currentMetaManager.slaveMySQLEndpoint.get(metaKey));
    }

    @Test
    public void testCheckMySQLChange() {
        Map<MetaKey, MySqlEndpoint> current = Maps.newHashMap();
        Map<MetaKey, MySqlEndpoint> future = Maps.newHashMap();
        MetaKey metaKey1 = new MetaKey.Builder().dc("dc").clusterId("clusterId1").clusterName("clusterName1").mhaName("mhaName1").build();
        MySqlEndpoint mySqlEndpoint1 = new MySqlEndpoint("127.0.0.1", 3301,"root", "root", true);
        current.put(metaKey1, mySqlEndpoint1);

        MetaKey metaKey2 = new MetaKey.Builder().dc("dc").clusterId("clusterId2").clusterName("clusterName2").mhaName("mhaName2").build();
        MySqlEndpoint mySqlEndpoint2 = new MySqlEndpoint("127.0.0.1", 3302,"root", "root", true);
        current.put(metaKey2, mySqlEndpoint2);

        MetaKey metaKey3 = new MetaKey.Builder().dc("dc").clusterId("clusterId2").clusterName("clusterName2").mhaName("mhaName2").build();
        MySqlEndpoint mySqlEndpoint3 = new MySqlEndpoint("127.0.0.3", 3303,"root", "root", true);
        future.put(metaKey3, mySqlEndpoint3);

        MetaKey metaKey4 = new MetaKey.Builder().dc("dc").clusterId("clusterId4").clusterName("clusterName4").mhaName("mhaName4").build();
        MySqlEndpoint mySqlEndpoint4 = new MySqlEndpoint("127.0.0.4", 3304,"root", "root", true);
        future.put(metaKey4, mySqlEndpoint4);

        DefaultCurrentMetaManager currentMetaManager = Mockito.spy(new DefaultCurrentMetaManager());
        Mockito.doNothing().when(currentMetaManager).notifyMasterMySQLEndpoint(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doNothing().when(currentMetaManager).notifySlaveMySQLEndpoint(Mockito.any(), Mockito.any(), Mockito.any());

        currentMetaManager.checkMasterMySQLChange(current, future);
        currentMetaManager.checkSlaveMySQLChange(current, future);

        MySQLEndpointComparator comparator = currentMetaManager.compareMySQLMeta(current, future);
        Assert.assertEquals(1, comparator.getAdded().size());
        Assert.assertEquals(1, comparator.getRemoved().size());
        Assert.assertEquals(1, comparator.getMofified().size());
    }
}
