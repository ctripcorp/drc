package com.ctrip.framework.drc.console.monitor.delay.config;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.DRC_DELAY_MESUREMENT;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.MYSQL_DELAY_MESUREMENT;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-24
 */
public class MonitorTableSourceProviderTest {

    @InjectMocks
    private MonitorTableSourceProvider monitorTableSourceProvider = new MonitorTableSourceProvider();

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    /**
     * In order to pass all the tests in this class locally,
     * make sure to copy all the k-v pairs in test/resources/META-INF/framework.qconfig/drc.properties to local file /opt/config/100023928/qconfig/drc.properties
     */
    @Test
    public void testGetMonitorConfig() {
        DelayMonitorConfig monitorConfig = monitorTableSourceProvider.getMonitorConfig("integration-test.drcOy");
        Assert.assertEquals("drc4.benchmark", monitorConfig.getTable());
        Assert.assertEquals("id", monitorConfig.getKey());
        Assert.assertEquals("datachange_lasttime", monitorConfig.getOnUpdate());
    }


    @Test
    public void testGetBeaconFilterOut() {
        String[] beaconFilterOut = monitorTableSourceProvider.getBeaconFilterOutMhaForMysql();
        Assert.assertEquals(2, beaconFilterOut.length);
        Assert.assertTrue(ArrayUtils.contains(beaconFilterOut, "mha1"));
        Assert.assertTrue(ArrayUtils.contains(beaconFilterOut, "mha2"));
        Assert.assertFalse(ArrayUtils.contains(beaconFilterOut, "mha3"));
        beaconFilterOut = monitorTableSourceProvider.getBeaconFilterOutMhaForDelay();
        Assert.assertEquals(1, beaconFilterOut.length);
        Assert.assertTrue(ArrayUtils.contains(beaconFilterOut, "mha1"));
        Assert.assertFalse(ArrayUtils.contains(beaconFilterOut, "mha2"));
        Assert.assertFalse(ArrayUtils.contains(beaconFilterOut, "mha3"));
        beaconFilterOut = monitorTableSourceProvider.getBeaconFilterOutCluster();
        Assert.assertEquals(0, beaconFilterOut.length);
    }

    @Test
    public void testGetFilterOutMhasForMultiSideMonitor() {
        String[] filterOutMhasForMultiSideMonitor = monitorTableSourceProvider.getFilterOutMhasForMultiSideMonitor();
        Assert.assertEquals(0, filterOutMhasForMultiSideMonitor.length);
    }

    @Test
    public void testProperties() {
        Assert.assertEquals("testReadUser", monitorTableSourceProvider.getReadUserVal());
        Assert.assertEquals("testReadPassword", monitorTableSourceProvider.getReadPasswordVal());
        Assert.assertEquals("testWriteUser", monitorTableSourceProvider.getWriteUserVal());
        Assert.assertEquals("testWritePassword", monitorTableSourceProvider.getWritePasswordVal());
        Assert.assertEquals("testMonitorUser", monitorTableSourceProvider.getMonitorUserVal());
        Assert.assertEquals("testMonitorPassword", monitorTableSourceProvider.getMonitorPasswordVal());
        Assert.assertEquals("on", monitorTableSourceProvider.getDelayMonitorSwitch(DRC_DELAY_MESUREMENT));
        Assert.assertEquals("on", monitorTableSourceProvider.getDelayMonitorSwitch(MYSQL_DELAY_MESUREMENT));
        Assert.assertEquals("on", monitorTableSourceProvider.getDelayMonitorSwitch("nosuchmeasurement"));
        Assert.assertEquals("on", monitorTableSourceProvider.getGtidMonitorSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getIncrementIdMonitorSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getTableConsistencySwitch());
        Assert.assertEquals("on", monitorTableSourceProvider.getUpdateClusterTblSwitch());
        Assert.assertEquals("on", monitorTableSourceProvider.getMySqlMonitorSwitch());
        Assert.assertEquals("on", monitorTableSourceProvider.getBeaconRegisterSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getBeaconRegisterMySqlSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getBeaconRegisterDelaySwitch());
        Assert.assertEquals("qconfig", monitorTableSourceProvider.getMhaDalclusterInfoSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getDataConsistentMonitorSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getTruncateConsistentMonitorSwitch());
        Assert.assertEquals("on", monitorTableSourceProvider.getListenReplicatorSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getUnitVericationManagerSwitch());
        Assert.assertEquals("off", monitorTableSourceProvider.getUnitVericationManagerSwitch());
        Assert.assertEquals("qconfig", monitorTableSourceProvider.getUcsStrategyIdMapSource());
        Assert.assertEquals("qconfig", monitorTableSourceProvider.getUidMapSource());
        Assert.assertEquals("qconfig", monitorTableSourceProvider.getUcsStrategyIdMapSource());
        Assert.assertEquals("http://test.t.com", monitorTableSourceProvider.getUnitResultHickwallAddress());
        Assert.assertEquals("off", monitorTableSourceProvider.getCacheAllClusterNamesSwitch());
        Assert.assertEquals("", monitorTableSourceProvider.getMetaData());
        Assert.assertEquals("off", monitorTableSourceProvider.getAllowFailoverSwitch());
        Assert.assertEquals(3000, monitorTableSourceProvider.getDalServiceTimeout());
        Assert.assertEquals("on", monitorTableSourceProvider.getDelayMonitorUpdatedbSwitch());
        Assert.assertEquals(60 * 8,monitorTableSourceProvider.getGtidMonitorPeriod());
    }
}
