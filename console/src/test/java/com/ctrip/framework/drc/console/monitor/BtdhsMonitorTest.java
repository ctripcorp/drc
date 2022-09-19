package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalMasterMySQLEndpointObservable;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.unidal.tuple.Triple;

import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static com.ctrip.framework.drc.core.monitor.enums.MeasurementEnum.BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @Author: hbshen
 * @Date: 2021/4/27
 */
public class BtdhsMonitorTest extends AbstractTest {

    @InjectMocks
    private BtdhsMonitor btdhsMonitor;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private DbClusterSourceProvider sourceProvider;

    @Mock
    private Reporter reporter = DefaultReporterHolder.getInstance();

    private MySqlEndpoint mha1MasterEndpoint = new MySqlEndpoint(CI_MYSQL_IP, CI_PORT1, CI_MYSQL_USER, CI_MYSQL_PASSWORD, true);
    private MySqlEndpoint mha1SlaveEndpoint = new MySqlEndpoint(CI_MYSQL_IP, CI_PORT2, CI_MYSQL_USER, CI_MYSQL_PASSWORD, false);
    private MySqlEndpoint mha3MasterEndpoint = new MySqlEndpoint(MYSQL_IP, SRC_PORT, MYSQL_USER, MYSQL_PASSWORD, true);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Mockito.doReturn(DC1).when(sourceProvider).getLocalDcName();
        Mockito.doReturn("on").when(monitorTableSourceProvider).getBtdhsMonitorSwitch();
        Mockito.doNothing().when(currentMetaManager).addObserver(btdhsMonitor);
        Mockito.doReturn("sha").when(consoleConfig).getRegion();
        Mockito.doReturn(Sets.newHashSet(Lists.newArrayList(DC1))).when(consoleConfig).getDcsInLocalRegion();
        btdhsMonitor.initialize();
        btdhsMonitor.isleader();
        
    }

    @Test
    public void testInit() {

        verify(sourceProvider, times(1)).getLocalDcName();
        verify(currentMetaManager, times(1)).addObserver(btdhsMonitor);
    }

    @Test
    public void testMonitorBtdhs() {
        btdhsMonitor.update(new Triple<>(META_KEY1, mha1MasterEndpoint, ActionEnum.ADD), new LocalMasterMySQLEndpointObservable());
        btdhsMonitor.update(new Triple<>(META_KEY1, mha1SlaveEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        btdhsMonitor.update(new Triple<>(META_KEY3, mha3MasterEndpoint, ActionEnum.ADD), new LocalSlaveMySQLEndpointObservable());
        Map<String, String> mha2MasterEndpointTags = btdhsMonitor.getEntity(mha3MasterEndpoint, META_KEY3).getTags();

        btdhsMonitor.scheduledTask();
        verify(reporter, times(2)).resetReportCounter(
                Mockito.any(), 
                Mockito.anyLong(), 
                Mockito.eq(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement())
        );
        verify(reporter, never()).resetReportCounter(
                Mockito.eq(mha2MasterEndpointTags), 
                Mockito.anyLong(), 
                Mockito.eq(BINLOG_TRANSACTION_DEPENDENCY_HISTORY_SIZE_MEASUREMENT.getMeasurement())
        );
    }


}
