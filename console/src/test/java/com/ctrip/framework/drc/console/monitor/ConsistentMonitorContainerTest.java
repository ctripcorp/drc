package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.enums.ActionEnum;
import com.ctrip.framework.drc.console.mock.LocalSlaveMySQLEndpointObservable;
import com.ctrip.framework.drc.console.monitor.consistency.container.ConsistencyCheckContainer;
import com.ctrip.framework.drc.console.monitor.consistency.instance.InstanceConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.FullDataConsistencyMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceTwoImpl;
import com.ctrip.framework.drc.console.service.monitor.MonitorService;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.unidal.tuple.Triple;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static com.ctrip.framework.drc.console.utils.UTConstants.*;

/**
 * Created by mingdongli
 * 2019/12/19 下午2:49.
 */
public class ConsistentMonitorContainerTest extends MockTest {

    @InjectMocks
    private ConsistentMonitorContainer consistentMonitorContainer;

    @Mock
    private ConsistencyCheckContainer checkContainer;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;

    @Mock
    private MetaInfoServiceTwoImpl metaInfoServiceTwo;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Mock
    private MonitorService monitorService;

    private static final String LOCAL_DC = "shaoy";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        when(dbClusterSourceProvider.getLocalDcName()).thenReturn(LOCAL_DC);
        when(checkContainer.addConsistencyCheck(any(InstanceConfig.class))).thenReturn(true);
        when(monitorService.queryMhaIdsToBeMonitored()).thenReturn(List.of(1L,2L,100L,0L));
        Mockito.doNothing().when(currentMetaManager).addObserver(consistentMonitorContainer);
        consistentMonitorContainer.isleader();
    }

    @Test
    public void testInit() throws Exception {
        Mockito.doReturn(1L).when(metaInfoService).getMhaGroupId(anyString());
        consistentMonitorContainer.initDaos();
        int res = consistentMonitorContainer.schedule();
        Assert.assertEquals(3, res);
    }

    @Test
    public void testInit2() throws Exception {
        Mockito.doReturn("on").when(monitorTableSourceProvider).getGeneralDataConsistentMonitorSwitch();
        Mockito.doNothing().when(currentMetaManager).addObserver(Mockito.any());
        consistentMonitorContainer.init();
        Mockito.verify(currentMetaManager, times(1)).addObserver(Mockito.any());
    }

    @Test
    public void testRecordInconsistencyResult() throws SQLException {
        FullDataConsistencyMonitorConfig config = new FullDataConsistencyMonitorConfig();
        config.setSchema("testSchema");
        config.setTable("testTable");
        config.setKey("testKey");
        consistentMonitorContainer.recordInconsistencyResult(config, 200, new Timestamp(System.currentTimeMillis()), Sets.newHashSet("1", "2"));
    }

    @Test
    public void testObserve() throws Exception {
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl.setMonitorSchemaName(SCHEMA_NAME);
        dataConsistencyMonitorTbl.setMonitorTableName(TABLE_NAME);
        Mockito.doReturn(Arrays.asList(dataConsistencyMonitorTbl)).when(metaInfoServiceTwo).getAllDataConsistencyMonitorTbl(MHA1DC1);
        Mockito.doNothing().when(checkContainer).removeConsistencyCheck(Mockito.anyString());

        consistentMonitorContainer.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.DELETE), new LocalSlaveMySQLEndpointObservable());
        verify(checkContainer, times(1)).removeConsistencyCheck(Mockito.anyString());
        verify(checkContainer, times(1)).removeConsistencyCheck(TABLE);
    }

    @Test
    public void testObserveForException() throws Exception {
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl.setMonitorSchemaName(SCHEMA_NAME);
        dataConsistencyMonitorTbl.setMonitorTableName(TABLE_NAME);
        Mockito.doThrow(new SQLException()).when(metaInfoServiceTwo).getAllDataConsistencyMonitorTbl(MHA1DC1);
        Mockito.doNothing().when(checkContainer).removeConsistencyCheck(Mockito.anyString());

        consistentMonitorContainer.update(new Triple<>(META_KEY1, MYSQL_ENDPOINT1_MHA1DC1, ActionEnum.DELETE), new LocalSlaveMySQLEndpointObservable());
        verify(checkContainer, never()).removeConsistencyCheck(Mockito.anyString());
        verify(checkContainer, never()).removeConsistencyCheck(TABLE);
    }
}
