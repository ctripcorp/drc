package com.ctrip.framework.drc.console.task;

import ch.vorburger.exec.ManagedProcessException;
import com.ctrip.framework.drc.console.dao.entity.DataConsistencyMonitorTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.monitor.delay.config.DelayMonitorConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.service.monitor.impl.ConsistencyConsistencyMonitorServiceImpl;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;
import static com.ctrip.framework.drc.console.utils.UTConstants.*;
import static org.mockito.Mockito.verify;

/**
 * @Author: hbshen
 * @Date: 2021/4/28
 */
public class UpdateDataConsistencyMetaTaskTest {

    @InjectMocks
    private UpdateDataConsistencyMetaTask task;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private MetaGenerator metaService;

    @Mock
    private ConsistencyConsistencyMonitorServiceImpl monitorService;

    @Before
    public void setUp() throws ManagedProcessException {
        MockitoAnnotations.openMocks(this);

        Mockito.doReturn(SWITCH_STATUS_ON).when(monitorTableSourceProvider).getUpdateConsistencyMetaSwitch();
        task.isleader();
    }

    @Test
    public void testAddAndDeleteConsistencyMeta() throws SQLException {
        MhaTbl mhaTbl1 = new MhaTbl();
        MhaTbl mhaTbl2 = new MhaTbl();
        mhaTbl1.setId(1L);
        mhaTbl2.setId(2L);
        mhaTbl1.setMhaName(MHA1DC1);
        mhaTbl2.setMhaName(MHA1DC2);
        List<MhaTbl> mhaTbls = Arrays.asList(mhaTbl1, mhaTbl2);

        DelayMonitorConfig delayMonitorConfig1= new DelayMonitorConfig();
        delayMonitorConfig1.setSchema(SCHEMA_NAME0);
        delayMonitorConfig1.setTable(TABLE_NAME0);
        delayMonitorConfig1.setKey(KEY);
        delayMonitorConfig1.setOnUpdate(ON_UPDATE);
        DelayMonitorConfig delayMonitorConfig2= new DelayMonitorConfig();
        delayMonitorConfig2.setSchema(SCHEMA_NAME1);
        delayMonitorConfig2.setTable(TABLE_NAME1);
        delayMonitorConfig2.setKey(KEY);
        delayMonitorConfig2.setOnUpdate(ON_UPDATE);
        Map<String, DelayMonitorConfig> delayMonitorConfigs = new HashMap<>() {{
           put(TABLE, delayMonitorConfig1);
           put(TABLE1, delayMonitorConfig2);
        }};

        DataConsistencyMonitorTbl dataConsistencyMonitorTbl1 = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl1.setId(1);
        dataConsistencyMonitorTbl1.setMhaId(1);
        dataConsistencyMonitorTbl1.setMonitorSchemaName(SCHEMA_NAME1);
        dataConsistencyMonitorTbl1.setMonitorTableName(TABLE_NAME1);
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl2 = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl2.setId(2);
        dataConsistencyMonitorTbl2.setMhaId(2);
        dataConsistencyMonitorTbl2.setMonitorSchemaName(SCHEMA_NAME2);
        dataConsistencyMonitorTbl2.setMonitorTableName(TABLE_NAME2);
        DataConsistencyMonitorTbl dataConsistencyMonitorTbl3 = new DataConsistencyMonitorTbl();
        dataConsistencyMonitorTbl3.setId(3);
        dataConsistencyMonitorTbl3.setMhaId(3);
        dataConsistencyMonitorTbl3.setMonitorSchemaName(SCHEMA_NAME3);
        dataConsistencyMonitorTbl3.setMonitorTableName(TABLE_NAME3);
        List<DataConsistencyMonitorTbl> dataConsistencyMonitorTbls = Arrays.asList(dataConsistencyMonitorTbl1, dataConsistencyMonitorTbl2, dataConsistencyMonitorTbl3);
        Mockito.doReturn(dataConsistencyMonitorTbls).when(metaService).getDataConsistencyMonitorTbls();

        Mockito.doNothing().when(monitorService).addDataConsistencyMonitor(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.doNothing().when(monitorService).deleteDataConsistencyMonitor(Mockito.anyInt());
        Pair<Integer, Integer> result = task.addAndDeleteConsistencyMeta(mhaTbls, delayMonitorConfigs);
        Assert.assertEquals(1, result.getKey().intValue());
        Assert.assertEquals(1, result.getValue().intValue());
        verify(monitorService, times(1)).addDataConsistencyMonitor(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        verify(monitorService, times(1)).addDataConsistencyMonitor(MHA1DC1, MHA1DC2, delayMonitorConfig1);
        verify(monitorService, times(1)).deleteDataConsistencyMonitor(Mockito.anyInt());
        verify(monitorService, times(1)).deleteDataConsistencyMonitor(2);


        Mockito.doThrow(new SQLException()).when(monitorService).addDataConsistencyMonitor(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.doThrow(new SQLException()).when(monitorService).deleteDataConsistencyMonitor(Mockito.anyInt());
        result = task.addAndDeleteConsistencyMeta(mhaTbls, delayMonitorConfigs);
        Assert.assertEquals(0, result.getKey().intValue());
        Assert.assertEquals(0, result.getValue().intValue());
    }
}
