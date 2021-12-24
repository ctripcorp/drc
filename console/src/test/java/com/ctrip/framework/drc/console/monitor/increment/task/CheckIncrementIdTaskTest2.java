package com.ctrip.framework.drc.console.monitor.increment.task;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.core.entity.DbCluster;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

public class CheckIncrementIdTaskTest2 {

    @InjectMocks
    private CheckIncrementIdTask checkIncrementIdTask;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testIsFilteredOut() {
        Mockito.doReturn(ArrayUtils.EMPTY_STRING_ARRAY).when(monitorTableSourceProvider).getFilterOutMhasForMultiSideMonitor();
        DbCluster dbCluster = new DbCluster();
        dbCluster.setMhaName("mha1");
        DbCluster dbCluster2 = new DbCluster();
        dbCluster2.setMhaName("mha2");
        Set<DbClusterSourceProvider.Mha> mhas = new HashSet<>() {{
            add(new DbClusterSourceProvider.Mha("dc1", dbCluster));
            add(new DbClusterSourceProvider.Mha("dc2", dbCluster2));
        }};
        Assert.assertFalse(checkIncrementIdTask.isFilteredOut(mhas));

        Mockito.doReturn(new String[]{"mha1"}).when(monitorTableSourceProvider).getFilterOutMhasForMultiSideMonitor();
        Assert.assertTrue(checkIncrementIdTask.isFilteredOut(mhas));

    }
}
