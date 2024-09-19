package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaAzView;
import com.ctrip.framework.drc.console.vo.v2.MhaSyncView;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by shiruixin
 * 2024/7/23 19:45
 */
public class MhaSyncStatusCheckTaskTest {
    @InjectMocks
    private MhaSyncStatusCheckTask task;
    @Mock
    private MhaReplicationServiceV2 mhaReplicationServiceV2;
    @Mock
    private Reporter reporter;
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask () throws Exception {
        MhaSyncView view = new MhaSyncView();
        view.setMhaSyncIds(Sets.newHashSet(0L));
        view.setDbNameSet(Sets.newHashSet("testDbName"));
        view.setDbSyncSet(Sets.newHashSet("testSrcDbName->testDstDbName"));
        view.setDalClusterSet(Sets.newHashSet("testDalClusterName"));
        view.setDbMessengerSet(Sets.newHashSet("testDbMessenger"));
        view.setDbOtterSet(Sets.newHashSet("testOtter"));

        Mockito.when(mhaReplicationServiceV2.mhaSyncCount()).thenReturn(view);
        task.check();
        Mockito.verify(reporter,Mockito.times(6)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
    }
}
