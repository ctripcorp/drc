package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.console.vo.v2.MhaReplicationView;
import com.ctrip.framework.drc.console.vo.v2.ResourceSameAzView;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by shiruixin
 * 2024/7/24 19:43
 */
public class ResourceAzCheckTaskTest {
    @InjectMocks
    private ResourceAzCheckTask task;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private ResourceService resourceService;
    @Mock
    private Reporter reporter;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask () throws Exception {
        ResourceSameAzView view = new ResourceSameAzView();
        view.setApplierDbList(new ArrayList<>(Collections.singletonList("testApplierDb")));
        view.setApplierMhaReplicationList(new ArrayList<>(Collections.singletonList(new MhaReplicationView())));
        view.setMessengerMhaList(new ArrayList<>(Collections.singletonList("testMessengerMha")));
        view.setReplicatorMhaList(new ArrayList<>(Collections.singletonList("testReplicatorMha")));

        Mockito.when(resourceService.checkResourceAz()).thenReturn(view);
        task.check();
        Mockito.verify(reporter,Mockito.times(4)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
    }

}
