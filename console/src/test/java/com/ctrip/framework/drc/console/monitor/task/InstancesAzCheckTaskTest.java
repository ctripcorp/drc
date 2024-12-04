package com.ctrip.framework.drc.console.monitor.task;

import com.ctrip.framework.drc.console.service.v2.PojoBuilder;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Created by shiruixin
 * 2024/9/14 10:54
 */
public class InstancesAzCheckTaskTest {
    @InjectMocks
    private InstancesAzCheckTask task;
    @Mock
    private Reporter reporter;
    @Mock
    private ResourceService resourceService;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testScheduledTask () throws Exception {
        Mockito.when(resourceService.getAllInstanceAzInfo()).thenReturn(PojoBuilder.getMhaAzView());
        task.check();
        Mockito.verify(reporter,Mockito.times(6)).resetReportCounter(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
    }
}
