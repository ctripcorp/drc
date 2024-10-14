package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.applier.activity.monitor.MqMetricsActivity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by shiruixin
 * 2024/10/11 17:24
 */
public class MqProviderResourceTest {
    private MqProviderResource mqProviderResource;
    private ScheduledExecutorService executorService;
    private MqMetricsActivity reporter;

    @Before
    public void setUp() throws Exception {
        mqProviderResource = new MqProviderResource();
        mqProviderResource.registryKey = "registryKey";
        executorService = Mockito.mock(ScheduledExecutorService.class);
        Field field = MqProviderResource.class.getDeclaredField("scheduledExecutorService");
        field.setAccessible(true);
        field.set(mqProviderResource, executorService);
        reporter = Mockito.mock(MqMetricsActivity.class);
        mqProviderResource.reporter = reporter;
    }

    @Test
    public void testReportMessengerActive() throws Exception {
        mqProviderResource.doInitialize();
        Thread.sleep(1000);
        Mockito.verify(reporter, Mockito.atLeastOnce()).report(Mockito.any());
    }
}
