package com.ctrip.framework.drc.messenger.activity.monitor;

import com.ctrip.framework.drc.core.monitor.reporter.DefaultReporterHolder;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Created by shiruixin
 * 2024/11/8 16:28
 */
public class MqMetricsActivityTest {
    @Test
    public void testDoTask() {
        Reporter mockReporter = Mockito.mock(Reporter.class);
        MockedStatic<DefaultReporterHolder> mockedStatic = Mockito.mockStatic(DefaultReporterHolder.class);
        mockedStatic.when(() -> DefaultReporterHolder.getInstance()).thenReturn(mockReporter);

        MqMetricsActivity report = new MqMetricsActivity();
        MqMonitorContext context = new MqMonitorContext(0, "registryKey", "messenger.active");
        report.doTask(context);
        Mockito.verify(mockReporter, Mockito.times(1)).reportMessengerDelay(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
        mockedStatic.close();
    }

    @Test
    public void testDoTask2() {
        Reporter mockReporter = Mockito.mock(Reporter.class);
        MockedStatic<DefaultReporterHolder> mockedStatic = Mockito.mockStatic(DefaultReporterHolder.class);
        mockedStatic.when(() -> DefaultReporterHolder.getInstance()).thenReturn(mockReporter);

        MqMetricsActivity report = new MqMetricsActivity();
        MqMonitorContext context = new MqMonitorContext(0, "registryKey", "messenger.active", "applyActivity");
        report.doTask(context);
        Mockito.verify(mockReporter, Mockito.times(1)).reportMessengerDelay(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString());
        mockedStatic.close();
    }
}