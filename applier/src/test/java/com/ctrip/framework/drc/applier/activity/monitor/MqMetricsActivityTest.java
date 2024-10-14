package com.ctrip.framework.drc.applier.activity.monitor;

import org.junit.Test;


/**
 * Created by shiruixin
 * 2024/10/11 19:12
 */
public class MqMetricsActivityTest {

    @Test
    public void testDoTask() {
        MqMetricsActivity report = new MqMetricsActivity();
        MqMonitorContext context = new MqMonitorContext(0, "registryKey", "messenger.active");
        report.doTask(context);
    }
}
