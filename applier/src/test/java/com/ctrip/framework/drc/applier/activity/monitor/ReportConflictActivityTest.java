package com.ctrip.framework.drc.applier.activity.monitor;

import com.ctrip.framework.drc.applier.activity.monitor.entity.ConflictTransactionLog;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import static org.mockito.Mockito.mock;

/**
 * Created by jixinwang on 2020/10/14
 */
public class ReportConflictActivityTest {

    @Test
    public void testUploadConflictLog() throws Exception {
        RestOperations restTemplate = mock(RestOperations.class);
        ExecutorResource executorResource = new ExecutorResource();
        executorResource.initialize();
        ReportConflictActivity reportConflictActivity = new TmpReportConflictActivity();
        reportConflictActivity.executor = executorResource;
        reportConflictActivity.setRestTemplate(restTemplate);
        reportConflictActivity.conflictLogUploadSwitch = "on";
        reportConflictActivity.initialize();
        reportConflictActivity.start();
        ConflictTransactionLog conflictTransactionLog = new ConflictTransactionLog();
        boolean report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);
        report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);
        report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertFalse(report);
        Thread.sleep(200);
        reportConflictActivity.stop();
        reportConflictActivity.dispose();
    }

    class TmpReportConflictActivity extends ReportConflictActivity {
        @Override
        public int queueSize() {
            return 2;
        }
    }
}
