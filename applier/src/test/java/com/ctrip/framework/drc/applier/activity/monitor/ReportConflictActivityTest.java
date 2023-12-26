package com.ctrip.framework.drc.applier.activity.monitor;


import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import com.ctrip.framework.drc.fetcher.conflict.ConflictTransactionLog;
import com.ctrip.framework.drc.fetcher.resource.thread.ExecutorResource;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestOperations;

import static org.mockito.Mockito.mock;

/**
 * Created by jixinwang on 2020/10/14
 */
public class ReportConflictActivityTest {

    @Test
    public void testUploadConflictLog() throws Exception {
        MockitoAnnotations.openMocks(this);
        RestOperations restTemplate = mock(RestOperations.class);
        ExecutorResource executorResource = new ExecutorResource();
        executorResource.initialize();
        ReportConflictActivity reportConflictActivity = new TmpReportConflictActivity();
        reportConflictActivity.executor = executorResource;
        reportConflictActivity.setRestTemplate(restTemplate);
        
        reportConflictActivity.initialize();
        
        // test config in drc.properties
        ConflictTransactionLog conflictTransactionLog = new ConflictTransactionLog();
        boolean report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);
        report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);

        
        // {
        //		"srcMha": "mha1",
        //		"dstMha": "mha2",
        //		"gtid": "uuid1:1",
        //		"trxRowsNum": 1000,
        //		"cflRowsNum": 200,
        //		"trxRes": 0 
        //		"handleTime": 1695633004624, 
        //		"cflLogs": [ 
        //			{
        //				"db": "db",
        //				"table" : "table",
        //				"rawSql": "",
        //				"rawSqlRes": "",
        //				"dstRowRecord": "",
        //				"handleSql" "",
        //				"handleSqlRes": "",
        //				"rowRes": 0 
        //			}
        //		]
        // }
        ConflictRowLog conflictRowLog1 = new ConflictRowLog();
        ConflictRowLog conflictRowLog2 = new ConflictRowLog();
        conflictTransactionLog.setCflLogs(Lists.newArrayList(conflictRowLog1, conflictRowLog2));
        report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);

        
        report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertFalse(report);
        
    }

    @Test
    public void testDoReport() throws Exception {
        MockitoAnnotations.openMocks(this);
        RestOperations restTemplate = mock(RestOperations.class);
        ExecutorResource executorResource = new ExecutorResource();
        executorResource.initialize();
        ReportConflictActivity reportConflictActivity = new TmpReportConflictActivity();
        reportConflictActivity.executor = executorResource;
        reportConflictActivity.setRestTemplate(restTemplate);

        reportConflictActivity.initialize();
        reportConflictActivity.start();
        
        // test config in drc.properties
        ConflictTransactionLog conflictTransactionLog = new ConflictTransactionLog();
        boolean report = reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);

        reportConflictActivity.report(conflictTransactionLog);
        Assert.assertTrue(report);

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
