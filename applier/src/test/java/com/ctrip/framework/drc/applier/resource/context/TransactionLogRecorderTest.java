package com.ctrip.framework.drc.applier.resource.context;

import com.ctrip.framework.drc.applier.utils.ApplierDynamicConfig;
import com.ctrip.framework.drc.core.monitor.enums.ConflictDetail;
import com.ctrip.framework.drc.fetcher.conflict.ConflictRowLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.PriorityQueue;

import static com.ctrip.framework.drc.core.monitor.enums.ConflictResult.ROLLBACK;

/**
 * Created by shiruixin
 * 2025/6/30 15:58
 */
public class TransactionLogRecorderTest {
    TransactionLogRecorder trxLogRecorder;
    ApplierDynamicConfig mockConfig = Mockito.mock(ApplierDynamicConfig.class);;
    MockedStatic<ApplierDynamicConfig> theMock;
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        trxLogRecorder = new TransactionLogRecorder(2);
        theMock = Mockito.mockStatic(ApplierDynamicConfig.class);
        theMock.when(() -> ApplierDynamicConfig.getInstance()).thenReturn(mockConfig);
    }

    @After
    public void tearDown() throws Exception {
        theMock.close();
    }

    @Test
    public void testCflRowLogsQueue() {
        trxLogRecorder.setUploadLevel(ConflictDetail.AlertLevel.WARN);
//        Mockito.when(mockConfig.getConflictLogUpLevel()).thenReturn(Set.of(ConflictDetail.AlertLevel.WARN, ConflictDetail.AlertLevel.CRITICAL));
        ConflictRowLog l1 = new ConflictRowLog();
        l1.setDb("db");
        l1.setTable("tb");
        l1.setRowId(1L);
        l1.setRowRes(ROLLBACK.getValue());
        l1.setConflictDetail(ConflictDetail.INSERT_TO_UPDATE_NEWER_EXIST);


        ConflictRowLog l2 = new ConflictRowLog();
        l2.setDb("db");
        l2.setTable("tb");
        l2.setRowId(2L);
        l2.setRowRes(ROLLBACK.getValue());
        l2.setConflictDetail(ConflictDetail.INSERT_TO_UPDATE);

        ConflictRowLog l3 = new ConflictRowLog();
        l3.setDb("db");
        l3.setTable("tb");
        l3.setRowId(3L);
        l3.setRowRes(ROLLBACK.getValue());
        l3.setConflictDetail(ConflictDetail.INSERT_TO_UPDATE_SAME_EXIST);

        trxLogRecorder.recordCflRowLogIfNecessary(l1);
        trxLogRecorder.recordCflRowLogIfNecessary(l3);
        trxLogRecorder.recordCflRowLogIfNecessary(l2);
        PriorityQueue<ConflictRowLog> queue = trxLogRecorder.getCflRowLogsQueue();
        ConflictRowLog l1InQueue = queue.poll();
        ConflictRowLog l2InQueue = queue.poll();
        Assert.assertTrue(l1InQueue.equals(l2));
        Assert.assertTrue(l2InQueue.equals(l1));
    }
}