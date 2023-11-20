package com.ctrip.framework.drc.monitor.performance;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConflictTestCaseTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testGetInsertSql() throws InterruptedException {
        ConflictTestCase conflictTestCase = new ConflictTestCase();
        conflictTestCase.executorService.submit(() -> {
            conflictTestCase.getInsertSql();
        });
        conflictTestCase.executorService.submit(() -> {
            conflictTestCase.getInsertSql();
        });
        conflictTestCase.executorService.submit(() -> {
            conflictTestCase.getInsertSql();
        });
        Thread.sleep(200);
        Assert.assertEquals(3, conflictTestCase.pk.get());
    }
}