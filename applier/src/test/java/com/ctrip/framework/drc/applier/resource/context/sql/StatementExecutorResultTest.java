package com.ctrip.framework.drc.applier.resource.context.sql;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @Author Slight
 * Jul 05, 2020
 */
public class StatementExecutorResultTest {

    @Test
    public void name() {
        assertEquals("BATCHED", StatementExecutorResult.TYPE.BATCHED.toString());
    }
}