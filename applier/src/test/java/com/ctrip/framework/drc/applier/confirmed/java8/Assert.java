package com.ctrip.framework.drc.applier.confirmed.java8;

import com.ctrip.framework.drc.fetcher.event.FetcherRowsEvent;
import com.ctrip.framework.drc.applier.event.ApplierWriteRowsEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Nov 13, 2019
 */
public class Assert {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Test (expected = AssertionError.class)
    public void doAssertCatchException() {
        try {
            assert 1 == 2;
        } catch (Exception e) {

        }
    }

    @Test
    public void doAssertCatchThrowable() {
        int a = 1;
        try {
            assert 1 == 2;
            a = 2;
        } catch (Throwable t) {

        }
        assert a == 1;
    }

    @Test
    public void doAssertInstanceOf() {
        ApplierWriteRowsEvent event = new ApplierWriteRowsEvent();
        assert event instanceof FetcherRowsEvent;
        assert (event instanceof FetcherRowsEvent);
    }

    public int useFinally0() {
        int i = 0;
        try {
            assert false;
        } finally {
            i = 1;
            logger.info("inner: {}", i);
        }
        return i;
    }


    @Test (expected = AssertionError.class)
    public void testFinally() {
        int i = -1;
        try {
            i = useFinally0();
        } catch (Throwable t) {
            logger.info("out: {}", i);
            throw t;
        }
    }

    public int useFinally1() {
        int i = 0;
        try {
            assert false;
        } finally {
            i = 1;
            logger.info("inner: {}", i);
            return i;
        }
    }

    @Test
    public void shutFinally() {
        int i = -1;
        try {
            i = useFinally1();
            logger.info("out: {}", i);
        } catch (Throwable t) {
            throw t;
        }
    }

    @Test (expected = NullPointerException.class)
    public void emptyMessageNPE() {
        try {
            assert false;
        } catch (Throwable t) {
            t.getMessage().equals("");
        }
    }

    @Test
    public void emptyMessage() {
        try {
            assert false;
        } catch (Throwable t) {
            assertFalse("".equals(t.getMessage()));
        }
    }
}
