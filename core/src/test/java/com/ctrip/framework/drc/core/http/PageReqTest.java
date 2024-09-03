package com.ctrip.framework.drc.core.http;

import org.junit.Assert;
import org.junit.Test;

public class PageReqTest {

    @Test
    public void testGetPageIndex() {
        PageReq pageReq = new PageReq();
        Assert.assertEquals(1, pageReq.getPageIndex());
    }

    @Test
    public void testGetPageSize() {
        PageReq pageReq = new PageReq();
        Assert.assertEquals(20, pageReq.getPageSize());
    }

    @Test
    public void testSetPageIndex() {
        PageReq pageReq = new PageReq();
        pageReq.setPageIndex(10);
        Assert.assertEquals(pageReq.getPageIndex(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPageIndexException() {
        PageReq pageReq = new PageReq();
        pageReq.setPageIndex(-1);
    }


    @Test
    public void testSetPageSize() {
        PageReq pageReq = new PageReq();
        for (int i = 1; i <= 1000; i++) {
            pageReq.setPageSize(i);
            Assert.assertEquals(pageReq.getPageSize(), i);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPageSizeException1() {
        PageReq pageReq = new PageReq();
        pageReq.setPageSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPageSizeException2() {
        PageReq pageReq = new PageReq();
        pageReq.setPageSize(1001);
    }

}