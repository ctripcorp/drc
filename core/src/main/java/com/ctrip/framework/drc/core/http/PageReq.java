package com.ctrip.framework.drc.core.http;

public class PageReq {

    public static final int MAX_PAGE_SIZE = 200;
    private int pageIndex = 1;
    private int pageSize = 20;

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        if (pageIndex < 1) {
            throw new IllegalArgumentException("pageIndex should >= 1");
        }
        this.pageIndex = pageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize should > 0");
        }
        if (pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize should <= 200");
        }
        this.pageSize = pageSize;
    }
}
