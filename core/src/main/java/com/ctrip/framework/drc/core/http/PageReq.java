package com.ctrip.framework.drc.core.http;

import java.io.Serializable;
import java.util.Objects;

public class PageReq implements Serializable {

    public static final int MAX_PAGE_SIZE = 1000;
    private int pageIndex = 1;
    private int pageSize = 20;
    private int totalCount = 0;

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

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
            throw new IllegalArgumentException("pageSize should <= " + MAX_PAGE_SIZE);
        }
        this.pageSize = pageSize;
    }

    @Override
    public String toString() {
        return "PageReq{" +
                "pageIndex=" + pageIndex +
                ", pageSize=" + pageSize +
                ", totalCount=" + totalCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PageReq)) return false;
        PageReq pageReq = (PageReq) o;
        return pageIndex == pageReq.pageIndex && pageSize == pageReq.pageSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageIndex, pageSize);
    }
}
