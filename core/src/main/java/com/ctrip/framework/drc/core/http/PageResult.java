package com.ctrip.framework.drc.core.http;

import java.util.Collections;
import java.util.List;

public class PageResult<T> {
    private List<T> data;
    private int pageIndex;
    private int pageSize;
    private int totalCount;

    public static <T> PageResult<T> newInstance(List<T> data, int pageIndex, int pageSize, int totalCount) {
        PageResult<T> result = new PageResult<>();
        result.data = data;
        result.pageIndex = pageIndex;
        result.pageSize = pageSize;
        result.totalCount = totalCount;
        return result;
    }

    public static <T> PageResult<T> emptyResult() {
        PageResult<T> result = new PageResult<>();
        result.data = Collections.emptyList();
        result.pageIndex = 1;
        result.totalCount = 0;
        return result;
    }

    public List<T> getData() {
        return data;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getTotalCount() {
        return totalCount;
    }
}
