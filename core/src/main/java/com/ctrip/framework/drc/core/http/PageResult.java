package com.ctrip.framework.drc.core.http;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/11/23 下午7:22.
 */
public class PageResult<T> {
    private List<T> data;
    private Integer pageIndex;
    private Integer pageSize;
    private Integer totalCount;

    public static <T> PageResult<T> newInstance(List<T> data, Integer pageIndex, Integer pageSize, Integer totalCount) {
        PageResult<T> result = new PageResult<>();
        result.data = data;
        result.pageIndex = pageIndex;
        result.pageSize = pageSize;
        result.totalCount = totalCount;
        return result;
    }

    public List<T> getData() {
        return data;
    }

    public Integer getPageIndex() {
        return pageIndex;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public Integer getTotalCount() {
        return totalCount;
    }
}
