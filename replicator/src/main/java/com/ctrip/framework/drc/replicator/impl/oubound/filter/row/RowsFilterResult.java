package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class RowsFilterResult<V> {

    private boolean filtered;

    private V res;

    public RowsFilterResult(boolean filtered) {
        this.filtered = filtered;
    }

    public RowsFilterResult(boolean filtered, V res) {
        this.filtered = filtered;
        this.res = res;
    }

    public boolean isFiltered() {
        return filtered;
    }

    public void setFiltered(boolean filtered) {
        this.filtered = filtered;
    }

    public V getRes() {
        return res;
    }

    public void setRes(V res) {
        this.res = res;
    }
}
