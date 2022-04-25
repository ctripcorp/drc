package com.ctrip.framework.drc.replicator.impl.oubound.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class RowsFilterResult<V> {

    private boolean noRowFiltered;

    private V res;

    public RowsFilterResult(boolean noRowFiltered) {
        this.noRowFiltered = noRowFiltered;
    }

    public RowsFilterResult(boolean noRowFiltered, V res) {
        this.noRowFiltered = noRowFiltered;
        this.res = res;
    }

    public boolean isNoRowFiltered() {
        return noRowFiltered;
    }

    public void setNoRowFiltered(boolean noRowFiltered) {
        this.noRowFiltered = noRowFiltered;
    }

    public V getRes() {
        return res;
    }

    public void setRes(V res) {
        this.res = res;
    }
}
