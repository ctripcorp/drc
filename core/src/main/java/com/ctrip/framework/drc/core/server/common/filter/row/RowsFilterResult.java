package com.ctrip.framework.drc.core.server.common.filter.row;

/**
 * @Author limingdong
 * @create 2022/4/24
 */
public class RowsFilterResult<V> {

    private Status noRowFiltered;

    private V res;

    public RowsFilterResult(Status noRowFiltered) {
        this.noRowFiltered = noRowFiltered;
    }

    public RowsFilterResult(Status noRowFiltered, V res) {
        this.noRowFiltered = noRowFiltered;
        this.res = res;
    }

    public Status isNoRowFiltered() {
        return noRowFiltered;
    }

    public V getRes() {
        return res;
    }

    public void setRes(V res) {
        this.res = res;
    }

    public enum Status {

        No_Filtered(true),

        Filtered(false),

        Illegal(false),

        No_Filter_Rule(true);

        private boolean noRowFiltered;

        Status(boolean noRowFiltered) {
            this.noRowFiltered = noRowFiltered;
        }

        public boolean noRowFiltered() {
            return noRowFiltered;
        }

        public static Status from(boolean res) {
            return res ? No_Filtered : Filtered;
        }
    }
}
