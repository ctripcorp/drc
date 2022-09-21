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

        No_Filtered(true, false),

        Filtered(false, false),

        Illegal(true, true);

        private boolean noRowFiltered;

        private boolean illegal;

        Status(boolean noRowFiltered, boolean illegal) {
            this.noRowFiltered = noRowFiltered;
            this.illegal = illegal;
        }

        public boolean noRowFiltered() {
            return noRowFiltered;
        }

        public boolean isIllegal() {
            return illegal;
        }

        public static Status from(boolean res) {
            return res ? No_Filtered : Filtered;
        }
    }
}
