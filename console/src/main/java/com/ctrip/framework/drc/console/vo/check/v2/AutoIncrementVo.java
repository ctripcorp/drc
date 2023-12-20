package com.ctrip.framework.drc.console.vo.check.v2;

/**
 * Created by dengquanliang
 * 2023/12/15 16:38
 */
public class AutoIncrementVo {
    private int increment;
    private int offset;

    public AutoIncrementVo(int increment, int offset) {
        this.offset = offset;
        this.increment = increment;
    }

    public AutoIncrementVo() {
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getIncrement() {
        return increment;
    }

    public void setIncrement(int increment) {
        this.increment = increment;
    }
}
