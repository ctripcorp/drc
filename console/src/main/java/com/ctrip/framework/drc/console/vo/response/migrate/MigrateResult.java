package com.ctrip.framework.drc.console.vo.response.migrate;

/**
 * Created by dengquanliang
 * 2023/6/15 17:00
 */
public class MigrateResult {
    private int insertSize;
    private int updateSize;

    public MigrateResult(int insertSize, int updateSize) {
        this.insertSize = insertSize;
        this.updateSize = updateSize;
    }

    public int getInsertSize() {
        return insertSize;
    }

    public void setInsertSize(int insertSize) {
        this.insertSize = insertSize;
    }

    public int getUpdateSize() {
        return updateSize;
    }

    public void setUpdateSize(int updateSize) {
        this.updateSize = updateSize;
    }
}
