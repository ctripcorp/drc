package com.ctrip.framework.drc.console.vo.response.migrate;

/**
 * Created by dengquanliang
 * 2023/6/15 17:00
 */
public class MigrateResult {
    private int insertSize;
    private int updateSize;
    private int deleteSize;
    private int expectedSize;

    public MigrateResult(int insertSize, int updateSize, int deleteSize, int expectedSize) {
        this.insertSize = insertSize;
        this.updateSize = updateSize;
        this.deleteSize = deleteSize;
        this.expectedSize = expectedSize;
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

    public int getDeleteSize() {
        return deleteSize;
    }

    public void setDeleteSize(int deleteSize) {
        this.deleteSize = deleteSize;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public void setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
    }
}
