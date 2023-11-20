package com.ctrip.framework.drc.console.param.log;

/**
 * Created by dengquanliang
 * 2023/11/3 3:13 下午
 */
public class ConflictHandleSqlDto {
    private Long rowLogId;
    private String handleSql;

    public Long getRowLogId() {
        return rowLogId;
    }

    public void setRowLogId(Long rowLogId) {
        this.rowLogId = rowLogId;
    }

    public String getHandleSql() {
        return handleSql;
    }

    public void setHandleSql(String handleSql) {
        this.handleSql = handleSql;
    }
}
