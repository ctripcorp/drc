package com.ctrip.framework.drc.core.server.common.filter.column;

import java.util.List;

/**
 * Created by jixinwang on 2022/12/30
 */
public class ColumnsFilterContext {

    private String tableName;

    private List<Integer> extractedColumnsIndex;

    public ColumnsFilterContext(String tableName, List<Integer> extractedColumnsIndex) {
        this.tableName = tableName;
        this.extractedColumnsIndex = extractedColumnsIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Integer> getExtractedColumnsIndex() {
        return extractedColumnsIndex;
    }

    public void setExtractedColumnsIndex(List<Integer> extractedColumnsIndex) {
        this.extractedColumnsIndex = extractedColumnsIndex;
    }
}
