package com.ctrip.framework.drc.console.dto;

import java.util.List;
import java.util.Map;

/**
 * Created by jixinwang on 2021/2/24
 */
public class CurrentRecordDto {
    private List<Map<String, Object>> columnPattern;
    private List<String> columnNameList;
    private List<CurrentResultSetDto> currentResultSetDto;
    private List<String> keyValueList;

    public List<Map<String, Object>> getColumnPattern() {
        return columnPattern;
    }

    public void setColumnPattern(List<Map<String, Object>> columnPattern) {
        this.columnPattern = columnPattern;
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public List<CurrentResultSetDto> getCurrentResultSetDto() {
        return currentResultSetDto;
    }

    public void setCurrentResultSetDto(List<CurrentResultSetDto> currentResultSetDto) {
        this.currentResultSetDto = currentResultSetDto;
    }

    public List<String> getKeyValueList() {
        return keyValueList;
    }

    public void setKeyValueList(List<String> keyValueList) {
        this.keyValueList = keyValueList;
    }
}
