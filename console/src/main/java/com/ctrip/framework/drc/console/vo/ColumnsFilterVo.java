package com.ctrip.framework.drc.console.vo;

import java.util.List;

/**
 * @ClassName ColumnsFilterVo
 * @Author haodongPan
 * @Date 2023/1/18 15:22
 * @Version: $
 */
public class ColumnsFilterVo {
    
    private Long id;
    
    private String mode;
    
    private List<String> columns;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
