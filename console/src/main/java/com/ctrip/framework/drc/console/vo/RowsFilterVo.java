package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.xpipe.codec.JsonCodec;

import java.util.List;

/**
 * @ClassName RowsFilterVo
 * @Author haodongPan
 * @Date 2022/5/19 14:20
 * @Version: $
 */
public class RowsFilterVo {
    private Long id;
    
    private String name;
    
    private String mode;
    
    private List<String> columns;
    
    private String content;
    
    public RowsFilterVo (RowsFilterTbl rowsFilterTbl) {
        this.id = rowsFilterTbl.getId();
        this.name = rowsFilterTbl.getName();
        this.mode = rowsFilterTbl.getMode();
        String parametersJson = rowsFilterTbl.getParameters();
        RowsFilterConfig.Parameters parameters = 
                JsonCodec.INSTANCE.decode(parametersJson, RowsFilterConfig.Parameters.class);
        this.columns = parameters.getColumns();
        this.content = parameters.getContext();
    }
    
    @Override
    public String toString() {
        return "RowsFilterVo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", mode='" + mode + '\'' +
                ", columns=" + columns +
                ", content='" + content + '\'' +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
