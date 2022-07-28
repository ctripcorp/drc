package com.ctrip.framework.drc.console.vo;


import com.ctrip.framework.drc.console.dao.entity.DataMediaTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterTbl;
import com.ctrip.framework.drc.core.meta.RowsFilterConfig;
import com.ctrip.xpipe.codec.JsonCodec;

import java.util.List;

/**
 * @ClassName RowsFilterMappingVo
 * @Author haodongPan
 * @Date 2022/5/18 21:46
 * @Version: $
 */
public class RowsFilterMappingVo {
    
    private Long id ;
    
    
    private Long dataMediaId;

    private String namespace;

    private String name;

    private Integer type;
    
    private Long dataMediaSourceId;

    private String dataMediaSourceName;

    
    private Long rowsFilterId;
    
    private String mode;
    
    private List<String> columns;
    
    private String context;
    
    private boolean illegalArgument;

    private Integer fetchMode;

    public RowsFilterMappingVo() {
    }

    public RowsFilterMappingVo (RowsFilterMappingTbl mapping, DataMediaTbl dataMedia, RowsFilterTbl rowsFilterTbl) {
        this.setId(mapping.getId());
        this.setRowsFilter(rowsFilterTbl);
        this.setDataMedia(dataMedia);
    }
    

    public void setDataMedia(DataMediaTbl dataMedia) {
        this.dataMediaId = dataMedia.getId();
        this.namespace = dataMedia.getNamespcae();
        this.name = dataMedia.getName();
        this.type = dataMedia.getType();
        this.dataMediaSourceId = dataMedia.getDataMediaSourceId();
    }
    
    public void setRowsFilter(RowsFilterTbl rowsFilterTbl) {
        this.rowsFilterId = rowsFilterTbl.getId();
        this.mode = rowsFilterTbl.getMode();
        RowsFilterConfig.Parameters parameters = 
                JsonCodec.INSTANCE.decode(rowsFilterTbl.getParameters(), RowsFilterConfig.Parameters.class);
        this.columns = parameters.getColumns();
        this.context = parameters.getContext();
        this.illegalArgument = parameters.getIllegalArgument();
        this.fetchMode = parameters.getFetchMode();
    }


    @Override
    public String toString() {
        return "RowsFilterMappingVo{" +
                "id=" + id +
                ", dataMediaId=" + dataMediaId +
                ", namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", dataMediaSourceId=" + dataMediaSourceId +
                ", dataMediaSourceName='" + dataMediaSourceName + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", mode='" + mode + '\'' +
                ", columns=" + columns +
                ", context='" + context + '\'' +
                ", illegalArgument=" + illegalArgument +
                ", fetchMode=" + fetchMode +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDataMediaId() {
        return dataMediaId;
    }

    public void setDataMediaId(Long dataMediaId) {
        this.dataMediaId = dataMediaId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Long getDataMediaSourceId() {
        return dataMediaSourceId;
    }

    public void setDataMediaSourceId(Long dataMediaSourceId) {
        this.dataMediaSourceId = dataMediaSourceId;
    }

    public String getDataMediaSourceName() {
        return dataMediaSourceName;
    }

    public void setDataMediaSourceName(String dataMediaSourceName) {
        this.dataMediaSourceName = dataMediaSourceName;
    }

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
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

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public boolean isIllegalArgument() {
        return illegalArgument;
    }

    public void setIllegalArgument(boolean illegalArgument) {
        this.illegalArgument = illegalArgument;
    }

    public Integer getFetchMode() {
        return fetchMode;
    }

    public void setFetchMode(Integer fetchMode) {
        this.fetchMode = fetchMode;
    }
}
